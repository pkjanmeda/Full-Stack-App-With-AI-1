import asyncio
import json
import os
import uuid
from datetime import datetime
from time import perf_counter

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from nats.aio.client import Client as NATS
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

app = FastAPI()

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
LANGGRAPH_URL = os.getenv('LANGGRAPH_URL', 'http://langgraph:5000')

nc = NATS()
js = None
tracer = trace.get_tracer(__name__)

class ChatRequest(BaseModel):
    sessionId: str | None = None
    message: str


class WsChatRequest(BaseModel):
    type: str = 'chat'
    message: str


def parse_otlp_headers(raw_headers: str) -> dict[str, str]:
    headers = {}
    for pair in raw_headers.split(','):
        key, sep, value = pair.partition('=')
        if sep and key.strip() and value.strip():
            headers[key.strip()] = value.strip()
    return headers


def setup_telemetry():
    endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', '').strip()
    if not endpoint:
        return

    service_name = os.getenv('OTEL_SERVICE_NAME', 'api-service').strip() or 'api-service'
    environment = os.getenv('OTEL_ENVIRONMENT', 'local').strip() or 'local'
    raw_headers = os.getenv('OTEL_EXPORTER_OTLP_HEADERS', '')

    resource = Resource.create(
        {
            'service.name': service_name,
            'deployment.environment': environment,
        }
    )
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    exporter = OTLPSpanExporter(endpoint=endpoint, headers=parse_otlp_headers(raw_headers))
    provider.add_span_processor(BatchSpanProcessor(exporter))


@app.on_event('startup')
async def startup_event():
    global js
    setup_telemetry()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    for stream_name, subjects in [
        ('chat_incoming', ['chat.incoming']),
        ('chat_response', ['chat.response']),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except Exception:
            pass


@app.post('/api/chat')
async def send_chat(body: ChatRequest):
    message = body.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail='message is required')

    session_id = body.sessionId or str(uuid.uuid4())
    result = await queue_chat_message(session_id=session_id, message=message)
    return {
        'status': result.get('status', 'queued'),
        'messageId': str(uuid.uuid4()),
        'sessionId': session_id,
        'orchestration': result.get('orchestration'),
        'cacheHit': result.get('cacheHit', False),
    }


async def queue_chat_message(session_id: str, message: str):
    with tracer.start_as_current_span('api.send_chat', kind=SpanKind.SERVER) as span:
        span.set_attribute('openinference.span.kind', 'CHAIN')
        span.set_attribute('chat.session_id', session_id)
        span.set_attribute('chat.message_length', len(message))
        span.set_attribute('chat.question_type', 'chat-question')
        span.set_attribute('cache.hit', False)
        span.set_attribute('cache.source', 'none')
        span.add_event('chat_request_received')

        payload = {
            'sessionId': session_id,
            'message': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }

        upstream_headers = {}
        inject(upstream_headers)

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f'{LANGGRAPH_URL}/orchestrate',
                    json={
                        'sessionId': session_id,
                        'task': 'chat-question',
                        'payload': {'message': message},
                    },
                    headers=upstream_headers,
                    timeout=10.0,
                )
                response.raise_for_status()
                orchestration = response.json()
                span.set_attribute('chat.orchestration', orchestration.get('status', 'unknown'))
                span.set_attribute('chat.orchestration.target', orchestration.get('target', 'none'))
                cache_hit = bool(orchestration.get('cacheHit', False))
                span.set_attribute('cache.hit', cache_hit)
                if cache_hit:
                    span.set_attribute('cache.source', 'redis-semantic-cache')
                    if orchestration.get('similarityScore') is not None:
                        span.set_attribute('cache.score', float(orchestration.get('similarityScore')))
                span.add_event('langgraph_orchestration_complete')
            except Exception as exc:
                span.set_attribute('chat.orchestration', 'fallback-queued')
                span.add_event('langgraph_orchestration_failed')
                if js is None:
                    raise HTTPException(status_code=503, detail='message bus is not initialized')
                fallback_headers = {}
                inject(fallback_headers)
                await js.publish('chat.incoming', json.dumps(payload).encode(), headers=fallback_headers)
                span.set_attribute('nats.publish.subject', 'chat.incoming')
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                return {'status': 'queued', 'orchestration': None, 'cacheHit': False}

            return {
                'status': 'queued',
                'orchestration': orchestration.get('status'),
                'cacheHit': bool(orchestration.get('cacheHit', False)),
            }


@app.websocket('/api/chat/ws/{session_id}')
async def chat_ws(websocket: WebSocket, session_id: str):
    await websocket.accept()
    sub = await nc.subscribe('chat.response')
    stream_started_at = perf_counter()
    first_chunk_sent = False
    shutdown_event = asyncio.Event()

    async def pump_chat_responses():
        nonlocal first_chunk_sent
        try:
            while not shutdown_event.is_set():
                try:
                    msg = await sub.next_msg(timeout=15)
                except asyncio.TimeoutError:
                    continue

                carrier = dict(msg.headers or {})
                parent_context = extract(carrier)
                with tracer.start_as_current_span('api.ws.emit', context=parent_context) as span:
                    span.set_attribute('openinference.span.kind', 'CHAIN')
                    data = json.loads(msg.data.decode())
                    if data.get('sessionId') != session_id:
                        continue

                    span.set_attribute('chat.session_id', session_id)
                    span.set_attribute('stream.transport', 'websocket')
                    span.set_attribute('stream.response_source', str(data.get('responseSource', 'unknown')))
                    cache_hit = bool(data.get('cacheHit', False))
                    span.set_attribute('cache.hit', cache_hit)
                    span.set_attribute('cache.source', str(data.get('responseSource', 'none')) if cache_hit else 'none')
                    if data.get('similarityScore') is not None:
                        span.set_attribute('cache.score', float(data.get('similarityScore')))
                    reply = data.get('reply')
                    if isinstance(reply, str) and reply.strip():
                        words = reply.split()
                        span.set_attribute('stream.word_count', len(words))
                        span.set_attribute('stream.chunk_delay_ms', 100)
                        partial = ''
                        for index, word in enumerate(words, 1):
                            partial = f'{partial} {word}'.strip()
                            chunk = {**data, 'reply': partial, 'isPartial': True}
                            await websocket.send_json(chunk)
                            if not first_chunk_sent:
                                first_chunk_latency_ms = int((perf_counter() - stream_started_at) * 1000)
                                span.set_attribute('stream.first_chunk_latency_ms', first_chunk_latency_ms)
                                first_chunk_sent = True
                            span.add_event('stream_chunk_emitted', {'stream.chunk_index': index})
                            await asyncio.sleep(0.1)
                        final_chunk = {**data, 'reply': reply, 'isPartial': False}
                        await websocket.send_json(final_chunk)
                        span.add_event('stream_completed')
                        span.set_attribute('stream.total_duration_ms', int((perf_counter() - stream_started_at) * 1000))
                    else:
                        await websocket.send_json(data)
        except WebSocketDisconnect:
            shutdown_event.set()

    pump_task = asyncio.create_task(pump_chat_responses())

    try:
        while True:
            raw_message = await websocket.receive_text()
            with tracer.start_as_current_span('api.ws.receive') as span:
                span.set_attribute('openinference.span.kind', 'CHAIN')
                span.set_attribute('chat.session_id', session_id)
                span.set_attribute('stream.transport', 'websocket')

                try:
                    payload = WsChatRequest.model_validate_json(raw_message)
                except Exception:
                    await websocket.send_json({'type': 'error', 'message': 'invalid websocket payload'})
                    continue

                if payload.type != 'chat':
                    await websocket.send_json({'type': 'error', 'message': 'unsupported websocket message type'})
                    continue

                message = payload.message.strip()
                if not message:
                    await websocket.send_json({'type': 'error', 'message': 'message is required'})
                    continue

                enqueue_result = await queue_chat_message(session_id=session_id, message=message)
                await websocket.send_json(
                    {
                        'type': 'ack',
                        'status': enqueue_result.get('status', 'queued'),
                        'orchestration': enqueue_result.get('orchestration'),
                        'cacheHit': enqueue_result.get('cacheHit', False),
                        'sessionId': session_id,
                    }
                )
    except WebSocketDisconnect:
        pass
    finally:
        shutdown_event.set()
        pump_task.cancel()
        await sub.unsubscribe()


