import asyncio
import json
import os
import uuid
from datetime import datetime
from time import perf_counter

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from nats.aio.client import Client as NATS
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

app = FastAPI()

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
LANGGRAPH_URL = os.getenv('LANGGRAPH_URL', 'http://langgraph:5000')

nc = NATS()
js = None
tracer = trace.get_tracer(__name__)

class ChatRequest(BaseModel):
    sessionId: str | None = None
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
    with tracer.start_as_current_span('api.send_chat') as span:
        message = body.message.strip()
        if not message:
            raise HTTPException(status_code=400, detail='message is required')

        session_id = body.sessionId or str(uuid.uuid4())
        span.set_attribute('chat.session_id', session_id)
        span.set_attribute('chat.message_length', len(message))
        span.set_attribute('chat.question_type', 'chat-question')
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
                return {'status': 'queued', 'messageId': str(uuid.uuid4()), 'sessionId': session_id}

        return {
            'status': 'queued',
            'messageId': str(uuid.uuid4()),
            'sessionId': session_id,
            'orchestration': orchestration.get('status'),
        }


@app.get('/api/chat/stream')
async def stream(sessionId: str = Query(...)):
    async def event_generator():
        sub = await nc.subscribe('chat.response')
        first_chunk_sent = False
        stream_started_at = perf_counter()
        try:
            while True:
                try:
                    msg = await sub.next_msg(timeout=15)
                except asyncio.TimeoutError:
                    yield ': ping\n\n'
                    continue

                carrier = dict(msg.headers or {})
                parent_context = extract(carrier)
                with tracer.start_as_current_span('api.stream.emit', context=parent_context) as span:
                    data = json.loads(msg.data.decode())
                    if data.get('sessionId') == sessionId:
                        span.set_attribute('chat.session_id', sessionId)
                        span.set_attribute('stream.response_source', str(data.get('responseSource', 'unknown')))
                        reply = data.get('reply')
                        if isinstance(reply, str) and reply.strip():
                            words = reply.split()
                            span.set_attribute('stream.word_count', len(words))
                            span.set_attribute('stream.chunk_delay_ms', 100)
                            partial = ''
                            for index, word in enumerate(words, 1):
                                partial = f'{partial} {word}'.strip()
                                chunk = {**data, 'reply': partial, 'isPartial': True}
                                yield f'event: message\ndata: {json.dumps(chunk)}\n\n'
                                if not first_chunk_sent:
                                    first_chunk_latency_ms = int((perf_counter() - stream_started_at) * 1000)
                                    span.set_attribute('stream.first_chunk_latency_ms', first_chunk_latency_ms)
                                    first_chunk_sent = True
                                span.add_event('stream_chunk_emitted', {'stream.chunk_index': index})
                                await asyncio.sleep(0.1)
                            final_chunk = {**data, 'reply': reply, 'isPartial': False}
                            yield f'event: message\ndata: {json.dumps(final_chunk)}\n\n'
                            span.add_event('stream_completed')
                            span.set_attribute('stream.total_duration_ms', int((perf_counter() - stream_started_at) * 1000))
                        else:
                            yield f'event: message\ndata: {json.dumps(data)}\n\n'
        finally:
            await sub.unsubscribe()

    return StreamingResponse(event_generator(), media_type='text/event-stream')
