import asyncio
import json
import os
import uuid
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from nats.aio.client import Client as NATS
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

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

        payload = {
            'sessionId': session_id,
            'message': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f'{LANGGRAPH_URL}/orchestrate',
                    json={
                        'sessionId': session_id,
                        'task': 'chat-question',
                        'payload': {'message': message},
                    },
                    timeout=10.0,
                )
                response.raise_for_status()
                orchestration = response.json()
                span.set_attribute('chat.orchestration', orchestration.get('status', 'unknown'))
            except Exception:
                span.set_attribute('chat.orchestration', 'fallback-queued')
                if js is None:
                    raise HTTPException(status_code=503, detail='message bus is not initialized')
                await js.publish('chat.incoming', json.dumps(payload).encode())
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
        try:
            while True:
                try:
                    msg = await sub.next_msg(timeout=15)
                except asyncio.TimeoutError:
                    yield ': ping\n\n'
                    continue

                data = json.loads(msg.data.decode())
                if data.get('sessionId') == sessionId:
                    reply = data.get('reply')
                    if isinstance(reply, str) and reply.strip():
                        words = reply.split()
                        partial = ''
                        for word in words:
                            partial = f'{partial} {word}'.strip()
                            chunk = {**data, 'reply': partial, 'isPartial': True}
                            yield f'event: message\ndata: {json.dumps(chunk)}\n\n'
                            await asyncio.sleep(0.1)
                        final_chunk = {**data, 'reply': reply, 'isPartial': False}
                        yield f'event: message\ndata: {json.dumps(final_chunk)}\n\n'
                    else:
                        yield f'event: message\ndata: {json.dumps(data)}\n\n'
        finally:
            await sub.unsubscribe()

    return StreamingResponse(event_generator(), media_type='text/event-stream')
