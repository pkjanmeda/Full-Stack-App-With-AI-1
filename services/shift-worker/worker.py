import asyncio
import json
import os
import random
from datetime import datetime

from nats.aio.client import Client as NATS
from nats.errors import TimeoutError
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
tracer = trace.get_tracer(__name__)


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

    service_name = os.getenv('OTEL_SERVICE_NAME', 'shift-worker').strip() or 'shift-worker'
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

async def ensure_streams(js):
    for stream_name, subjects in [
        ('chat_incoming', ['chat.incoming']),
        ('chat_response', ['chat.response']),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except Exception:
            pass


def random_response(message: str) -> str:
    suffixes = [
        '— this is your randomized answer.',
        'and the system thinks it sounds interesting.',
        'with a little extra magic from Python.',
    ]
    return f'{message} {random.choice(suffixes)}'


async def main():
    setup_telemetry()
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    await ensure_streams(js)

    sub = await js.subscribe('chat.incoming', durable='worker_pool')
    print(f'Worker connected to NATS at {NATS_URL}')

    while True:
        try:
            msg = await sub.next_msg(timeout=5)
        except TimeoutError:
            continue

        incoming_headers = dict(msg.headers or {})
        parent_context = extract(incoming_headers)
        with tracer.start_as_current_span('worker.shift.process', context=parent_context, kind=SpanKind.CONSUMER) as span:
            span.set_attribute('openinference.span.kind', 'TOOL')
            span.set_attribute('cache.hit', False)
            span.set_attribute('cache.source', 'none')
            span.set_attribute('cache.score', 0.0)
            try:
                payload = json.loads(msg.data.decode())
                message = payload.get('message', '')
                session_id = payload.get('sessionId', '')

                span.set_attribute('chat.session_id', session_id)
                span.set_attribute('chat.message_length', len(message))
                span.set_attribute('orchestration.target', 'shift-worker')
                span.add_event('worker_message_received')

                response_text = random_response(message)
                result = {
                    'sessionId': session_id,
                    'reply': response_text,
                    'originalMessage': message,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'responseSource': 'shift-worker',
                    'cacheHit': False,
                }

                outgoing_headers = {}
                inject(outgoing_headers)
                await js.publish('chat.response', json.dumps(result).encode(), headers=outgoing_headers)
                span.set_attribute('nats.publish.subject', 'chat.response')
                span.set_attribute('chat.reply_length', len(response_text))
                span.add_event('worker_response_published')
                await msg.ack()
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                raise


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Worker shutting down')
