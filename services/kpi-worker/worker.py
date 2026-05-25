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

from cosmos_store import CosmosKpiStore

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT', 'https://cosmos-emulator:8081/')
COSMOS_KEY = os.getenv(
    'COSMOS_KEY',
    'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='
)
COSMOS_DB = os.getenv('COSMOS_DB', 'factory_ops')
COSMOS_CONTAINER = os.getenv('COSMOS_CONTAINER', 'kpi_data')
COSMOS_PARTITION_KEY = os.getenv('COSMOS_PARTITION_KEY', '/lineId')
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

    service_name = os.getenv('OTEL_SERVICE_NAME', 'kpi-worker').strip() or 'kpi-worker'
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

SYNTHETIC_KPI_DOCS = [
    {
        'id': 'kpi-1',
        'lineId': 'LineA',
        'product': 'Widget X',
        'goodQty': 1180,
        'wasteQty': 20,
        'efficiency': 94.4,
        'cycleTimeSeconds': 42,
        'timestamp': '2026-05-22T14:00:00Z',
    },
    {
        'id': 'kpi-2',
        'lineId': 'LineB',
        'product': 'Widget Y',
        'goodQty': 980,
        'wasteQty': 35,
        'efficiency': 89.7,
        'cycleTimeSeconds': 48,
        'timestamp': '2026-05-22T22:00:00Z',
    },
    {
        'id': 'kpi-3',
        'lineId': 'LineC',
        'product': 'Widget Z',
        'goodQty': 1025,
        'wasteQty': 15,
        'efficiency': 96.1,
        'cycleTimeSeconds': 39,
        'timestamp': '2026-05-23T06:00:00Z',
    },
]


store = CosmosKpiStore(
    endpoint=COSMOS_ENDPOINT,
    key=COSMOS_KEY,
    database_name=COSMOS_DB,
    container_name=COSMOS_CONTAINER,
    partition_key_path=COSMOS_PARTITION_KEY,
    seed_documents=SYNTHETIC_KPI_DOCS,
)


async def ensure_streams(js):
    for stream_name, subjects in [
        ('chat_kpi', ['chat.kpi']),
        ('chat_response', ['chat.response']),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except Exception:
            pass


def build_kpi_query(message: str):
    known_products = ['Widget X', 'Widget Y', 'Widget Z']
    normalized = message.lower()

    for product in known_products:
        if product.lower() in normalized:
            return (
                'SELECT * FROM c WHERE c.product = @product ORDER BY c.timestamp DESC',
                [{'name': '@product', 'value': product}],
                product,
            )

    if any(term in normalized for term in ['product', 'kpi', 'metric', 'performance', 'efficiency']):
        return (
            'SELECT * FROM c ORDER BY c.timestamp DESC',
            [],
            None,
        )

    return (
        'SELECT TOP 1 * FROM c ORDER BY c.timestamp DESC',
        [],
        None,
    )


def query_kpi_documents(message: str):
    query, parameters, product = build_kpi_query(message)
    items = store.query_with_retry(query=query, parameters=parameters)
    return items, product


def format_kpi_response(message: str, items, product):
    if not items:
        fallback_facts = [
            'KPI fact: current utilization is 87% for this metric.',
            'KPI fact: average response time improved by 12%.',
            'KPI fact: throughput is trending at 97% of goal.',
        ]
        return f"{random.choice(fallback_facts)} {message}"

    selected = items[0]
    summary = (
        f"Product KPI for {selected.get('product', 'unknown')}: "
        f"goodQty={selected.get('goodQty', 'n/a')}, "
        f"wasteQty={selected.get('wasteQty', 'n/a')}, "
        f"efficiency={selected.get('efficiency', 'n/a')}%."
    )

    if product:
        return f"{summary} Based on product '{product}' from your question."

    return f"{summary} {message}"


async def main():
    setup_telemetry()
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    await ensure_streams(js)

    sub = await js.subscribe('chat.kpi', durable='kpi_worker_pool')
    print(f'KPI worker connected to NATS at {NATS_URL}')
    print(f'KPI worker connecting to Cosmos at {COSMOS_ENDPOINT}')
    store.ensure_resources_with_retry(retries=30, delay_seconds=1.0)

    while True:
        try:
            msg = await sub.next_msg(timeout=5)
        except TimeoutError:
            continue

        incoming_headers = dict(msg.headers or {})
        parent_context = extract(incoming_headers)
        with tracer.start_as_current_span('worker.kpi.process', context=parent_context, kind=SpanKind.CONSUMER) as span:
            span.set_attribute('openinference.span.kind', 'TOOL')
            span.set_attribute('cache.hit', False)
            span.set_attribute('cache.source', 'none')
            span.set_attribute('cache.score', 0.0)
            payload = {}
            try:
                payload = json.loads(msg.data.decode())
                message = payload.get('message', '')
                session_id = payload.get('sessionId', '')

                span.set_attribute('chat.session_id', session_id)
                span.set_attribute('chat.message_length', len(message))
                span.set_attribute('input.value', str(message))
                span.set_attribute('input.mime_type', 'text/plain')
                span.set_attribute('orchestration.target', 'kpi-worker')
                span.add_event('worker_message_received')

                items, product = query_kpi_documents(message)
                response_text = format_kpi_response(message, items, product)
                result = {
                    'sessionId': session_id,
                    'reply': response_text,
                    'originalMessage': message,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'kpiHits': len(items),
                    'responseSource': 'kpi-worker',
                    'cacheHit': False,
                }

                span.set_attribute('kpi.query.product', product or 'all')
                span.set_attribute('kpi.query.hit_count', len(items))
                span.set_attribute('chat.reply_length', len(response_text))
                span.set_attribute('output.value', response_text)
                span.set_attribute('output.mime_type', 'text/plain')

                outgoing_headers = {}
                inject(outgoing_headers)
                await js.publish('chat.response', json.dumps(result).encode(), headers=outgoing_headers)
                span.set_attribute('nats.publish.subject', 'chat.response')
                span.add_event('worker_response_published')
                await msg.ack()
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                # Return a safe user-facing message instead of crashing the worker.
                retry_message = (
                    'I am unable to retrieve KPI data right now. '
                    'Please try again in a few moments.'
                )
                try:
                    outgoing_headers = {}
                    inject(outgoing_headers)
                    await js.publish(
                        'chat.response',
                        json.dumps(
                            {
                                'sessionId': str(payload.get('sessionId', '')),
                                'reply': retry_message,
                                'originalMessage': str(payload.get('message', '')),
                                'timestamp': datetime.utcnow().isoformat() + 'Z',
                                'responseSource': 'kpi-worker-fallback',
                                'cacheHit': False,
                            }
                        ).encode(),
                        headers=outgoing_headers,
                    )
                    span.set_attribute('nats.publish.subject', 'chat.response')
                    span.set_attribute('output.value', retry_message)
                    span.set_attribute('output.mime_type', 'text/plain')
                except Exception:
                    # Keep worker alive even if fallback response publish fails.
                    pass

                try:
                    await msg.ack()
                except Exception:
                    pass

                print('KPI worker transient failure; returned retry message to client.')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('KPI worker shutting down')
