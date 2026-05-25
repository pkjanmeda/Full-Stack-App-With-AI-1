import asyncio
import json
import os
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

from shift_store import CosmosShiftStore, extract_line_hint, extract_shift_hint

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT', 'https://cosmos-emulator:8081/')
COSMOS_KEY = os.getenv(
    'COSMOS_KEY',
    'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='
)
COSMOS_DB = os.getenv('COSMOS_DB', 'factory_ops')
COSMOS_CONTAINER = os.getenv('COSMOS_CONTAINER', 'shift_data')
COSMOS_PARTITION_KEY = os.getenv('COSMOS_PARTITION_KEY', '/lineId')
tracer = trace.get_tracer(__name__)


SHIFT_SEED_DOCS = [
    {
        'id': 'shift-worker-a-s1',
        'lineId': 'LineA',
        'docType': 'worker_shift',
        'operator': 'Ava Johnson',
        'shift': 'shift1',
        'product': 'Widget X',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-b-s1',
        'lineId': 'LineB',
        'docType': 'worker_shift',
        'operator': 'Noah Patel',
        'shift': 'shift1',
        'product': 'Widget Y',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-c-s1',
        'lineId': 'LineC',
        'docType': 'worker_shift',
        'operator': 'Mia Lopez',
        'shift': 'shift1',
        'product': 'Widget Z',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-a-s2',
        'lineId': 'LineA',
        'docType': 'worker_shift',
        'operator': 'Ethan Brooks',
        'shift': 'shift2',
        'product': 'Widget X',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-b-s2',
        'lineId': 'LineB',
        'docType': 'worker_shift',
        'operator': 'Sophia Chen',
        'shift': 'shift2',
        'product': 'Widget Y',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-c-s2',
        'lineId': 'LineC',
        'docType': 'worker_shift',
        'operator': 'Lucas Grant',
        'shift': 'shift2',
        'product': 'Widget Z',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-a-s3',
        'lineId': 'LineA',
        'docType': 'worker_shift',
        'operator': 'Olivia Reed',
        'shift': 'shift3',
        'product': 'Widget X',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-b-s3',
        'lineId': 'LineB',
        'docType': 'worker_shift',
        'operator': 'Liam Ortiz',
        'shift': 'shift3',
        'product': 'Widget Y',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-worker-c-s3',
        'lineId': 'LineC',
        'docType': 'worker_shift',
        'operator': 'Emma Diaz',
        'shift': 'shift3',
        'product': 'Widget Z',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-a-s1',
        'lineId': 'LineA',
        'docType': 'forms_summary',
        'operator': 'Ava Johnson',
        'product': 'Widget X',
        'formsFilled': 18,
        'shift': 'shift1',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-b-s1',
        'lineId': 'LineB',
        'docType': 'forms_summary',
        'operator': 'Noah Patel',
        'product': 'Widget Y',
        'formsFilled': 15,
        'shift': 'shift1',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-c-s1',
        'lineId': 'LineC',
        'docType': 'forms_summary',
        'operator': 'Mia Lopez',
        'product': 'Widget Z',
        'formsFilled': 21,
        'shift': 'shift1',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-a-s2',
        'lineId': 'LineA',
        'docType': 'forms_summary',
        'operator': 'Ethan Brooks',
        'product': 'Widget X',
        'formsFilled': 20,
        'shift': 'shift2',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-b-s2',
        'lineId': 'LineB',
        'docType': 'forms_summary',
        'operator': 'Sophia Chen',
        'product': 'Widget Y',
        'formsFilled': 17,
        'shift': 'shift2',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-c-s2',
        'lineId': 'LineC',
        'docType': 'forms_summary',
        'operator': 'Lucas Grant',
        'product': 'Widget Z',
        'formsFilled': 19,
        'shift': 'shift2',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-a-s3',
        'lineId': 'LineA',
        'docType': 'forms_summary',
        'operator': 'Olivia Reed',
        'product': 'Widget X',
        'formsFilled': 22,
        'shift': 'shift3',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-b-s3',
        'lineId': 'LineB',
        'docType': 'forms_summary',
        'operator': 'Liam Ortiz',
        'product': 'Widget Y',
        'formsFilled': 18,
        'shift': 'shift3',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-forms-c-s3',
        'lineId': 'LineC',
        'docType': 'forms_summary',
        'operator': 'Emma Diaz',
        'product': 'Widget Z',
        'formsFilled': 24,
        'shift': 'shift3',
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-a-s1',
        'lineId': 'LineA',
        'docType': 'production_total',
        'shift': 'shift1',
        'product': 'Widget X',
        'totalProduced': 1320,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-b-s1',
        'lineId': 'LineB',
        'docType': 'production_total',
        'shift': 'shift1',
        'product': 'Widget Y',
        'totalProduced': 1185,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-c-s1',
        'lineId': 'LineC',
        'docType': 'production_total',
        'shift': 'shift1',
        'product': 'Widget Z',
        'totalProduced': 1410,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-a-s2',
        'lineId': 'LineA',
        'docType': 'production_total',
        'shift': 'shift2',
        'product': 'Widget X',
        'totalProduced': 1275,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-b-s2',
        'lineId': 'LineB',
        'docType': 'production_total',
        'shift': 'shift2',
        'product': 'Widget Y',
        'totalProduced': 1230,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-c-s2',
        'lineId': 'LineC',
        'docType': 'production_total',
        'shift': 'shift2',
        'product': 'Widget Z',
        'totalProduced': 1360,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-a-s3',
        'lineId': 'LineA',
        'docType': 'production_total',
        'shift': 'shift3',
        'product': 'Widget X',
        'totalProduced': 1450,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-b-s3',
        'lineId': 'LineB',
        'docType': 'production_total',
        'shift': 'shift3',
        'product': 'Widget Y',
        'totalProduced': 1340,
        'date': '2026-05-24',
    },
    {
        'id': 'shift-prod-c-s3',
        'lineId': 'LineC',
        'docType': 'production_total',
        'shift': 'shift3',
        'product': 'Widget Z',
        'totalProduced': 1495,
        'date': '2026-05-24',
    },
]


shift_store = CosmosShiftStore(
    endpoint=COSMOS_ENDPOINT,
    key=COSMOS_KEY,
    database_name=COSMOS_DB,
    container_name=COSMOS_CONTAINER,
    partition_key_path=COSMOS_PARTITION_KEY,
    seed_documents=SHIFT_SEED_DOCS,
)


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


def build_shift_query(message: str):
    normalized = message.lower()
    line_hint = extract_line_hint(message)
    shift_hint, shift_explicit = extract_shift_hint(message)

    def append_shift_filter(base_query: str, params: list[dict]) -> tuple[str, list[dict]]:
        if 'ORDER BY' in base_query.upper():
            where_part, order_part = base_query.rsplit(' ORDER BY ', 1)
            return (
                where_part + ' AND c.shift = @shift ORDER BY ' + order_part,
                [*params, {'name': '@shift', 'value': shift_hint}],
            )
        return (
            base_query + ' AND c.shift = @shift',
            [*params, {'name': '@shift', 'value': shift_hint}],
        )

    if 'worker' in normalized or 'who worked' in normalized:
        if line_hint:
            query, parameters = append_shift_filter(
                'SELECT * FROM c WHERE c.docType = @docType AND c.lineId = @lineId ORDER BY c.date DESC',
                [
                    {'name': '@docType', 'value': 'worker_shift'},
                    {'name': '@lineId', 'value': line_hint},
                ],
            )
            return (
                'worker_shift',
                query,
                parameters,
                shift_hint,
                shift_explicit,
            )
        query, parameters = append_shift_filter(
            'SELECT * FROM c WHERE c.docType = @docType ORDER BY c.date DESC',
            [{'name': '@docType', 'value': 'worker_shift'}],
        )
        return (
            'worker_shift',
            query,
            parameters,
            shift_hint,
            shift_explicit,
        )

    if 'form' in normalized:
        if line_hint:
            query, parameters = append_shift_filter(
                'SELECT * FROM c WHERE c.docType = @docType AND c.lineId = @lineId ORDER BY c.formsFilled DESC',
                [
                    {'name': '@docType', 'value': 'forms_summary'},
                    {'name': '@lineId', 'value': line_hint},
                ],
            )
            return (
                'forms_summary',
                query,
                parameters,
                shift_hint,
                shift_explicit,
            )
        query, parameters = append_shift_filter(
            'SELECT * FROM c WHERE c.docType = @docType ORDER BY c.formsFilled DESC',
            [{'name': '@docType', 'value': 'forms_summary'}],
        )
        return (
            'forms_summary',
            query,
            parameters,
            shift_hint,
            shift_explicit,
        )

    if 'produced' in normalized or 'production' in normalized or 'output' in normalized:
        if line_hint:
            query, parameters = append_shift_filter(
                'SELECT * FROM c WHERE c.docType = @docType AND c.lineId = @lineId ORDER BY c.totalProduced DESC',
                [
                    {'name': '@docType', 'value': 'production_total'},
                    {'name': '@lineId', 'value': line_hint},
                ],
            )
            return (
                'production_total',
                query,
                parameters,
                shift_hint,
                shift_explicit,
            )
        query, parameters = append_shift_filter(
            'SELECT * FROM c WHERE c.docType = @docType ORDER BY c.totalProduced DESC',
            [{'name': '@docType', 'value': 'production_total'}],
        )
        return (
            'production_total',
            query,
            parameters,
            shift_hint,
            shift_explicit,
        )

    query, parameters = append_shift_filter(
        'SELECT * FROM c WHERE c.docType IN ("worker_shift", "forms_summary", "production_total") ORDER BY c.date DESC',
        [],
    )
    return (
        'overview',
        query,
        parameters,
        shift_hint,
        shift_explicit,
    )


def summarize_shift_data(query_type: str, items: list[dict], shift_hint: str, shift_explicit: bool) -> str:
    shift_label = shift_hint.replace('shift', 'Shift ')
    shift_default_suffix = '' if shift_explicit else ' (defaulted to Shift 3)'
    if not items:
        return f'{shift_label}{shift_default_suffix}: data is currently unavailable for that request.'

    if query_type == 'worker_shift':
        workers = [f"{item.get('operator', 'unknown')} ({item.get('lineId', 'n/a')})" for item in items[:5]]
        return f'{shift_label}{shift_default_suffix} workers active by line: ' + ', '.join(workers) + '.'

    if query_type == 'forms_summary':
        entries = [
            f"{item.get('operator', 'unknown')} filled {item.get('formsFilled', 0)} forms on {item.get('lineId', 'n/a')} for {item.get('product', 'n/a')}"
            for item in items[:5]
        ]
        return f'{shift_label}{shift_default_suffix} forms summary: ' + '; '.join(entries) + '.'

    if query_type == 'production_total':
        entries = [
            f"{item.get('lineId', 'n/a')} produced {item.get('totalProduced', 0)} units of {item.get('product', 'n/a')}"
            for item in items[:5]
        ]
        return f'{shift_label}{shift_default_suffix} production totals: ' + '; '.join(entries) + '.'

    worker_count = sum(1 for item in items if item.get('docType') == 'worker_shift')
    total_forms = sum(int(item.get('formsFilled', 0) or 0) for item in items if item.get('docType') == 'forms_summary')
    total_output = sum(int(item.get('totalProduced', 0) or 0) for item in items if item.get('docType') == 'production_total')
    return (
        f'{shift_label}{shift_default_suffix} overview: {worker_count} workers logged, '
        f'{total_forms} forms filled, and {total_output} total units produced across lines.'
    )


async def main():
    setup_telemetry()
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    await ensure_streams(js)
    print(f'Shift worker connecting to Cosmos at {COSMOS_ENDPOINT}')
    shift_store.ensure_resources_with_retry(retries=30, delay_seconds=1.0)

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
                span.set_attribute('input.value', str(message))
                span.set_attribute('input.mime_type', 'text/plain')
                span.set_attribute('orchestration.target', 'shift-worker')
                span.add_event('worker_message_received')

                query_type, query, parameters, shift_hint, shift_explicit = build_shift_query(message)
                items = shift_store.query_with_retry(query=query, parameters=parameters)
                response_text = summarize_shift_data(
                    query_type=query_type,
                    items=items,
                    shift_hint=shift_hint,
                    shift_explicit=shift_explicit,
                )
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
                span.set_attribute('shift.query.type', query_type)
                span.set_attribute('shift.query.hit_count', len(items))
                span.set_attribute('shift.query.shift', shift_hint)
                span.set_attribute('shift.query.shift_explicit', shift_explicit)
                span.set_attribute('output.value', response_text)
                span.set_attribute('output.mime_type', 'text/plain')
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
