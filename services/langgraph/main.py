import json
import os
from datetime import datetime

from fastapi import FastAPI, HTTPException
from nats.aio.client import Client as NATS
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import BaseModel

from agents import graph

app = FastAPI()

LANGGRAPH_MODE = os.getenv('LANGGRAPH_MODE', 'local')
NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')

nc = NATS()
js = None
tracer = trace.get_tracer(__name__)


class OrchestrationRequest(BaseModel):
    sessionId: str
    task: str
    payload: dict


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

    service_name = os.getenv('OTEL_SERVICE_NAME', 'langgraph-orchestrator').strip() or 'langgraph-orchestrator'
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


def stream_name_for_subject(subject: str) -> str:
    return subject.replace('.', '_')


async def publish_stream(subject: str, payload: dict):
    global js
    if js is None:
        raise RuntimeError('JetStream client is not initialized')
    await js.publish(subject, json.dumps(payload).encode())


@app.on_event('startup')
async def startup_event():
    global js
    setup_telemetry()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    subjects = set()
    for node in graph.nodes.values():
        subjects.update(node.subjects)
    subjects.add('chat.response')

    for subject in subjects:
        stream_name = stream_name_for_subject(subject)
        try:
            await js.add_stream(name=stream_name, subjects=[subject])
        except Exception:
            pass


@app.on_event('shutdown')
async def shutdown_event():
    await nc.close()


@app.get('/health')
async def health():
    return {
        'status': 'langgraph-orchestrator',
        'mode': LANGGRAPH_MODE,
        'graphNodes': [node.id for node in graph.nodes.values()],
    }


@app.post('/orchestrate')
async def orchestrate(request: OrchestrationRequest):
    with tracer.start_as_current_span('orchestrate.request') as span:
        span.set_attribute('chat.session_id', request.sessionId)
        span.set_attribute('chat.task', request.task)

        message = str(request.payload.get('message', '')).strip()
        if not message:
            raise HTTPException(status_code=400, detail='message is required')

        span.set_attribute('chat.message_length', len(message))
        selected_node = graph.find_best_node(message)
        if selected_node:
            span.set_attribute('orchestration.status', 'forwarded')
            span.set_attribute('orchestration.target', selected_node.id)

            payload = {
                'sessionId': request.sessionId,
                'message': message,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'orchestration': f'{selected_node.id}-routing',
                'nodeId': selected_node.id,
                'nodeName': selected_node.name,
            }
            await publish_stream(selected_node.route_name, payload)
            return {
                'status': 'forwarded',
                'target': selected_node.id,
                'sessionId': request.sessionId,
                'message': f'Question forwarded to {selected_node.name} for handling.',
            }

        span.set_attribute('orchestration.status', 'declined')
        decline_text = (
            'Sorry, I cannot answer that question right now. '
            'Your feedback has been submitted to the product owner so that this functionality can be added.'
        )
        response_payload = {
            'sessionId': request.sessionId,
            'reply': decline_text,
            'originalMessage': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'feedbackSubmitted': True,
        }
        await publish_stream('chat.response', response_payload)

        return {
            'status': 'declined',
            'sessionId': request.sessionId,
            'reply': decline_text,
            'feedbackSubmitted': True,
        }
