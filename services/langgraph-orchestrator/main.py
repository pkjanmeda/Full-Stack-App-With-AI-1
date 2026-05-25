import asyncio
import json
import os
import re
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from nats.aio.client import Client as NATS
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind
from pydantic import BaseModel

from agents import graph
from redis_memory import RedisConversationMemory

app = FastAPI()

LANGGRAPH_MODE = os.getenv('LANGGRAPH_MODE', 'local')
NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
REDIS_MEMORY_TTL_SECONDS = int(os.getenv('REDIS_MEMORY_TTL_SECONDS', '3600'))
REDIS_MEMORY_MAX_TURNS = int(os.getenv('REDIS_MEMORY_MAX_TURNS', '50'))
REDIS_SEMANTIC_THRESHOLD = float(os.getenv('REDIS_SEMANTIC_THRESHOLD', '0.72'))

nc = NATS()
js = None
tracer = trace.get_tracer(__name__)
redis_memory = None
memory_cache_task = None
thread_name_memory = {}

GREETING_TERMS = {
    'hi',
    'hello',
    'hey',
    'good morning',
    'good afternoon',
    'good evening',
}

NAME_PATTERNS = [
    re.compile(r"\bmy name is\s+([a-zA-Z][a-zA-Z\s'-]{0,40})", re.IGNORECASE),
    re.compile(r"\bi am\s+([a-zA-Z][a-zA-Z\s'-]{0,40})", re.IGNORECASE),
    re.compile(r"\bi'm\s+([a-zA-Z][a-zA-Z\s'-]{0,40})", re.IGNORECASE),
    re.compile(r"\bcall me\s+([a-zA-Z][a-zA-Z\s'-]{0,40})", re.IGNORECASE),
]

NAME_QUERY_PATTERNS = [
    re.compile(r"\bwhat is my name\b", re.IGNORECASE),
    re.compile(r"\bdo you know my name\b", re.IGNORECASE),
    re.compile(r"\bwho am i\b", re.IGNORECASE),
]

SHIFT_ANALYTICS_PATTERNS = [
    re.compile(r"\bshift\s+summary\b", re.IGNORECASE),
    re.compile(r"\bshift\b", re.IGNORECASE),
    re.compile(r"\bwho worked\b", re.IGNORECASE),
    re.compile(r"\bworkers?\b", re.IGNORECASE),
    re.compile(r"\bforms?\b", re.IGNORECASE),
    re.compile(r"\btotal\s+products?\b", re.IGNORECASE),
    re.compile(r"\bproducts?\s+produced\b", re.IGNORECASE),
    re.compile(r"\bproduction\s+line\b", re.IGNORECASE),
    re.compile(r"\bline\s*[a-z0-9]+\b", re.IGNORECASE),
]


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


def normalize_whitespace(value: str) -> str:
    return ' '.join(value.strip().split())


def sanitize_name(value: str) -> str | None:
    cleaned = normalize_whitespace(value)
    if not cleaned:
        return None
    # Cap to first three words to keep profile values simple and predictable.
    parts = cleaned.split(' ')[:3]
    normalized = ' '.join(part.capitalize() for part in parts)
    if len(normalized) > 50:
        normalized = normalized[:50].rstrip()
    return normalized or None


def extract_user_name(message: str) -> str | None:
    for pattern in NAME_PATTERNS:
        match = pattern.search(message)
        if match:
            return sanitize_name(match.group(1))
    return None


def is_name_query(message: str) -> bool:
    return any(pattern.search(message) for pattern in NAME_QUERY_PATTERNS)


def is_greeting(message: str) -> bool:
    normalized = normalize_whitespace(message).lower()
    if normalized in GREETING_TERMS:
        return True
    return any(normalized.startswith(term + ' ') for term in GREETING_TERMS)


def is_shift_analytics_query(message: str) -> bool:
    normalized = normalize_whitespace(message).lower()
    if normalized in {'summary', 'summarize'}:
        return True
    return any(pattern.search(message) for pattern in SHIFT_ANALYTICS_PATTERNS)


def extract_shift_bucket(message: str) -> str:
    lowered = message.lower()
    if re.search(r'\bshift\s*1\b', lowered) or 'first shift' in lowered or 'shift one' in lowered:
        return 'shift1'
    if re.search(r'\bshift\s*2\b', lowered) or 'second shift' in lowered or 'shift two' in lowered:
        return 'shift2'
    if re.search(r'\bshift\s*3\b', lowered) or 'third shift' in lowered or 'shift three' in lowered:
        return 'shift3'
    # Shift analytics defaults to shift3 in the worker when no explicit shift is provided.
    if is_shift_analytics_query(message):
        return 'shift3'
    return 'none'


async def publish_stream(subject: str, payload: dict):
    global js
    if js is None:
        raise RuntimeError('JetStream client is not initialized')
    headers = {}
    inject(headers)
    await js.publish(subject, json.dumps(payload).encode(), headers=headers)


async def save_user_name(session_id: str, user_name: str):
    thread_name_memory[session_id] = user_name
    if redis_memory is not None:
        try:
            await redis_memory.set_user_name(session_id, user_name)
        except Exception:
            pass


async def load_user_name(session_id: str) -> str | None:
    if redis_memory is not None:
        try:
            stored = await redis_memory.get_user_name(session_id)
            if stored:
                thread_name_memory[session_id] = stored
                return stored
        except Exception:
            pass
    return thread_name_memory.get(session_id)


async def publish_direct_response(session_id: str, message: str, reply: str, source: str):
    response_payload = {
        'sessionId': session_id,
        'reply': reply,
        'originalMessage': message,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'responseSource': source,
        'cacheHit': False,
    }
    await publish_stream('chat.response', response_payload)
    return {
        'status': 'responded',
        'sessionId': session_id,
        'reply': reply,
        'cacheHit': False,
    }


async def cache_response_messages():
    if redis_memory is None:
        return

    sub = await nc.subscribe('chat.response')
    try:
        while True:
            try:
                msg = await sub.next_msg(timeout=5)
            except asyncio.TimeoutError:
                continue

            try:
                payload = json.loads(msg.data.decode())
            except json.JSONDecodeError:
                continue

            session_id = str(payload.get('sessionId', '')).strip()
            reply = str(payload.get('reply', '')).strip()
            original_message = str(payload.get('originalMessage', '')).strip()
            source = str(payload.get('responseSource', 'unknown'))
            if session_id and reply and original_message:
                context_tag = extract_shift_bucket(original_message)
                await redis_memory.add_turn(
                    session_id=session_id,
                    user_message=original_message,
                    assistant_reply=reply,
                    source=source,
                    context_tag=context_tag,
                )
    except asyncio.CancelledError:
        pass
    finally:
        await sub.unsubscribe()


@app.on_event('startup')
async def startup_event():
    global js, redis_memory, memory_cache_task
    setup_telemetry()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    redis_memory = RedisConversationMemory(
        redis_url=REDIS_URL,
        ttl_seconds=REDIS_MEMORY_TTL_SECONDS,
        max_turns=REDIS_MEMORY_MAX_TURNS,
        similarity_threshold=REDIS_SEMANTIC_THRESHOLD,
    )
    try:
        await redis_memory.connect()
        memory_cache_task = asyncio.create_task(cache_response_messages())
    except Exception:
        redis_memory = None

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
    if memory_cache_task is not None:
        memory_cache_task.cancel()
    if redis_memory is not None:
        await redis_memory.close()
    await nc.close()


@app.get('/health')
async def health():
    return {
        'status': 'langgraph-orchestrator',
        'mode': LANGGRAPH_MODE,
        'graphNodes': [node.id for node in graph.nodes.values()],
    }


@app.post('/orchestrate')
async def orchestrate(request: OrchestrationRequest, raw_request: Request):
    incoming_headers = dict(raw_request.headers)
    parent_context = extract(incoming_headers)

    with tracer.start_as_current_span('orchestrate.request', context=parent_context, kind=SpanKind.SERVER) as span:
        span.set_attribute('openinference.span.kind', 'AGENT')
        span.set_attribute('chat.session_id', request.sessionId)
        span.set_attribute('chat.task', request.task)
        span.set_attribute('cache.hit', False)
        span.set_attribute('cache.source', 'none')
        span.add_event('orchestration_received')

        message = str(request.payload.get('message', '')).strip()
        if not message:
            raise HTTPException(status_code=400, detail='message is required')

        span.set_attribute('chat.message_length', len(message))
        span.set_attribute('input.value', message)
        span.set_attribute('input.mime_type', 'text/plain')

        extracted_name = extract_user_name(message)
        if extracted_name:
            await save_user_name(request.sessionId, extracted_name)
            span.set_attribute('orchestration.status', 'basic-ai-name-saved')
            span.set_attribute('basic_ai.intent', 'save_name')
            span.set_attribute('output.value', f'Nice to meet you, {extracted_name}. I will remember your name for this thread.')
            span.set_attribute('output.mime_type', 'text/plain')
            span.add_event('basic_ai_name_saved')
            span.set_attribute('nats.publish.subject', 'chat.response')
            return await publish_direct_response(
                session_id=request.sessionId,
                message=message,
                reply=f'Nice to meet you, {extracted_name}. I will remember your name for this thread.',
                source='langgraph-basic-ai',
            )

        if is_name_query(message):
            stored_name = await load_user_name(request.sessionId)
            span.set_attribute('orchestration.status', 'basic-ai-name-query')
            span.set_attribute('basic_ai.intent', 'query_name')
            span.add_event('basic_ai_name_queried')
            span.set_attribute('nats.publish.subject', 'chat.response')
            if stored_name:
                span.set_attribute('output.value', f'Your name is {stored_name}.')
                span.set_attribute('output.mime_type', 'text/plain')
                return await publish_direct_response(
                    session_id=request.sessionId,
                    message=message,
                    reply=f'Your name is {stored_name}.',
                    source='langgraph-basic-ai',
                )
            span.set_attribute('output.value', 'I do not know your name yet. Tell me by saying: my name is <your name>.')
            span.set_attribute('output.mime_type', 'text/plain')
            return await publish_direct_response(
                session_id=request.sessionId,
                message=message,
                reply='I do not know your name yet. Tell me by saying: my name is <your name>.',
                source='langgraph-basic-ai',
            )

        if is_greeting(message):
            stored_name = await load_user_name(request.sessionId)
            span.set_attribute('orchestration.status', 'basic-ai-greeting')
            span.set_attribute('basic_ai.intent', 'greeting')
            span.add_event('basic_ai_greeting_handled')
            span.set_attribute('nats.publish.subject', 'chat.response')
            reply = 'Hello! How can I help you today?'
            if stored_name:
                reply = f'Hello {stored_name}! How can I help you today?'
            span.set_attribute('output.value', reply)
            span.set_attribute('output.mime_type', 'text/plain')
            return await publish_direct_response(
                session_id=request.sessionId,
                message=message,
                reply=reply,
                source='langgraph-basic-ai',
            )

        if redis_memory is not None:
            required_context_tag = extract_shift_bucket(message) if is_shift_analytics_query(message) else None
            cached_match = await redis_memory.search_similar(
                request.sessionId,
                message,
                required_context_tag=required_context_tag,
            )
            if cached_match is not None:
                span.set_attribute('orchestration.status', 'cache-hit')
                span.set_attribute('semantic_cache.hit', True)
                span.set_attribute('semantic_cache.score', cached_match.get('score', 0.0))
                span.set_attribute('cache.hit', True)
                span.set_attribute('cache.source', 'redis-semantic-cache')
                span.set_attribute('cache.score', cached_match.get('score', 0.0))
                if required_context_tag:
                    span.set_attribute('cache.shift', required_context_tag)
                span.set_attribute('output.value', str(cached_match.get('reply', '')))
                span.set_attribute('output.mime_type', 'text/plain')
                span.add_event('semantic_cache_hit')

                response_payload = {
                    'sessionId': request.sessionId,
                    'reply': cached_match.get('reply'),
                    'originalMessage': message,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'responseSource': 'redis-semantic-cache',
                    'cacheHit': True,
                    'similarityScore': cached_match.get('score'),
                    'matchedQuestion': cached_match.get('matchedQuestion'),
                }
                await publish_stream('chat.response', response_payload)
                span.set_attribute('nats.publish.subject', 'chat.response')
                return {
                    'status': 'served-from-cache',
                    'sessionId': request.sessionId,
                    'reply': cached_match.get('reply'),
                    'similarityScore': cached_match.get('score'),
                    'cacheHit': True,
                }

            span.set_attribute('semantic_cache.hit', False)

        shift_intent = is_shift_analytics_query(message)
        shift_node = graph.get_node('shift-agent')

        if shift_intent and shift_node is not None:
            selected_node = shift_node
            matched_nodes = [selected_node.id]
            span.set_attribute('orchestration.intent', 'shift-analytics')
        else:
            # Keep existing routing behavior but avoid routing to shift-worker unless
            # explicit shift-analytics intent is detected.
            non_shift_nodes = [node for node in graph.nodes.values() if node.id != 'shift-agent']
            matched_nodes = [node.id for node in non_shift_nodes if node.matches(message)]
            selected_node = (
                sorted(
                    [node for node in non_shift_nodes if node.matches(message)],
                    key=lambda node: node.priority,
                    reverse=True,
                )[0]
                if matched_nodes
                else None
            )
            span.set_attribute('orchestration.intent', 'standard-routing')

        span.set_attribute('orchestration.candidate_count', len(matched_nodes))
        span.set_attribute('orchestration.candidates', ','.join(matched_nodes) if matched_nodes else 'none')

        if selected_node:
            span.set_attribute('orchestration.status', 'forwarded')
            span.set_attribute('orchestration.target', selected_node.id)
            matched_keywords = [k for k in selected_node.match_keywords if k in message.lower()]
            span.set_attribute('orchestration.matched_keywords', ','.join(matched_keywords) if matched_keywords else 'none')
            span.set_attribute('output.value', f'forwarded:{selected_node.id}')
            span.set_attribute('output.mime_type', 'text/plain')
            span.add_event('orchestration_route_selected')

            payload = {
                'sessionId': request.sessionId,
                'message': message,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'orchestration': f'{selected_node.id}-routing',
                'nodeId': selected_node.id,
                'nodeName': selected_node.name,
                'responseSource': 'langgraph-routing',
            }
            await publish_stream(selected_node.route_name, payload)
            span.set_attribute('nats.publish.subject', selected_node.route_name)
            span.add_event('orchestration_forwarded')
            return {
                'status': 'forwarded',
                'target': selected_node.id,
                'sessionId': request.sessionId,
                'message': f'Question forwarded to {selected_node.name} for handling.',
                'cacheHit': False,
            }

        span.set_attribute('orchestration.status', 'declined')
        decline_text = (
            'Sorry, I cannot answer that question right now. '
            'Your feedback has been submitted to the product owner so that this functionality can be added.'
        )
        span.set_attribute('output.value', decline_text)
        span.set_attribute('output.mime_type', 'text/plain')
        response_payload = {
            'sessionId': request.sessionId,
            'reply': decline_text,
            'originalMessage': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'feedbackSubmitted': True,
            'declineReason': 'no_node_match',
            'responseSource': 'langgraph-direct',
            'cacheHit': False,
        }
        await publish_stream('chat.response', response_payload)
        span.set_attribute('nats.publish.subject', 'chat.response')
        span.set_attribute('orchestration.decline_reason', 'no_node_match')
        span.add_event('orchestration_declined')

        return {
            'status': 'declined',
            'sessionId': request.sessionId,
            'reply': decline_text,
            'feedbackSubmitted': True,
            'cacheHit': False,
        }
