import json
import os
from datetime import datetime

from fastapi import FastAPI, HTTPException
from nats.aio.client import Client as NATS
from pydantic import BaseModel

app = FastAPI()

LANGGRAPH_MODE = os.getenv('LANGGRAPH_MODE', 'local')
NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')

nc = NATS()
js = None

class OrchestrationRequest(BaseModel):
    sessionId: str
    task: str
    payload: dict


def requires_shift_handling(message: str) -> bool:
    normalized = message.lower()
    shift_keywords = [
        'shift',
        'schedule',
        'roster',
        'work time',
        'shift change',
        'shift swap',
        'shift start',
        'shift end',
        'time off',
    ]
    return any(keyword in normalized for keyword in shift_keywords)


def requires_kpi_handling(message: str) -> bool:
    normalized = message.lower()
    kpi_keywords = [
        'kpi',
        'metric',
        'performance',
        'goal',
        'target',
        'indicator',
        'dashboard',
        'trend',
    ]
    return any(keyword in normalized for keyword in kpi_keywords)


async def publish_stream(subject: str, payload: dict):
    global js
    if js is None:
        raise RuntimeError('JetStream client is not initialized')
    await js.publish(subject, json.dumps(payload).encode())


@app.on_event('startup')
async def startup_event():
    global js
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    for stream_name, subjects in [
        ('chat_incoming', ['chat.incoming']),
        ('chat_kpi', ['chat.kpi']),
        ('chat_response', ['chat.response']),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
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
    }


@app.post('/orchestrate')
async def orchestrate(request: OrchestrationRequest):
    message = str(request.payload.get('message', '')).strip()
    if not message:
        raise HTTPException(status_code=400, detail='message is required')

    if requires_kpi_handling(message):
        payload = {
            'sessionId': request.sessionId,
            'message': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'orchestration': 'kpi-routing',
        }
        await publish_stream('chat.kpi', payload)
        return {
            'status': 'forwarded',
            'target': 'kpi-worker',
            'sessionId': request.sessionId,
            'message': 'Question forwarded to kpi-worker for handling.',
        }

    if requires_shift_handling(message):
        payload = {
            'sessionId': request.sessionId,
            'message': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'orchestration': 'shift-routing',
        }
        await publish_stream('chat.incoming', payload)
        return {
            'status': 'forwarded',
            'target': 'shift-worker',
            'sessionId': request.sessionId,
            'message': 'Question forwarded to shift-worker for handling.',
        }

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
