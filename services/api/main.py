import asyncio
import json
import os
import uuid
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from nats.aio.client import Client as NATS

app = FastAPI()

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')

nc = NATS()
js = None

class ChatRequest(BaseModel):
    sessionId: str | None = None
    message: str


@app.on_event('startup')
async def startup_event():
    global js
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
    payload = {
        'sessionId': session_id,
        'message': message,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
    }

    await js.publish('chat.incoming', json.dumps(payload).encode())
    return {'status': 'queued', 'messageId': str(uuid.uuid4()), 'sessionId': session_id}


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
                    yield f'event: message\ndata: {json.dumps(data)}\n\n'
        finally:
            await sub.unsubscribe()

    return StreamingResponse(event_generator(), media_type='text/event-stream')
