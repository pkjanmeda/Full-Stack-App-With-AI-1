import asyncio
import json
import os
import random
from datetime import datetime

import httpx
from nats.aio.client import Client as NATS
from nats.js.errors import JetStreamError

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
LANGGRAPH_URL = os.getenv('LANGGRAPH_URL', 'http://langgraph:5000')

async def ensure_streams(js):
    for stream_name, subjects in [
        ('chat_incoming', ['chat.incoming']),
        ('chat_response', ['chat.response']),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except JetStreamError:
            pass


def random_response(message: str) -> str:
    suffixes = [
        '— this is your randomized answer.',
        'and the system thinks it sounds interesting.',
        'with a little extra magic from Python.',
    ]
    return f'{message} {random.choice(suffixes)}'


async def orchestrate(session_id: str, task: str, payload: dict) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f'{LANGGRAPH_URL}/orchestrate',
            json={
                'sessionId': session_id,
                'task': task,
                'payload': payload,
            },
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()


async def main():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    await ensure_streams(js)

    sub = await js.subscribe('chat.incoming', durable='worker_pool', ack_wait=30, ack_policy='explicit')
    print(f'Worker connected to NATS at {NATS_URL}')

    async for msg in sub:
        payload = json.loads(msg.data.decode())
        orchestration = await orchestrate(
            payload['sessionId'],
            'randomize-text',
            {'message': payload['message']},
        )
        response_text = f"{orchestration.get('instructions', '')} {random_response(payload['message'])}"
        result = {
            'sessionId': payload['sessionId'],
            'reply': response_text,
            'originalMessage': payload['message'],
            'orchestration': orchestration,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }
        await js.publish('chat.response', json.dumps(result).encode())
        await msg.ack()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Worker shutting down')
