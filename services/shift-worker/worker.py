import asyncio
import json
import os
import random
from datetime import datetime

from nats.aio.client import Client as NATS
from nats.errors import TimeoutError

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')

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

        payload = json.loads(msg.data.decode())
        response_text = random_response(payload['message'])
        result = {
            'sessionId': payload['sessionId'],
            'reply': response_text,
            'originalMessage': payload['message'],
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }
        await js.publish('chat.response', json.dumps(result).encode())
        await msg.ack()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Worker shutting down')
