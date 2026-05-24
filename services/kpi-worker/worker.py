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
        ('chat_kpi', ['chat.kpi']),
        ('chat_response', ['chat.response']),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except Exception:
            pass


def random_kpi_response(message: str) -> str:
    facts = [
        'KPI fact: current utilization is 87% for this metric.',
        'KPI fact: average response time improved by 12%.',
        'KPI fact: throughput is trending at 97% of goal.',
    ]
    return f"{random.choice(facts)} {message}"


async def main():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    await ensure_streams(js)

    sub = await js.subscribe('chat.kpi', durable='kpi_worker_pool')
    print(f'KPI worker connected to NATS at {NATS_URL}')

    while True:
        try:
            msg = await sub.next_msg(timeout=5)
        except TimeoutError:
            continue

        payload = json.loads(msg.data.decode())
        response_text = random_kpi_response(payload['message'])
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
        print('KPI worker shutting down')
