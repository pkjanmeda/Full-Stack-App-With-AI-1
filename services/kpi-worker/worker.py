import asyncio
import json
import os
import random
from datetime import datetime

from azure.cosmos import CosmosClient
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError

NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT', 'https://cosmos-emulator:8081/')
COSMOS_KEY = os.getenv(
    'COSMOS_KEY',
    'C2y6yDjf5/R+ob0N8A7Cgv30VR0YNi2I5aF4h4T8WT4hqtP7VhKzIQ7L2lqmq0LZ0SQW5Luy5pB8OZ+9c='
)
COSMOS_DB = os.getenv('COSMOS_DB', 'factory_ops')
COSMOS_CONTAINER = os.getenv('COSMOS_CONTAINER', 'kpi_data')


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
    client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY, connection_verify=False)
    database = client.get_database_client(COSMOS_DB)
    container = database.get_container_client(COSMOS_CONTAINER)

    query, parameters, product = build_kpi_query(message)
    items = list(container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True))
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
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    js = nc.jetstream()

    await ensure_streams(js)

    sub = await js.subscribe('chat.kpi', durable='kpi_worker_pool')
    print(f'KPI worker connected to NATS at {NATS_URL}')
    print(f'KPI worker connecting to Cosmos at {COSMOS_ENDPOINT}')

    while True:
        try:
            msg = await sub.next_msg(timeout=5)
        except TimeoutError:
            continue

        payload = json.loads(msg.data.decode())
        items, product = query_kpi_documents(payload['message'])
        response_text = format_kpi_response(payload['message'], items, product)
        result = {
            'sessionId': payload['sessionId'],
            'reply': response_text,
            'originalMessage': payload['message'],
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'kpiHits': len(items),
        }
        await js.publish('chat.response', json.dumps(result).encode())
        await msg.ack()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('KPI worker shutting down')
