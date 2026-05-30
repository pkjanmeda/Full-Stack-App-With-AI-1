import argparse
import json
import os
import re

from azure.cosmos import CosmosClient, PartitionKey

CREATE_DB_RE = re.compile(r"CREATE DATABASE IF NOT EXISTS\s+([A-Za-z0-9_-]+)", re.IGNORECASE)
CREATE_CONTAINER_RE = re.compile(
    r"CREATE CONTAINER IF NOT EXISTS\s+([A-Za-z0-9_-]+)\s+WITH PARTITION KEY\s+(/[-A-Za-z0-9_]+)",
    re.IGNORECASE,
)
INSERT_RE = re.compile(r"INSERT INTO\s+([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)\s+VALUES\s+(.+)$", re.IGNORECASE)


def parse_init_file(path):
    database_names = set()
    containers = {}
    inserts = []

    with open(path, 'r', encoding='utf-8') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith('--'):
                continue

            db_match = CREATE_DB_RE.match(line)
            if db_match:
                database_names.add(db_match.group(1))
                continue

            container_match = CREATE_CONTAINER_RE.match(line)
            if container_match:
                containers[container_match.group(1)] = {
                    'partitionKey': container_match.group(2),
                }
                continue

            insert_match = INSERT_RE.match(line)
            if insert_match:
                db_name = insert_match.group(1)
                container_name = insert_match.group(2)
                document_text = insert_match.group(3).strip()
                if document_text.endswith(';'):
                    document_text = document_text[:-1].strip()
                try:
                    document = json.loads(document_text)
                except json.JSONDecodeError as exc:
                    raise ValueError(f'Invalid JSON document in line: {line}') from exc

                inserts.append({
                    'database': db_name,
                    'container': container_name,
                    'document': document,
                })
                continue

            raise ValueError(f'Unsupported line in init file: {line}')

    return {
        'databases': sorted(database_names),
        'containers': containers,
        'inserts': inserts,
    }


def main():
    parser = argparse.ArgumentParser(description='Load synthetic Cosmos DB emulator data.')
    parser.add_argument('init_file', help='Path to the SQL initialization file')
    parser.add_argument(
        '--endpoint',
        default=os.getenv('COSMOS_ENDPOINT', 'https://localhost:8081/'),
        help='Cosmos DB emulator endpoint',
    )
    parser.add_argument(
        '--key',
        default=os.getenv(
            'COSMOS_KEY',
            'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='
        ),
        help='Cosmos DB account key',
    )
    args = parser.parse_args()

    init_data = parse_init_file(args.init_file)
    client = CosmosClient(args.endpoint, credential=args.key, connection_verify=False)

    containers_by_db = {}
    for container_name, info in init_data['containers'].items():
        partition_key = info['partitionKey']
        containers_by_db[container_name] = partition_key

    for db_name in init_data['databases']:
        client.create_database_if_not_exists(id=db_name)

    for container_name, partition_info in containers_by_db.items():
        db_name = list(init_data['databases'])[0] if len(init_data['databases']) == 1 else None
        if db_name is None:
            raise RuntimeError('Multiple databases declared; load_data.py currently supports one database.')
        database = client.get_database_client(db_name)
        database.create_container_if_not_exists(
            id=container_name,
            partition_key=PartitionKey(path=partition_info),
        )

    for insert in init_data['inserts']:
        database = client.get_database_client(insert['database'])
        container = database.get_container_client(insert['container'])
        container.upsert_item(insert['document'])
        print(f"Inserted document {insert['document'].get('id')} into {insert['database']}.{insert['container']}")

    print('Synthetic Cosmos DB data load complete.')


if __name__ == '__main__':
    main()
