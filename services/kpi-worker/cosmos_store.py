import time
from typing import List

from azure.core.exceptions import ServiceRequestError
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError


class CosmosKpiStore:
    def __init__(
        self,
        endpoint: str,
        key: str,
        database_name: str,
        container_name: str,
        partition_key_path: str,
        seed_documents: List[dict],
    ):
        self.endpoint = endpoint
        self.key = key
        self.database_name = database_name
        self.container_name = container_name
        self.partition_key_path = partition_key_path
        self.seed_documents = seed_documents

    def _create_client(self):
        try:
            return CosmosClient(self.endpoint, credential=self.key, connection_verify=False)
        except (AttributeError, Exception) as e:
            raise ServiceRequestError(f"Failed to create Cosmos client: {e}") from e

    def _get_container(self):
        client = self._create_client()
        database = client.create_database_if_not_exists(id=self.database_name)
        return database.create_container_if_not_exists(
            id=self.container_name,
            partition_key=PartitionKey(path=self.partition_key_path),
        )

    def _seed_if_empty(self, container):
        count_query = 'SELECT VALUE COUNT(1) FROM c'
        count_items = list(container.query_items(query=count_query, enable_cross_partition_query=True))
        existing_count = count_items[0] if count_items else 0
        if existing_count > 0:
            return

        for doc in self.seed_documents:
            container.upsert_item(doc)

    def ensure_resources_with_retry(self, retries: int = 10, delay_seconds: float = 2.0):
        for attempt in range(1, retries + 1):
            try:
                container = self._get_container()
                self._seed_if_empty(container)
                print(f'Cosmos resources ready: {self.database_name}.{self.container_name}')
                return True
            except ServiceRequestError as exc:
                if attempt == retries:
                    print(f'Cosmos bootstrap failed after {retries} attempts: {exc}')
                    return False
                print(f'Cosmos bootstrap waiting (attempt {attempt}/{retries}): {exc}')
                time.sleep(delay_seconds)

    def query_with_retry(self, query: str, parameters: list, retries: int = 5, delay_seconds: float = 2.0):
        for attempt in range(1, retries + 1):
            try:
                container = self._get_container()
                return list(
                    container.query_items(
                        query=query,
                        parameters=parameters,
                        enable_cross_partition_query=True,
                    )
                )
            except CosmosResourceNotFoundError as exc:
                print(f'Cosmos resource missing, bootstrapping: {exc}')
                self.ensure_resources_with_retry(retries=3, delay_seconds=1.0)
                container = self._get_container()
                return list(
                    container.query_items(
                        query=query,
                        parameters=parameters,
                        enable_cross_partition_query=True,
                    )
                )
            except ServiceRequestError as exc:
                if attempt == retries:
                    print(f'Cosmos query failed after {retries} attempts: {exc}')
                    return []
                print(f'Cosmos not ready (attempt {attempt}/{retries}): {exc}')
                time.sleep(delay_seconds)

        return []
