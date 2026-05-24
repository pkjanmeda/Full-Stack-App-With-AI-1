# Cosmos DB Standards

## SDK

Use:
- azure-cosmos
- async SDK when possible

## Client Lifecycle

- Create Cosmos client once.
- Reuse database/container references.
- Avoid recreating clients per request.

## Partitioning

- Design partition keys carefully.
- Optimize for query locality.
- Avoid hot partitions.

## Document Design

Documents should:
- include schema version
- include timestamps
- support optimistic concurrency

Example fields:
- id
- tenant_id
- workflow_id
- created_at
- updated_at
- schema_version

## Querying

- Prefer point reads over cross-partition queries.
- Avoid SELECT * in production.
- Use parameterized queries.

## Resilience

Handle:
- 429 throttling
- transient network failures
- retry-after headers

## Observability

Log:
- RU consumption
- latency
- partition key
- retry attempts