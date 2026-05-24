# AI Engineering Standards

This repository contains a multi-agent AI orchestration platform built using:
- Python 3.12+
- LangGraph
- LangChain
- Azure Cosmos DB
- Async-first architecture

## General Requirements

- Prefer async/await everywhere.
- Strong typing is mandatory.
- Use Pydantic models for all state objects.
- Avoid global mutable state.
- Prefer composition over inheritance.
- All agent outputs must be structured.
- Use deterministic workflows where possible.
- Minimize hidden side effects.
- Fail fast with actionable error messages.

## Code Style

- Use type hints for all functions.
- Use dataclasses or Pydantic models for state.
- Keep functions under 50 lines when practical.
- Prefer pure functions.
- Add docstrings for public APIs.
- Avoid deeply nested conditionals.

## Logging

- Use structured logging.
- Include correlation IDs.
- Never log secrets or tokens.

## Security

- Never hardcode secrets.
- Use Azure Managed Identity when possible.
- Store configuration in environment variables.

## Performance

- Reuse Cosmos DB clients.
- Reuse LLM clients.
- Avoid unnecessary serialization.
- Batch Cosmos operations where possible.

## Testing

- All workflows should have unit tests.
- Mock LLM calls in tests.
- Use deterministic test fixtures.