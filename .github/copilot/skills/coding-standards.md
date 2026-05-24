# Python Coding Standards

## Python

- Target Python 3.12+
- Use uv or poetry
- Use Ruff for linting
- Use mypy for type checking

## Preferred Libraries

- pydantic
- tenacity
- structlog
- pytest
- httpx

## Async Standards

- Prefer async I/O
- Never block event loop
- Use asyncio.gather carefully

## Error Handling

- Use custom exception types
- Preserve stack traces
- Avoid silent failures