# Observability Standards

## Tracing

Capture:
- workflow_id
- correlation_id
- node execution duration
- token usage
- model latency

## Metrics

Track:
- success rate
- retries
- token cost
- tool failures
- graph execution time

## Logging

Use structured JSON logs.

Never log:
- secrets
- API keys
- sensitive prompts

## Debugging

Persist:
- graph transitions
- node outputs
- tool execution metadata