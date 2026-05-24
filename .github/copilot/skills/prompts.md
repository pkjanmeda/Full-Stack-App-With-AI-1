# Prompt Engineering Standards

## Prompt Structure

Prompts should contain:
- role
- task
- constraints
- expected output schema
- examples when needed

## Output Formatting

Prefer:
- JSON outputs
- structured responses
- deterministic formatting

## Hallucination Reduction

- Ground responses in retrieved context.
- Require citations when appropriate.
- Ask for uncertainty explicitly.

## Token Management

- Keep prompts concise.
- Avoid unnecessary history replay.
- Summarize long conversations.

## Agent Prompts

Each agent should:
- have a single responsibility
- know available tools
- know forbidden behaviors