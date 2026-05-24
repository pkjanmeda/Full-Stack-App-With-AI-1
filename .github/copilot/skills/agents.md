# Multi-Agent Design Standards

## Agent Responsibilities

Agents must have:
- clear scope
- isolated responsibilities
- explicit contracts

Avoid:
- monolithic agents
- hidden orchestration
- shared mutable memory

## Recommended Agent Types

- Planner Agent
- Research Agent
- Tool Execution Agent
- Validation Agent
- Critic Agent
- Memory Agent
- Summarization Agent

## Communication

- Use structured payloads between agents.
- Avoid passing raw prompts between agents.
- Use normalized schemas.

## Memory

Separate:
- short-term workflow state
- long-term memory
- vector memory
- execution metadata

## Tool Usage

Agents should:
- validate tool inputs
- handle tool failures gracefully
- avoid tool chaining explosion

## Human-in-the-loop

Support:
- approvals
- overrides
- escalation
- interruption/resume