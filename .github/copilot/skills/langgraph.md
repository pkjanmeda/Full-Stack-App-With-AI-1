# LangGraph Development Standards

## Graph Design

- Keep nodes single-purpose.
- Nodes should transform state predictably.
- Prefer explicit edges over dynamic routing.
- Avoid circular graphs unless intentional.

## State Management

- Use TypedDict or Pydantic models.
- State updates should be additive and explicit.
- Never mutate nested state in-place.

## Node Design

Each node should:
- accept state
- return partial updated state
- avoid direct external side effects

Example:

```python
async def planner_node(state: AgentState) -> dict:
    plan = await planner.generate(state.user_goal)

    return {
        "execution_plan": plan
    }
```

## Routing

Use conditional edges for:

- retries
- approvals
- escalation
- tool selection

## Error Handling
- Handle transient LLM failures.
- Add retry policies.
- Capture node execution metadata.

## Persistence
- Persist checkpoints for long-running workflows.
- Store resumable execution state in Cosmos DB.