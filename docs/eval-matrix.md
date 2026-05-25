# Starter Eval Matrix

This document defines a practical baseline for Phoenix evals for this project.

Companion setup/runbook:
- docs/phoenix-dataset-setup.md
- docs/phoenix-evals-runbook.md

## Scope

Evaluate chat behavior end-to-end across:
- API gateway and WebSocket stream delivery
- LangGraph orchestration and routing
- Redis semantic cache behavior
- Shift/KPI worker responses

## Eval-Ready Trace Fields

The API now emits a dedicated final-answer span per delivered response:
- Span name: api.final_answer
- Input fields:
  - eval.input.raw
  - eval.input.normalized_text
- Output fields:
  - eval.output.raw
  - eval.output.normalized_text
- Context fields:
  - eval.response.source
  - cache.hit
  - cache.source
  - cache.score (when available)
  - chat.session_id

Use these fields as the primary extraction surface for eval datasets.

## Eval Matrix

| Eval Category | Question | Dataset Slice | Signal Type | Pass Target |
|---|---|---|---|---|
| Routing Correctness | Was the message routed to the expected path? | orchestration spans grouped by orchestration.target | Classification | >= 95% |
| Cache Appropriateness | When cache.hit=true, is output still valid for input? | final-answer spans where cache.hit=true | LLM judge / human label | >= 90% pass |
| Response Relevance | Does output answer user input? | all final-answer spans | LLM judge 1-5 | >= 4.0 avg |
| KPI Groundedness | Is KPI response consistent with expected KPI format and values? | eval.response.source=kpi-worker | Rule + judge | >= 95% pass |
| Decline Quality | Are declines clear and safe? | eval.response.source=langgraph-direct | Rule + judge | >= 98% pass |
| Stream UX | Is first chunk latency acceptable? | api.ws.emit spans | Numeric threshold | P95 <= target |
| Stream Completion | Does stream complete without truncation? | api.ws.emit spans with stream.word_count | Rule | >= 99% pass |

## Suggested Dataset Construction

1. Export trace slices by route/source:
- eval.response.source in [kpi-worker, shift-worker, langgraph-direct, redis-semantic-cache]

2. Ensure balanced test set:
- 25% shift
- 25% KPI
- 25% decline
- 25% cache-hit candidates

3. Build a gold subset (50-100 rows):
- input
- expected route class
- expected response characteristics

## Phoenix Eval Setup Steps

1. Create a dataset from api.final_answer spans.
2. Map columns:
- input: eval.input.normalized_text
- output: eval.output.normalized_text
- metadata: cache.hit, eval.response.source, cache.score
3. Add evaluators:
- route classifier evaluator
- cache appropriateness evaluator
- relevance evaluator
- latency threshold evaluator (from api.ws.emit)
4. Save experiment as baseline and compare all future runs to baseline.

## Regression Gate Proposal

Fail release if any of the following happen:
- Routing correctness drops by > 2%
- Cache appropriateness drops below 90%
- Relevance score drops below 4.0
- P95 first-chunk latency increases by > 20%

## Notes

- Prefer normalized fields for eval prompts and scoring.
- Keep raw fields for forensic analysis and debugging.
- Track cache-hit and non-cache performance separately to avoid hiding regressions.
