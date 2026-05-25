# Phoenix Evals Runbook

This runbook gives a repeatable sequence to create a dataset and run evals for this project.

Use the dataset steps below only with an Arize space/profile. The repo's local Phoenix container does not implement the cloud `spaces` or `datasets` endpoints that `ax` expects.

## Prerequisites

1. ax CLI installed and available:
- ax --version

2. Active Arize profile:
- ax profiles create
- ax profiles show

3. Running app stack with trace emission:
- docker compose up -d

For a local-only regression run without managed datasets, use `python scripts/run_local_phoenix_regression.py` instead.

## Step 1: Create or update dataset

Pick a target space:
- ax spaces list -o json --limit 20

Create starter dataset:
- ax datasets create --name "factory-chat-evals-v1" --space YOUR_SPACE --file datasets/evals/starter-evals.jsonl

If it already exists:
- ax datasets append "factory-chat-evals-v1" --space YOUR_SPACE --file datasets/evals/starter-evals.jsonl

Verify row count:
- ax datasets export "factory-chat-evals-v1" --space YOUR_SPACE --stdout | jq 'length'

## Step 2: Generate traces for evaluation

Run chats through frontend or API to produce fresh traces with these fields:
- eval.input.normalized_text
- eval.output.normalized_text
- eval.response.source
- cache.hit
- cache.source
- cache.score

## Step 3: Build eval mappings in Phoenix

Recommended mapping:
- Input column: input
- Predicted source: eval.response.source
- Expected source: expected_source
- Expected decline: expected_decline
- Cache checks: cache.hit, cache.source

## Step 4: Starter eval suite

1. Routing correctness
- Compare expected_source vs eval.response.source

2. Decline correctness
- expected_decline against langgraph-direct behavior

3. Cache behavior
- Verify cache.hit true only when appropriate
- Track cache.score distribution for hits

4. Response quality (LLM judge)
- Judge eval.output.normalized_text against input for relevance/helpfulness

## Step 5: Regression gate suggestions

Fail or block release if:
- Routing accuracy < 95%
- Cache appropriateness < 90%
- Relevance average < 4.0/5

## Notes

- Use normalized fields for stable evaluator prompts.
- Keep raw fields for forensic debugging.
- Evaluate cache-hit and non-cache paths separately.
