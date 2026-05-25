# Phoenix Dataset Setup Guide

This repo includes a starter eval dataset at:
- datasets/evals/starter-evals.jsonl

## 1. Install and configure ax CLI (one time)

This guide is for managed datasets in an Arize space. The local Phoenix container at `http://localhost:6006` does not expose the `spaces` and `datasets` API used by `ax datasets`.

Windows PowerShell:
- pip install arize-ax-cli

Then create and activate a profile (interactive):
- ax profiles create

Check profile status:
- ax profiles show

Note:
- If no active profile is configured, `ax` commands against Arize resources will fail.

## 2. Pick target space

- ax spaces list -o json --limit 20

Copy the space name (or ID) you want to use.

If you are staying fully local, skip this guide and use `python scripts/run_local_phoenix_regression.py` with the JSONL files in `datasets/evals/`.

## 3. Create dataset from starter file

- ax datasets create --name "factory-chat-evals-v1" --space YOUR_SPACE --file datasets/evals/starter-evals.jsonl

If the dataset already exists, append new rows instead:
- ax datasets append "factory-chat-evals-v1" --space YOUR_SPACE --file datasets/evals/starter-evals.jsonl

## 4. Verify dataset

- ax datasets get "factory-chat-evals-v1" --space YOUR_SPACE -o json
- ax datasets export "factory-chat-evals-v1" --space YOUR_SPACE --stdout | jq 'length'

## 5. Append more examples later

- ax datasets append "factory-chat-evals-v1" --space YOUR_SPACE --file datasets/evals/starter-evals.jsonl

Or append inline JSON:
- ax datasets append "factory-chat-evals-v1" --space YOUR_SPACE --json '[{"input":"sample","expected_route":"decline"}]'

## 6. Quick eval workflow after dataset creation

1. Generate fresh traces from the app (run a few chats through frontend/API).
2. In Phoenix, create an evaluator using dataset fields:
  - input: input
  - expected route/source: expected_route or expected_source
3. Compare against trace fields:
  - eval.response.source
  - cache.hit
  - cache.source
4. Save baseline metrics and rerun after code changes.

## Recommended columns in this project

Core fields (already in starter file):
- input
- expected_route
- expected_source
- expected_decline
- cache_eligible
- expected_contains

Optional fields you can add:
- expected_cache_hit
- expected_keywords
- severity
- notes

## Suggested eval mappings in Phoenix

For dataset-driven evaluators:
- input: input
- expected class: expected_route or expected_source
- expected binary: expected_decline

For trace-joined evaluators:
- compare expected_source with eval.response.source
- compare expected_decline with observed response source and content
- compare expected_cache_hit with cache.hit

## Practical workflow

1. Start with this static dataset for routing and decline correctness.
2. Export production traces and build a v2 dataset with real difficult examples.
3. Keep separate datasets for:
- routing correctness
- cache-hit appropriateness
- response quality (LLM judge)

## Troubleshooting

- If ax command is not found:
  - pip install arize-ax-cli
- If unauthorized:
  - ax profiles show
  - recreate/update profile with `ax profiles create`
