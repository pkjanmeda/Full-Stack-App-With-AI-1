# Full Stack AI Chat Streaming App

This repository contains a modular full-stack application with:
- React web frontend chat interface
- Python FastAPI backend service to publish messages into NATS JetStream
- NATS JetStream backend for message streaming
- Python `shift-worker` that consumes messages and publishes randomized test responses
- Python `langgraph` orchestration service stub for local workflow coordination

## Local setup

1. Copy `.env.example` to `.env` and adjust values if needed.
2. Start the full stack:
   ```bash
   docker compose up --build
   ```
3. Open the frontend at `http://localhost:3000`.
4. Send a chat message and watch LangGraph decide whether to forward it to `shift-worker` or reply with product feedback.

## Services

- `frontend`: React chat UI
- `api`: REST endpoint to accept user chat messages
- `shift-worker`: JetStream consumer that processes shift-related chat questions and returns randomized responses
- `kpi-worker`: JetStream consumer that processes KPI-related chat questions and queries the Cosmos emulator for product KPI data
- `langgraph`: Python orchestration service for local LangGraph-style workflow
- `redis`: short-term conversation memory and semantic cache backend for `langgraph`
- `cosmos-emulator`: Cosmos DB emulator container with synthetic shift and KPI datasets
- `nats`: NATS JetStream broker

## Notes

- The API sends incoming questions to the LangGraph orchestrator first.
- The LangGraph orchestrator stores short-term conversation turns in Redis by session.
- Before routing, LangGraph performs semantic cache lookup in Redis and can return a cached response when similarity is high enough.
- The LangGraph orchestrator uses a lightweight local node graph to determine the correct AI agent node.
- Separate `shift-agent` and `kpi-agent` definitions are treated as graph nodes with their own subject routes.
- If the question is KPI-related, `kpi-worker` consumes the message from NATS and publishes to `chat.response`.
- If the question is shift-related, `shift-worker` consumes the message from NATS and publishes to `chat.response`.
- If the question is neither shift-related nor KPI-related, LangGraph publishes a decline response and feedback notice directly to `chat.response`.
- The `shift-worker` and `kpi-worker` services remain simple and produce randomized test responses.
- A separate Cosmos emulator project ships with `services/cosmos-emulator/cosmos-init.sql` for synthetic shift and KPI data.
- The first message is submitted through `POST /api/chat`, then the frontend upgrades to a WebSocket connection at `/api/chat/ws/{sessionId}`.
- The WebSocket channel is used for response streaming and optional follow-up chat submissions.
- LangGraph orchestration is local-only and does not require a LangChain API key in this setup.

## Tracing To Arize

The `api` and `langgraph` services include OpenTelemetry tracing.

By default, this repo runs Arize Phoenix locally in Docker and sends traces to it.

1. Start the stack:
   ```bash
   docker compose up --build
   ```
2. Open Phoenix UI at `http://localhost:6006`.

### Use Arize Cloud Instead (Optional)

Override OTLP endpoint and headers in your shell or `.env`:
   ```bash
   OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.arize.com/v1/traces
   OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer <ARIZE_API_KEY>
   ```

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to an empty value to disable telemetry export.
