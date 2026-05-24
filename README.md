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
- `kpi-worker`: JetStream consumer that processes KPI-related chat questions and returns randomized KPI facts plus the original question
- `langgraph`: Python orchestration service for local LangGraph-style workflow
- `nats`: NATS JetStream broker

## Notes

- The API sends incoming questions to the LangGraph orchestrator first.
- The LangGraph orchestrator uses static rules to decide whether to forward the question to `shift-worker` or `kpi-worker`.
- If the question is KPI-related, `kpi-worker` consumes the message from NATS and publishes to `chat.response`.
- If the question is shift-related, `shift-worker` consumes the message from NATS and publishes to `chat.response`.
- If the question is neither shift-related nor KPI-related, LangGraph publishes a decline response and feedback notice directly to `chat.response`.
- The `shift-worker` and `kpi-worker` services remain simple and produce randomized test responses.
- The frontend connects to `/api/chat/stream` for server-sent events.
- LangGraph orchestration is local-only and does not require a LangChain API key in this setup.
