# Full Stack AI Chat Streaming App

This repository contains a modular full-stack application with:
- React web frontend chat interface
- Python FastAPI backend service to publish messages into NATS JetStream
- NATS JetStream backend for message streaming
- Python worker pool that consumes messages and produces randomized responses
- LangGraph worker stub for local development

## Local setup

1. Copy `.env.example` to `.env` and adjust values if needed.
2. Start the full stack:
   ```bash
   docker compose up --build
   ```
3. Open the frontend at `http://localhost:3000`.
4. Send a chat message and watch the worker publish responses through JetStream.

## Services

- `frontend`: React chat UI
- `api`: REST endpoint to accept user chat messages
- `worker`: JetStream consumer that processes messages and publishes responses
- `langgraph`: Python orchestration service for local LangGraph-style workflow
- `nats`: NATS JetStream broker

## Notes

- The API publishes inbound messages to `chat.incoming`.
- The worker consumes from NATS and publishes to `chat.response`.
- The worker calls the local LangGraph orchestrator at `/orchestrate`.
- The frontend connects to `/api/chat/stream` for server-sent events.
- LangGraph orchestration is local-only and does not require a LangChain API key in this setup.
