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
4. Send a chat message and watch `shift-worker` publish responses through JetStream.

## Services

- `frontend`: React chat UI
- `api`: REST endpoint to accept user chat messages
- `shift-worker`: JetStream consumer that processes inbound chat messages and returns randomized responses
- `langgraph`: Python orchestration service for local LangGraph-style workflow
- `nats`: NATS JetStream broker

## Notes

- The API publishes inbound messages to `chat.incoming`.
- `shift-worker` consumes from NATS and publishes to `chat.response`.
- `shift-worker` currently appends randomized test text to each request payload.
- The `langgraph` service represents the orchestration layer, but the worker itself remains simple.
- The frontend connects to `/api/chat/stream` for server-sent events.
- LangGraph orchestration is local-only and does not require a LangChain API key in this setup.
