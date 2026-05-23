import os

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

LANGGRAPH_MODE = os.getenv('LANGGRAPH_MODE', 'local')

class OrchestrationRequest(BaseModel):
    sessionId: str
    task: str
    payload: dict


@app.get('/health')
async def health():
    return {
        'status': 'langgraph-orchestrator',
        'mode': LANGGRAPH_MODE,
    }


@app.post('/orchestrate')
async def orchestrate(request: OrchestrationRequest):
    return {
        'status': 'ok',
        'sessionId': request.sessionId,
        'task': request.task,
        'instructions': f"Execute the task '{request.task}' for session {request.sessionId} using local orchestration.",
        'payload': request.payload,
    }
