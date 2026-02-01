import os
import json
import base64
import asyncio
from typing import Dict

import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

AWS_REGION = os.environ.get("AWS_REGION", "us-west-1")
WS_API_ENDPOINT = os.environ.get("WS_API_ENDPOINT")  # e.g. https://{api-id}.execute-api.{region}.amazonaws.com/prod

apigw = None
if WS_API_ENDPOINT:
    apigw = boto3.client(
        "apigatewaymanagementapi",
        endpoint_url=WS_API_ENDPOINT,
        region_name=AWS_REGION
    )

# ---- Models ----
class StartReq(BaseModel):
    sessionId: str
    connectionId: str
    targetLang: str = "en"
    voiceId: str = "Joanna"

class IngestReq(BaseModel):
    sessionId: str
    seq: int
    pcm16: str               # base64
    sampleRate: int = 16000

class StopReq(BaseModel):
    sessionId: str

# ---- Session state ----
class SessionState:
    def __init__(self, connection_id: str, target_lang: str, voice_id: str):
        self.connection_id = connection_id
        self.target_lang = target_lang
        self.voice_id = voice_id
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.task: asyncio.Task | None = None
        # TODO: self.gemini = ... (streaming client/session)
        # TODO: buffering/phrase boundary detection

sessions: Dict[str, SessionState] = {}

def ws_send(connection_id: str, message: dict):
    if not apigw:
        print("WS_API_ENDPOINT not set; cannot post_to_connection. Message:", message)
        return

    apigw.post_to_connection(
        ConnectionId=connection_id,
        Data=json.dumps(message).encode("utf-8")
    )


@app.post("/session/start")
async def session_start(req: StartReq):
    if req.sessionId in sessions:
        raise HTTPException(status_code=409, detail="session already exists")

    st = SessionState(req.connectionId, req.targetLang, req.voiceId)
    sessions[req.sessionId] = st

    # TODO: st.task = asyncio.create_task(run_gemini_loop(req.sessionId, st))

    ws_send(req.connectionId, {"type": "broker_ready", "sessionId": req.sessionId})
    return {"ok": True}

@app.post("/ingest")
async def ingest(req: IngestReq):
    st = sessions.get(req.sessionId)
    if not st:
        raise HTTPException(status_code=404, detail="unknown session")

    audio_bytes = base64.b64decode(req.pcm16)
    await st.queue.put(audio_bytes)
    return {"ok": True}

@app.post("/session/stop")
async def session_stop(req: StopReq):
    st = sessions.pop(req.sessionId, None)
    if not st:
        return {"ok": True}

    if st.task:
        st.task.cancel()

    ws_send(st.connection_id, {"type": "broker_stopped", "sessionId": req.sessionId})
    return {"ok": True}

@app.get("/health")
def health():
    return {"ok": True}