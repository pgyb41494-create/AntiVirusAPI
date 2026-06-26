import json
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import storage


class EventCreate(BaseModel):
    module: str
    action: str
    status: str = Field(description="success | failed | blocked | simulated")
    detected: bool = False
    blocked: bool = False
    payload: Optional[dict] = None
    error_message: Optional[str] = None
    session_id: Optional[str] = None


class SessionCreate(BaseModel):
    id: str
    label: Optional[str] = None


class DetectionUpdate(BaseModel):
    detected: Optional[bool] = None
    blocked: Optional[bool] = None


class GuildWatchState(BaseModel):
    channel_id: int
    last_event_id: int = 0
    alert_role_id: Optional[int] = None
    alert_on_start: bool = True
    alert_on_blocked: bool = True


class GuildWatchesSync(BaseModel):
    watches: dict[str, GuildWatchState]


@asynccontextmanager
async def lifespan(_: FastAPI):
    await storage.init_db()
    yield


app = FastAPI(
    title="AntiVirus API",
    description="Event API for AV research simulator, website, and Discord bot",
    lifespan=lifespan,
)

_wildcard = storage.CORS_ORIGINS == ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _wildcard else storage.CORS_ORIGINS,
    allow_credentials=not _wildcard,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "service": "antivirus-api",
        "storage": storage.backend_name(),
    }


@app.post("/api/sessions")
async def create_session(body: SessionCreate, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await storage.create_session(body.id, body.label)


@app.patch("/api/sessions/{session_id}/finish")
async def finish_session(session_id: str, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    await storage.finish_session(session_id)
    return {"id": session_id, "finished": True}


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: str, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    session = await storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@app.post("/api/events")
async def create_event(body: EventCreate, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    event = await storage.add_event(body.model_dump())
    return {"id": event["id"], "created_at": event["created_at"]}


@app.get("/api/events")
async def list_events(
    session_id: Optional[str] = None,
    since_id: Optional[int] = Query(None),
    limit: int = 200,
    x_api_key: Optional[str] = Header(None),
):
    if since_id is not None and not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required for polling")
    return await storage.list_events(session_id, since_id, limit)


@app.get("/api/events/{event_id}")
async def get_event(event_id: int):
    event = await storage.get_event(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.patch("/api/events/{event_id}")
async def update_detection(event_id: int, body: DetectionUpdate):
    event = await storage.update_event(event_id, body.detected, body.blocked)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.get("/api/stats")
async def get_stats(session_id: Optional[str] = None):
    return await storage.get_stats(session_id)


@app.delete("/api/events")
async def clear_events(session_id: Optional[str] = None, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    await storage.clear_events(session_id)
    return {"cleared": True}


@app.get("/api/bot/watches")
async def get_bot_watches(x_api_key: Optional[str] = Header(None)):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    watches = await storage.get_guild_watches()
    return {"watches": watches}


@app.put("/api/bot/watches")
async def sync_bot_watches(body: GuildWatchesSync, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    payload = {
        gid: {
            "channel_id": w.channel_id,
            "last_event_id": w.last_event_id,
            "alert_role_id": w.alert_role_id,
            "alert_on_start": w.alert_on_start,
            "alert_on_blocked": w.alert_on_blocked,
        }
        for gid, w in body.watches.items()
    }
    await storage.sync_guild_watches(payload)
    return {"saved": len(payload)}
