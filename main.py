import json
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from database import (
    CORS_ORIGINS,
    get_pool,
    init_db,
    verify_bot_key,
    verify_simulator_key,
)


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


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="AntiVirus API",
    description="Event API for AV research simulator, website, and Discord bot",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "antivirus-api"}


@app.post("/api/sessions")
async def create_session(body: SessionCreate, x_api_key: Optional[str] = Header(None)):
    if not verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sessions (id, label) VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label
            """,
            body.id,
            body.label,
        )
    return {"id": body.id}


@app.patch("/api/sessions/{session_id}/finish")
async def finish_session(session_id: str, x_api_key: Optional[str] = Header(None)):
    if not verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE sessions SET finished_at = NOW() WHERE id = $1",
            session_id,
        )
    return {"id": session_id, "finished": True}


@app.post("/api/events")
async def create_event(body: EventCreate, x_api_key: Optional[str] = Header(None)):
    if not verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO events (module, action, status, detected, blocked, payload, error_message, session_id)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            RETURNING id, created_at
            """,
            body.module,
            body.action,
            body.status,
            body.detected,
            body.blocked,
            json.dumps(body.payload) if body.payload else None,
            body.error_message,
            body.session_id,
        )
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


@app.get("/api/events")
async def list_events(
    session_id: Optional[str] = None,
    since_id: Optional[int] = Query(None, description="Return events with id > since_id (for bot polling)"),
    limit: int = 200,
    x_api_key: Optional[str] = Header(None),
):
    if since_id is not None and not verify_bot_key(x_api_key):
        raise HTTPException(status_code=401, detail="Bot API key required for polling")
    pool = await get_pool()
    async with pool.acquire() as conn:
        if since_id is not None:
            rows = await conn.fetch(
                """
                SELECT * FROM events WHERE id > $1
                ORDER BY id ASC LIMIT $2
                """,
                since_id,
                limit,
            )
        elif session_id:
            rows = await conn.fetch(
                """
                SELECT * FROM events WHERE session_id = $1
                ORDER BY created_at DESC LIMIT $2
                """,
                session_id,
                limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM events ORDER BY created_at DESC LIMIT $1",
                limit,
            )
    return [_row_to_event(r) for r in rows]


@app.get("/api/events/{event_id}")
async def get_event(event_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM events WHERE id = $1", event_id)
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")
    return _row_to_event(row)


@app.patch("/api/events/{event_id}")
async def update_detection(event_id: int, body: DetectionUpdate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM events WHERE id = $1", event_id)
        if not row:
            raise HTTPException(status_code=404, detail="Event not found")
        detected = body.detected if body.detected is not None else row["detected"]
        blocked = body.blocked if body.blocked is not None else row["blocked"]
        updated = await conn.fetchrow(
            """
            UPDATE events SET detected = $1, blocked = $2
            WHERE id = $3 RETURNING *
            """,
            detected,
            blocked,
            event_id,
        )
    return _row_to_event(updated)


@app.get("/api/stats")
async def get_stats(session_id: Optional[str] = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if session_id:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'success') as succeeded,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'blocked') as blocked_status,
                    COUNT(*) FILTER (WHERE detected) as detected,
                    COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events WHERE session_id = $1
                """,
                session_id,
            )
            by_module = await conn.fetch(
                """
                SELECT module,
                       COUNT(*) as count,
                       COUNT(*) FILTER (WHERE detected) as detected,
                       COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events WHERE session_id = $1
                GROUP BY module
                """,
                session_id,
            )
        else:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'success') as succeeded,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'blocked') as blocked_status,
                    COUNT(*) FILTER (WHERE detected) as detected,
                    COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events
                """
            )
            by_module = await conn.fetch(
                """
                SELECT module,
                       COUNT(*) as count,
                       COUNT(*) FILTER (WHERE detected) as detected,
                       COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events GROUP BY module
                """
            )
    return {
        "total": row["total"],
        "succeeded": row["succeeded"],
        "failed": row["failed"],
        "blocked_status": row["blocked_status"],
        "detected": row["detected"],
        "blocked": row["blocked"],
        "by_module": [
            {
                "module": r["module"],
                "count": r["count"],
                "detected": r["detected"],
                "blocked": r["blocked"],
            }
            for r in by_module
        ],
    }


@app.delete("/api/events")
async def clear_events(session_id: Optional[str] = None, x_api_key: Optional[str] = Header(None)):
    if not verify_simulator_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        if session_id:
            await conn.execute("DELETE FROM events WHERE session_id = $1", session_id)
        else:
            await conn.execute("DELETE FROM events")
    return {"cleared": True}


def _row_to_event(row) -> dict:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return {
        "id": row["id"],
        "module": row["module"],
        "action": row["action"],
        "status": row["status"],
        "detected": row["detected"],
        "blocked": row["blocked"],
        "payload": payload,
        "error_message": row["error_message"],
        "session_id": row["session_id"],
        "created_at": row["created_at"].isoformat(),
    }
