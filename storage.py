"""Unified storage — PostgreSQL when available, in-memory fallback otherwise."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
SIMULATOR_API_KEY = os.getenv("SIMULATOR_API_KEY", "")
BOT_API_KEY = os.getenv("BOT_API_KEY", "")
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]

_pg_pool: asyncpg.Pool | None = None


def _pg_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def using_postgres() -> bool:
    return DATABASE_URL.startswith("postgres")


@dataclass
class MemoryStore:
    next_id: int = 1
    events: list[dict] = field(default_factory=list)
    sessions: list[dict] = field(default_factory=list)

    def create_session(self, sid: str, label: Optional[str]) -> dict:
        self.sessions = [s for s in self.sessions if s["id"] != sid]
        session = {
            "id": sid,
            "label": label,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }
        self.sessions.insert(0, session)
        return session

    def finish_session(self, sid: str) -> None:
        for s in self.sessions:
            if s["id"] == sid:
                s["finished_at"] = datetime.now(timezone.utc).isoformat()

    def add_event(self, data: dict) -> dict:
        event = {
            "id": self.next_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **data,
        }
        self.next_id += 1
        self.events.insert(0, event)
        return event

    def list_events(
        self,
        session_id: Optional[str] = None,
        since_id: Optional[int] = None,
        limit: int = 200,
    ) -> list[dict]:
        items = self.events
        if since_id is not None:
            items = [e for e in items if e["id"] > since_id]
            items.sort(key=lambda e: e["id"])
        elif session_id:
            items = [e for e in items if e.get("session_id") == session_id]
        else:
            items = sorted(items, key=lambda e: e["id"], reverse=True)
        return items[:limit]

    def get_event(self, event_id: int) -> Optional[dict]:
        return next((e for e in self.events if e["id"] == event_id), None)

    def update_event(self, event_id: int, detected: Optional[bool], blocked: Optional[bool]) -> Optional[dict]:
        event = self.get_event(event_id)
        if not event:
            return None
        if detected is not None:
            event["detected"] = detected
        if blocked is not None:
            event["blocked"] = blocked
        return event

    def clear_events(self, session_id: Optional[str] = None) -> None:
        if session_id:
            self.events = [e for e in self.events if e.get("session_id") != session_id]
        else:
            self.events = []

    def stats(self, session_id: Optional[str] = None) -> dict:
        items = self.events
        if session_id:
            items = [e for e in items if e.get("session_id") == session_id]
        by_module: dict[str, dict] = {}
        for e in items:
            m = e["module"]
            row = by_module.setdefault(m, {"module": m, "count": 0, "detected": 0, "blocked": 0})
            row["count"] += 1
            if e.get("detected"):
                row["detected"] += 1
            if e.get("blocked"):
                row["blocked"] += 1
        return {
            "total": len(items),
            "succeeded": sum(1 for e in items if e.get("status") == "success"),
            "failed": sum(1 for e in items if e.get("status") == "failed"),
            "blocked_status": sum(1 for e in items if e.get("status") == "blocked"),
            "detected": sum(1 for e in items if e.get("detected")),
            "blocked": sum(1 for e in items if e.get("blocked")),
            "by_module": list(by_module.values()),
        }


_memory = MemoryStore()


async def _get_pool() -> asyncpg.Pool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(_pg_url(DATABASE_URL), min_size=1, max_size=5)
    return _pg_pool


async def init_db() -> None:
    if not using_postgres():
        print("[storage] No PostgreSQL — using in-memory store")
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                module TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                detected BOOLEAN NOT NULL DEFAULT FALSE,
                blocked BOOLEAN NOT NULL DEFAULT FALSE,
                payload JSONB,
                error_message TEXT,
                session_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                label TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ
            )
        """)
    print("[storage] PostgreSQL ready")


def backend_name() -> str:
    return "postgresql" if using_postgres() else "memory"


def verify_simulator_key(provided: Optional[str]) -> bool:
    if not SIMULATOR_API_KEY:
        return True
    return provided == SIMULATOR_API_KEY


def verify_bot_key(provided: Optional[str]) -> bool:
    if not BOT_API_KEY:
        return True
    return provided == BOT_API_KEY


def _row_to_event(row: Any) -> dict:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    created = row["created_at"]
    if hasattr(created, "isoformat"):
        created = created.isoformat()
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
        "created_at": created,
    }


async def create_session(sid: str, label: Optional[str]) -> dict:
    if not using_postgres():
        return _memory.create_session(sid, label)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, label) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label",
            sid, label,
        )
    return {"id": sid}


async def finish_session(sid: str) -> None:
    if not using_postgres():
        _memory.finish_session(sid)
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET finished_at = NOW() WHERE id = $1", sid)


async def add_event(data: dict) -> dict:
    if not using_postgres():
        return _memory.add_event(data)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO events (module, action, status, detected, blocked, payload, error_message, session_id)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            RETURNING *
            """,
            data["module"], data["action"], data["status"], data["detected"], data["blocked"],
            json.dumps(data["payload"]) if data.get("payload") else None,
            data.get("error_message"), data.get("session_id"),
        )
    return _row_to_event(row)


async def list_events(
    session_id: Optional[str] = None,
    since_id: Optional[int] = None,
    limit: int = 200,
) -> list[dict]:
    if not using_postgres():
        return _memory.list_events(session_id, since_id, limit)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if since_id is not None:
            rows = await conn.fetch(
                "SELECT * FROM events WHERE id > $1 ORDER BY id ASC LIMIT $2", since_id, limit
            )
        elif session_id:
            rows = await conn.fetch(
                "SELECT * FROM events WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2",
                session_id, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM events ORDER BY created_at DESC LIMIT $1", limit
            )
    return [_row_to_event(r) for r in rows]


async def get_event(event_id: int) -> Optional[dict]:
    if not using_postgres():
        return _memory.get_event(event_id)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM events WHERE id = $1", event_id)
    return _row_to_event(row) if row else None


async def update_event(event_id: int, detected: Optional[bool], blocked: Optional[bool]) -> Optional[dict]:
    if not using_postgres():
        return _memory.update_event(event_id, detected, blocked)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM events WHERE id = $1", event_id)
        if not row:
            return None
        d = detected if detected is not None else row["detected"]
        b = blocked if blocked is not None else row["blocked"]
        updated = await conn.fetchrow(
            "UPDATE events SET detected = $1, blocked = $2 WHERE id = $3 RETURNING *",
            d, b, event_id,
        )
    return _row_to_event(updated)


async def get_stats(session_id: Optional[str] = None) -> dict:
    if not using_postgres():
        stats = _memory.stats(session_id)
        stats["storage"] = "memory"
        return stats
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if session_id:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) as total,
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
                SELECT module, COUNT(*) as count,
                    COUNT(*) FILTER (WHERE detected) as detected,
                    COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events WHERE session_id = $1 GROUP BY module
                """,
                session_id,
            )
        else:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) as total,
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
                SELECT module, COUNT(*) as count,
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
            {"module": r["module"], "count": r["count"], "detected": r["detected"], "blocked": r["blocked"]}
            for r in by_module
        ],
        "storage": "postgresql",
    }


async def clear_events(session_id: Optional[str] = None) -> None:
    if not using_postgres():
        _memory.clear_events(session_id)
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if session_id:
            await conn.execute("DELETE FROM events WHERE session_id = $1", session_id)
        else:
            await conn.execute("DELETE FROM events")
