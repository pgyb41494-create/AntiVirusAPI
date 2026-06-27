"""Unified storage — PostgreSQL when available, in-memory fallback otherwise."""

from __future__ import annotations

import asyncio
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
_liveview_cache: dict[str, dict] = {}
_memory_frame_seq: dict[str, int] = {}


def _pg_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def using_postgres() -> bool:
    return DATABASE_URL.startswith("postgres")


@dataclass
class MemoryStore:
    next_id: int = 1
    next_cmd_id: int = 1
    events: list[dict] = field(default_factory=list)
    sessions: list[dict] = field(default_factory=list)
    guild_watches: dict[str, dict] = field(default_factory=dict)
    heartbeats: dict[str, dict] = field(default_factory=dict)
    commands: list[dict] = field(default_factory=list)
    liveview: dict[str, dict] = field(default_factory=dict)
    liveview_frames: dict[str, dict] = field(default_factory=dict)

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
        if since_id is None:
            items = _exclude_liveview_stream(items)
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
        items = _exclude_liveview_stream(items)
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

_LIVEVIEW_STREAM_SQL = "NOT (module = 'liveview' AND action = 'screen_frame')"


def _is_liveview_stream_event(event: dict) -> bool:
    """Continuous live-preview frames — not logged to dashboard or Discord."""
    return event.get("module") == "liveview" and (event.get("action") or "") == "screen_frame"


def _exclude_liveview_stream(events: list[dict]) -> list[dict]:
    return [e for e in events if not _is_liveview_stream_event(e)]


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
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_guild_watches (
                guild_id TEXT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                last_event_id INTEGER NOT NULL DEFAULT 0,
                alert_role_id BIGINT,
                alert_on_start BOOLEAN NOT NULL DEFAULT TRUE,
                alert_on_blocked BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute(
            "ALTER TABLE discord_guild_watches ADD COLUMN IF NOT EXISTS alert_role_id BIGINT"
        )
        await conn.execute(
            "ALTER TABLE discord_guild_watches ADD COLUMN IF NOT EXISTS alert_on_start BOOLEAN NOT NULL DEFAULT TRUE"
        )
        await conn.execute(
            "ALTER TABLE discord_guild_watches ADD COLUMN IF NOT EXISTS alert_on_blocked BOOLEAN NOT NULL DEFAULT TRUE"
        )
        await conn.execute(
            "ALTER TABLE discord_guild_watches ADD COLUMN IF NOT EXISTS control_channel_id BIGINT"
        )
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS heartbeats (
                hostname TEXT PRIMARY KEY,
                username TEXT,
                last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                screen_width INTEGER,
                screen_height INTEGER
            )
        """)
        await conn.execute(
            "ALTER TABLE heartbeats ADD COLUMN IF NOT EXISTS screen_width INTEGER"
        )
        await conn.execute(
            "ALTER TABLE heartbeats ADD COLUMN IF NOT EXISTS screen_height INTEGER"
        )
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS remote_commands (
                id SERIAL PRIMARY KEY,
                hostname TEXT NOT NULL,
                module TEXT NOT NULL DEFAULT '',
                session_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                guild_id TEXT,
                command_kind TEXT NOT NULL DEFAULT 'module',
                payload JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute(
            "ALTER TABLE remote_commands ADD COLUMN IF NOT EXISTS command_kind TEXT NOT NULL DEFAULT 'module'"
        )
        await conn.execute(
            "ALTER TABLE remote_commands ADD COLUMN IF NOT EXISTS payload JSONB"
        )
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


async def get_session(sid: str) -> Optional[dict]:
    if not using_postgres():
        return next((s for s in _memory.sessions if s["id"] == sid), None)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sessions WHERE id = $1", sid)
    if not row:
        return None
    started = row["started_at"]
    finished = row["finished_at"]
    return {
        "id": row["id"],
        "label": row["label"],
        "started_at": started.isoformat() if hasattr(started, "isoformat") else started,
        "finished_at": finished.isoformat() if finished and hasattr(finished, "isoformat") else finished,
    }


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
    events = [_row_to_event(r) for r in rows]
    if since_id is None:
        events = _exclude_liveview_stream(events)
    return events


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
                FROM events WHERE session_id = $1 AND """ + _LIVEVIEW_STREAM_SQL,
                session_id,
            )
            by_module = await conn.fetch(
                """
                SELECT module, COUNT(*) as count,
                    COUNT(*) FILTER (WHERE detected) as detected,
                    COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events WHERE session_id = $1 AND """ + _LIVEVIEW_STREAM_SQL + " GROUP BY module",
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
                FROM events WHERE """ + _LIVEVIEW_STREAM_SQL
            )
            by_module = await conn.fetch(
                """
                SELECT module, COUNT(*) as count,
                    COUNT(*) FILTER (WHERE detected) as detected,
                    COUNT(*) FILTER (WHERE blocked) as blocked
                FROM events WHERE """ + _LIVEVIEW_STREAM_SQL + " GROUP BY module"
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


async def get_guild_watches() -> dict[str, dict]:
    """Discord bot linked channels — persisted across bot restarts."""
    if not using_postgres():
        return dict(_memory.guild_watches)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT guild_id, channel_id, last_event_id,
                   alert_role_id, alert_on_start, alert_on_blocked,
                   control_channel_id
            FROM discord_guild_watches
            """
        )
    return {
        str(r["guild_id"]): {
            "channel_id": int(r["channel_id"]),
            "last_event_id": int(r["last_event_id"]),
            "alert_role_id": int(r["alert_role_id"]) if r["alert_role_id"] else None,
            "alert_on_start": bool(r["alert_on_start"]) if r["alert_on_start"] is not None else True,
            "alert_on_blocked": bool(r["alert_on_blocked"]) if r["alert_on_blocked"] is not None else True,
            "control_channel_id": int(r["control_channel_id"]) if r.get("control_channel_id") else None,
        }
        for r in rows
    }


async def sync_guild_watches(watches: dict[str, dict]) -> None:
    """Replace stored guild watches with the bot's current state."""
    if not using_postgres():
        _memory.guild_watches = {
            gid: {
                "channel_id": int(data["channel_id"]),
                "last_event_id": int(data.get("last_event_id", 0)),
                "alert_role_id": data.get("alert_role_id"),
                "alert_on_start": bool(data.get("alert_on_start", True)),
                "alert_on_blocked": bool(data.get("alert_on_blocked", True)),
                "control_channel_id": int(data["control_channel_id"])
                if data.get("control_channel_id")
                else None,
            }
            for gid, data in watches.items()
        }
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetch("SELECT guild_id FROM discord_guild_watches")
            existing_ids = {str(r["guild_id"]) for r in existing}
            incoming_ids = set(watches.keys())
            for removed in existing_ids - incoming_ids:
                await conn.execute(
                    "DELETE FROM discord_guild_watches WHERE guild_id = $1", removed
                )
            for gid, data in watches.items():
                await conn.execute(
                    """
                    INSERT INTO discord_guild_watches (
                        guild_id, channel_id, last_event_id,
                        alert_role_id, alert_on_start, alert_on_blocked,
                        control_channel_id, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    ON CONFLICT (guild_id) DO UPDATE SET
                        channel_id = EXCLUDED.channel_id,
                        last_event_id = EXCLUDED.last_event_id,
                        alert_role_id = EXCLUDED.alert_role_id,
                        alert_on_start = EXCLUDED.alert_on_start,
                        alert_on_blocked = EXCLUDED.alert_on_blocked,
                        control_channel_id = EXCLUDED.control_channel_id,
                        updated_at = NOW()
                    """,
                    gid,
                    int(data["channel_id"]),
                    int(data.get("last_event_id", 0)),
                    int(data["alert_role_id"]) if data.get("alert_role_id") else None,
                    bool(data.get("alert_on_start", True)),
                    bool(data.get("alert_on_blocked", True)),
                    int(data["control_channel_id"]) if data.get("control_channel_id") else None,
                )


async def upsert_heartbeat(
    hostname: str,
    username: str,
    screen_width: int | None = None,
    screen_height: int | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    if not using_postgres():
        _memory.heartbeats[hostname] = {
            "hostname": hostname,
            "username": username,
            "last_seen": now,
            "screen_width": screen_width,
            "screen_height": screen_height,
        }
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO heartbeats (hostname, username, last_seen, screen_width, screen_height)
            VALUES ($1, $2, NOW(), $3, $4)
            ON CONFLICT (hostname) DO UPDATE SET
                username = EXCLUDED.username,
                last_seen = NOW(),
                screen_width = EXCLUDED.screen_width,
                screen_height = EXCLUDED.screen_height
            """,
            hostname,
            username,
            screen_width,
            screen_height,
        )


async def list_online_hosts(minutes: int = 3) -> list[dict]:
    if not using_postgres():
        cutoff = datetime.now(timezone.utc).timestamp() - minutes * 60
        hosts = []
        for row in _memory.heartbeats.values():
            try:
                seen = datetime.fromisoformat(row["last_seen"].replace("Z", "+00:00"))
                if seen.timestamp() >= cutoff:
                    hosts.append(row)
            except (ValueError, KeyError):
                continue
        hosts.sort(key=lambda h: h.get("last_seen", ""), reverse=True)
        return hosts
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT hostname, username, last_seen, screen_width, screen_height
            FROM heartbeats
            WHERE last_seen >= NOW() - ($1 || ' minutes')::interval
            ORDER BY last_seen DESC
            """,
            str(minutes),
        )
    return [
        {
            "hostname": r["hostname"],
            "username": r["username"] or "",
            "last_seen": r["last_seen"].isoformat(),
            "screen_width": r["screen_width"],
            "screen_height": r["screen_height"],
        }
        for r in rows
    ]


async def create_remote_command(
    hostname: str,
    guild_id: Optional[str] = None,
    *,
    module: Optional[str] = None,
    command_kind: str = "module",
    payload: Optional[dict] = None,
) -> dict:
    import uuid

    session_id = ""
    mod = module or ""
    if command_kind == "module":
        if not mod:
            raise ValueError("module is required for module commands")
        session_id = str(uuid.uuid4())
        await create_session(session_id, label=f"remote:{hostname}")
    if not using_postgres():
        cmd = {
            "id": _memory.next_cmd_id,
            "hostname": hostname,
            "module": mod,
            "session_id": session_id,
            "status": "pending",
            "guild_id": guild_id,
            "command_kind": command_kind,
            "payload": payload or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _memory.next_cmd_id += 1
        _memory.commands.append(cmd)
        return cmd
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO remote_commands (
                hostname, module, session_id, status, guild_id, command_kind, payload
            )
            VALUES ($1, $2, $3, 'pending', $4, $5, $6::jsonb)
            RETURNING id, hostname, module, session_id, status, guild_id,
                      command_kind, payload, created_at
            """,
            hostname,
            mod,
            session_id,
            guild_id,
            command_kind,
            json.dumps(payload or {}),
        )
    pl = row["payload"]
    if isinstance(pl, str):
        pl = json.loads(pl) if pl else {}
    return {
        "id": int(row["id"]),
        "hostname": row["hostname"],
        "module": row["module"],
        "session_id": row["session_id"],
        "status": row["status"],
        "guild_id": row["guild_id"],
        "command_kind": row["command_kind"],
        "payload": pl or {},
        "created_at": row["created_at"].isoformat(),
    }


async def list_pending_commands(hostname: str) -> list[dict]:
    if not using_postgres():
        pending = [
            c
            for c in _memory.commands
            if c["hostname"] == hostname and c["status"] == "pending"
        ]
        pending.sort(key=lambda c: c["id"])
        return pending
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, hostname, module, session_id, status, command_kind, payload, created_at
            FROM remote_commands
            WHERE hostname = $1 AND status = 'pending'
            ORDER BY id ASC
            """,
            hostname,
        )
    out = []
    for r in rows:
        pl = r["payload"]
        if isinstance(pl, str):
            pl = json.loads(pl) if pl else {}
        out.append(
            {
                "id": int(r["id"]),
                "hostname": r["hostname"],
                "module": r["module"],
                "session_id": r["session_id"],
                "status": r["status"],
                "command_kind": r.get("command_kind") or "module",
                "payload": pl or {},
                "created_at": r["created_at"].isoformat(),
            }
        )
    return out


async def complete_remote_command(command_id: int) -> bool:
    if not using_postgres():
        for cmd in _memory.commands:
            if cmd["id"] == command_id:
                cmd["status"] = "done"
                return True
        return False
    pool = await _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE remote_commands SET status = 'done' WHERE id = $1 AND status = 'pending'",
            command_id,
        )
    return result.endswith("1")


async def latest_liveview_event(hostname: str, since_id: Optional[int] = None) -> dict | None:
    """Latest frame — in-memory cache first (30fps path), then DB."""
    cached = _liveview_cache.get(hostname)
    if cached:
        if since_id is not None and int(cached["id"]) <= since_id:
            return None
        return cached

    if not using_postgres():
        for event in reversed(_memory.events):
            if event.get("module") != "liveview":
                continue
            payload = event.get("payload") or {}
            if payload.get("hostname") == hostname and payload.get("image_base64"):
                if since_id is not None and int(event["id"]) <= since_id:
                    return None
                return event
        row = _memory.liveview_frames.get(hostname)
        if row:
            if since_id is not None and int(row["id"]) <= since_id:
                return None
            return row
        return None

    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS liveview_frames (
                hostname TEXT PRIMARY KEY,
                frame_id BIGINT NOT NULL DEFAULT 0,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        row = await conn.fetchrow(
            """
            SELECT frame_id, payload, updated_at
            FROM liveview_frames
            WHERE hostname = $1
            """,
            hostname,
        )
    if not row:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    fid = int(row["frame_id"])
    if since_id is not None and fid <= since_id:
        return None
    event = {
        "id": fid,
        "module": "liveview",
        "action": "screen_frame",
        "status": "success",
        "payload": payload,
        "session_id": None,
        "created_at": row["updated_at"].isoformat(),
    }
    _liveview_cache[hostname] = event
    return event


async def _persist_liveview_frame_pg(host: str, merged: dict) -> None:
    """Background Postgres write — live reads use in-memory cache."""
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS liveview_frames (
                    hostname TEXT PRIMARY KEY,
                    frame_id BIGINT NOT NULL DEFAULT 0,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                """
                INSERT INTO liveview_frames (hostname, frame_id, payload, updated_at)
                VALUES ($1, 1, $2::jsonb, NOW())
                ON CONFLICT (hostname) DO UPDATE SET
                    frame_id = liveview_frames.frame_id + 1,
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                """,
                host,
                json.dumps(merged),
            )
    except Exception:
        pass


async def upsert_liveview_frame(hostname: str, payload: dict) -> dict:
    """Fast single-row frame store for high-FPS streaming (not the events table)."""
    host = hostname.strip()
    merged = {**payload, "hostname": host}
    now = datetime.now(timezone.utc).isoformat()

    if not using_postgres():
        prev = _memory.liveview_frames.get(host, {})
        fid = int(prev.get("id") or 0) + 1
        event = {
            "id": fid,
            "module": "liveview",
            "action": "screen_frame",
            "status": "success",
            "payload": merged,
            "session_id": None,
            "created_at": now,
        }
        _memory.liveview_frames[host] = event
        _liveview_cache[host] = event
        return event

    fid = _memory_frame_seq.get(host, 0) + 1
    _memory_frame_seq[host] = fid
    event = {
        "id": fid,
        "module": "liveview",
        "action": "screen_frame",
        "status": "success",
        "payload": merged,
        "session_id": None,
        "created_at": now,
    }
    _liveview_cache[host] = event
    asyncio.create_task(_persist_liveview_frame_pg(host, merged))
    return event


async def set_liveview(
    hostname: str,
    enabled: bool,
    interval: float = 3.0,
    guild_id: str | None = None,
    quality: str = "balanced",
) -> dict:
    q = (quality or "balanced").strip().lower()
    if q not in ("ultra", "speed", "balanced", "hd", "full"):
        q = "balanced"
    state = {
        "hostname": hostname,
        "enabled": enabled,
        "interval": max(0.033, float(interval)),
        "quality": q,
        "guild_id": guild_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if not using_postgres():
        if enabled:
            _memory.liveview[hostname] = state
        else:
            _memory.liveview.pop(hostname, None)
            _memory.liveview_frames.pop(hostname, None)
            _liveview_cache.pop(hostname, None)
        return state
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS liveview_sessions (
                hostname TEXT PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT FALSE,
                interval_seconds REAL NOT NULL DEFAULT 3,
                quality_preset TEXT NOT NULL DEFAULT 'balanced',
                guild_id TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "ALTER TABLE liveview_sessions ADD COLUMN IF NOT EXISTS quality_preset TEXT NOT NULL DEFAULT 'balanced'"
        )
        if enabled:
            await conn.execute(
                """
                INSERT INTO liveview_sessions (hostname, enabled, interval_seconds, quality_preset, guild_id, updated_at)
                VALUES ($1, TRUE, $2, $3, $4, NOW())
                ON CONFLICT (hostname) DO UPDATE SET
                    enabled = TRUE,
                    interval_seconds = EXCLUDED.interval_seconds,
                    quality_preset = EXCLUDED.quality_preset,
                    guild_id = EXCLUDED.guild_id,
                    updated_at = NOW()
                """,
                hostname,
                state["interval"],
                q,
                guild_id,
            )
        else:
            await conn.execute(
                "UPDATE liveview_sessions SET enabled = FALSE, updated_at = NOW() WHERE hostname = $1",
                hostname,
            )
            await conn.execute("DELETE FROM liveview_frames WHERE hostname = $1", hostname)
            _liveview_cache.pop(hostname, None)
    return state


async def get_liveview(hostname: str) -> dict:
    if not using_postgres():
        row = _memory.liveview.get(hostname) or {}
        return {
            "hostname": hostname,
            "enabled": bool(row.get("enabled")),
            "interval": float(row.get("interval") or 3),
            "quality": str(row.get("quality") or "balanced"),
        }
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS liveview_sessions (
                hostname TEXT PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT FALSE,
                interval_seconds REAL NOT NULL DEFAULT 3,
                quality_preset TEXT NOT NULL DEFAULT 'balanced',
                guild_id TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "ALTER TABLE liveview_sessions ADD COLUMN IF NOT EXISTS quality_preset TEXT NOT NULL DEFAULT 'balanced'"
        )
        row = await conn.fetchrow(
            "SELECT enabled, interval_seconds, quality_preset FROM liveview_sessions WHERE hostname = $1",
            hostname,
        )
    if not row:
        return {"hostname": hostname, "enabled": False, "interval": 3, "quality": "balanced"}
    return {
        "hostname": hostname,
        "enabled": bool(row["enabled"]),
        "interval": float(row["interval_seconds"] or 3),
        "quality": str(row["quality_preset"] or "balanced"),
    }
