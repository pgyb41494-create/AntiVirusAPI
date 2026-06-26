import os
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR / 'events.db'}")
SIMULATOR_API_KEY = os.getenv("SIMULATOR_API_KEY", "")
BOT_API_KEY = os.getenv("BOT_API_KEY", "")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

_pool: asyncpg.Pool | None = None
_use_postgres = DATABASE_URL.startswith("postgres")


def _pg_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if _use_postgres:
            _pool = await asyncpg.create_pool(_pg_url(DATABASE_URL), min_size=1, max_size=5)
        else:
            raise RuntimeError("SQLite fallback requires postgres DATABASE_URL on Railway")
    return _pool


async def init_db() -> None:
    if not _use_postgres:
        return
    pool = await get_pool()
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
            CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC)
        """)


def verify_simulator_key(provided: str | None) -> bool:
    if not SIMULATOR_API_KEY:
        return True
    return provided == SIMULATOR_API_KEY


def verify_bot_key(provided: str | None) -> bool:
    if not BOT_API_KEY:
        return True
    return provided == BOT_API_KEY
