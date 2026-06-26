# AntiVirusAPI

FastAPI backend for the AV research simulator. Deploy on **Railway** with PostgreSQL.

## Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/api/health` | — | Health check |
| POST | `/api/events` | Simulator key | Log simulation event |
| GET | `/api/events` | — | List events (dashboard) |
| GET | `/api/events?since_id=N` | Bot key | Poll new events (Discord bot) |
| GET | `/api/stats` | — | Aggregate stats |
| PATCH | `/api/events/{id}` | — | Update detected/blocked flags |
| GET | `/api/bot/watches` | Bot key | Discord linked channels |
| PUT | `/api/bot/watches` | Bot key | Save Discord linked channels |

## Railway deploy

1. Push this repo to GitHub
2. [Railway](https://railway.app) → **New Project** → **Deploy from GitHub** → select `AntiVirusAPI`
3. Add **PostgreSQL** plugin (Railway sets `DATABASE_URL`)
4. Set variables:
   - `SIMULATOR_API_KEY` — random secret for the simulator
   - `BOT_API_KEY` — random secret for the Discord bot
   - `CORS_ORIGINS` — your Vercel website URL
5. Copy the public Railway URL (e.g. `https://antivirusapi-production.up.railway.app`)

## Local dev

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Set DATABASE_URL to a local Postgres or Railway connection string
uvicorn main:app --reload --port 8000
```

## Connected services

- **AntiVirusWebsite** (Vercel) — reads from this API
- **AntiVirusBot** (Railway) — polls `since_id` for new events
- **Simulator** (local VM) — POSTs events with `x-api-key: SIMULATOR_API_KEY`
