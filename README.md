# Nexora Monitor

A single-window, read-only live status dashboard for the entire Nexora pipeline:

```
Humans → M365/Fireflies → Graph Sync → nxsys DB → Embeddings → Projects DB → Father Agent → Reports
```

One web page. Auto-refreshes every 15 seconds. Red / yellow / green at a glance, with drill-downs for every stage.

## What it shows

| Panel | What it reads |
|---|---|
| **Top status bar** | Overall health pill (worst of all pipeline stages) |
| **Pipeline flow** | 8 stages as colored dots in a left-to-right strip |
| **Graph Sync stages** | `nxsys.graph_sync_state` — 5 rows (emails, calendar, teams_chats, sharepoint, documents) with status + last-success age |
| **Data freshness** | Row counts and latest-sync age for 8 tables in `nxsys` |
| **Active projects** | `npm_projects.projects WHERE is_active = TRUE` with per-project SAP entity counts (GL accounts, business processes, materials, customers, vendors, risks, ...) |
| **Father agent** | Task counts by status, 24h conversation activity, 24h failures, latest row per component from `agent_brain.health_checks` |
| **Issues** | Aggregated red flags sorted by severity — sync errors, stale data, recent failures |

Three databases are queried (all on one PostgreSQL host): `nxsys`, `npm_projects`, `agent_brain`. The dashboard **never writes** to any of them.

## Architecture

- **Backend**: FastAPI, one file (`app.py`). One JSON endpoint `/api/status` returns the entire dashboard state.
- **Frontend**: One HTML page (`templates/dashboard.html`), Tailwind + Alpine.js via CDN. No build step, no `npm install`.
- **DB driver**: `psycopg2-binary`, one connection per query, no pool (low-traffic dashboard).
- **Queries**: `queries.py` — one function per panel, plus a `dashboard_state()` aggregator. Easy to extend.

## Local development

Requires Python 3.10+ and network access to a PostgreSQL that has the three Nexora databases.

```bash
cd nexora-monitor
python -m venv .venv
source .venv/bin/activate         # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# edit .env — set DB_HOST, DB_USER, DB_PASSWORD

uvicorn app:app --reload --port 8080
# → http://localhost:8080
```

### Running it on the EC2 host directly (recommended path)

The simplest secure deployment: run it on the same box that hosts the databases, behind Nginx + basic auth. No DB exposure to the public internet.

```bash
# copy to the server
scp -i ~/keys/npm-key.pem -r nexora-monitor/ ubuntu@3.6.105.91:/home/ubuntu/

# on the server
ssh ubuntu@3.6.105.91
cd /home/ubuntu/nexora-monitor
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# reuse the nexora-father .env for DB creds, or write your own
cp /home/ubuntu/nexora-father/.env .env
echo "BASIC_AUTH_USER=admin" >> .env
echo "BASIC_AUTH_PASSWORD=$(openssl rand -hex 16)" >> .env

# run under PM2 alongside the rest of Nexora
pm2 start "uvicorn app:app --host 127.0.0.1 --port 8765" --name nexora-monitor
pm2 save
```

Then point Nginx at `127.0.0.1:8765` on a hostname of your choice.

## Deploying to Render

The repo ships with a `render.yaml` blueprint — in the Render dashboard, choose **New → Blueprint**, point it at the GitHub repo, and fill in the required env vars (`DB_HOST`, `DB_USER`, `DB_PASSWORD`, optionally `BASIC_AUTH_USER` / `BASIC_AUTH_PASSWORD`).

**Important caveat:** Render runs in the public internet, so it needs a route to your PostgreSQL. You have three options:

1. **Expose PostgreSQL with TLS + IP whitelist** (simplest, but increases attack surface). Edit `pg_hba.conf` to accept the Render egress IPs, require `hostssl`, and use a dedicated read-only DB user for the monitor.
2. **SSH tunnel from Render** (more moving parts) — deploy a small sidecar that opens an SSH tunnel to EC2 and forwards 5432.
3. **Don't use Render at all** — run the monitor on the EC2 box (see above), it's one less thing to secure.

For option 1, create a restricted role:
```sql
CREATE USER monitor_ro WITH PASSWORD 'strong-random-here';
GRANT CONNECT ON DATABASE nxsys, npm_projects, agent_brain TO monitor_ro;
GRANT USAGE ON SCHEMA public TO monitor_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO monitor_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO monitor_ro;
```

Then set `DB_USER=monitor_ro` in Render. The monitor never issues anything except `SELECT`, so this is safe.

## Configuration

All via environment variables. See `.env.example`.

| Variable | Default | Purpose |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `npm` | Database user (read-only recommended) |
| `DB_PASSWORD` | — | Database password |
| `REFRESH_SECONDS` | `15` | Browser poll interval |
| `FRESHNESS_WARN_MIN` | `120` | Stage older than this → yellow |
| `FRESHNESS_CRIT_MIN` | `360` | Stage older than this → red |
| `BASIC_AUTH_USER` | _(empty)_ | If set with password, enables HTTP Basic auth |
| `BASIC_AUTH_PASSWORD` | _(empty)_ | |

## Endpoints

| Path | Purpose |
|---|---|
| `GET /` | Dashboard HTML (auth-gated if configured) |
| `GET /api/status` | Full dashboard state as JSON — what the frontend polls |
| `GET /healthz` | Liveness — always 200 if the app process is up |
| `GET /readyz` | Readiness — 200 only if all three databases respond to `SELECT 1`, else 503 with failure detail |

Use `/readyz` for Render's health check URL.

## Extending

The dashboard state is assembled in `queries.dashboard_state()`. To add a new panel:

1. Add a query function in `queries.py` that returns a JSON-serialisable dict/list.
2. Call it from `dashboard_state()` and wrap in the `safe(...)` helper so a failure in one panel never breaks the rest.
3. Add a `<div class="card">` in `templates/dashboard.html` bound via Alpine `x-data` / `x-text`.

The severity model is three-tier: `green | yellow | red | unknown`. Use `_worst(*severities)` to roll up. Thresholds for freshness-based severity come from `FRESHNESS_WARN_MIN` and `FRESHNESS_CRIT_MIN`.

## Security notes

- **Read-only by design.** There are no `INSERT`/`UPDATE`/`DELETE` statements anywhere in `queries.py`. Use a read-only DB user in production as defence in depth.
- **Basic auth is off by default.** Set `BASIC_AUTH_USER` + `BASIC_AUTH_PASSWORD` before exposing this to the internet.
- **Data is sensitive.** This dashboard surfaces mailbox names, chat counts, project health, error messages with PII, etc. Treat the deployed URL as confidential.
- **No CORS setup.** The `/api/status` endpoint is same-origin only; if you want to embed it elsewhere, add CORS middleware and think carefully about auth.

## Known limitations (v1)

- No PM2 process state polling — the watchdog component health from `agent_brain.health_checks` is the proxy. Shell access from a hosted dashboard is out of scope.
- No metrics history — every render is "right now". If you want 24-hour graphs, plug the JSON into Grafana or persist snapshots.
- No WebSocket push — polling only. 15s is fine for a pipeline whose slowest stage is 54 minutes.
- Project entity counts re-query on every refresh. At one active project this is fine; if you activate dozens, add a cache.
