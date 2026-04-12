"""
Nexora Monitor — FastAPI dashboard server.

Routes:
    GET /             → dashboard HTML shell (Tailwind + Alpine via CDN)
    GET /api/status   → full dashboard state as JSON (polled every REFRESH_SECONDS)
    GET /healthz      → liveness probe (200 OK if the server is up; DB not checked)
    GET /readyz       → readiness probe (200 OK only if all three DBs respond)

Run locally:
    cp .env.example .env   # edit DB_* values
    pip install -r requirements.txt
    uvicorn app:app --reload --port 8080
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()  # load .env before importing queries so DB_* are available

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

import queries

REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "15"))

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Nexora Monitor", version="0.2.0")


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    try:
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {"refresh_seconds": REFRESH_SECONDS},
        )
    except Exception as e:  # noqa: BLE001
        # Fall back to a plain-text error so we see the problem in the browser
        # instead of a silent 404/500 from an upstream proxy.
        return PlainTextResponse(
            f"Template render failed: {type(e).__name__}: {e}",
            status_code=500,
        )


@app.get("/api/status")
def api_status(project_id: int | None = None):
    try:
        state = queries.dashboard_state(project_id=project_id)
    except Exception as e:  # noqa: BLE001
        return JSONResponse(
            status_code=500,
            content={"error": f"{type(e).__name__}: {e}", "overall": "red"},
        )
    return state


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/readyz")
def readyz():
    # Try to open one connection per DB
    failures = []
    for db in ("nxsys", "npm_projects", "agent_brain"):
        try:
            with queries._conn(db) as c:
                with c.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        except Exception as e:  # noqa: BLE001
            failures.append(f"{db}: {e}")
    if failures:
        return JSONResponse(status_code=503, content={"ok": False, "failures": failures})
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
