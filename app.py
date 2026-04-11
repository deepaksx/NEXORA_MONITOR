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

from __future__ import annotations

import os
import secrets
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()  # load .env before importing queries so DB_* are available

from fastapi import FastAPI, HTTPException, Request, Depends, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

import queries

REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "15"))
BASIC_AUTH_USER = os.environ.get("BASIC_AUTH_USER", "").strip()
BASIC_AUTH_PASSWORD = os.environ.get("BASIC_AUTH_PASSWORD", "").strip()

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Nexora Monitor", version="0.1.0")
security = HTTPBasic(auto_error=False)


def _auth(creds: HTTPBasicCredentials | None = Depends(security)) -> None:
    if not BASIC_AUTH_USER and not BASIC_AUTH_PASSWORD:
        return  # auth disabled
    if creds is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )
    ok_user = secrets.compare_digest(creds.username, BASIC_AUTH_USER)
    ok_pass = secrets.compare_digest(creds.password, BASIC_AUTH_PASSWORD)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


@app.get("/", response_class=HTMLResponse)
def index(request: Request, _=Depends(_auth)):
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"refresh_seconds": REFRESH_SECONDS},
    )


@app.get("/api/status")
def api_status(_=Depends(_auth)):
    try:
        state = queries.dashboard_state()
    except Exception as e:  # noqa: BLE001
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "overall": "red"},
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
