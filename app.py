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
import subprocess
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()  # load .env before importing queries so DB_* are available

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

import asyncio
import uuid

import queries
import generate_kickoff
import generate_document
import migrations

try:
    migrations.run()
except Exception as _e:  # noqa: BLE001
    # Don't crash the monitor if migrations fail (e.g. DB briefly down); log and move on.
    import logging as _logging
    _logging.getLogger("nexora.migrations").warning("migrations.run() failed: %s", _e)

FATHER_DIR = os.environ.get("FATHER_DIR", "/home/ubuntu/nexora-father")

# In-memory job tracker for background document generation
_jobs: dict[str, dict] = {}  # job_id -> {status, doc_id, error, ...}


async def _run_generate_job(job_id: str, doc_id: int, doc_type: str, project_id: int):
    """Background task: generate doc, save to DB+S3, update job status."""
    try:
        data = queries.kickoff_project_data(project_id)
        html = await generate_document.generate_doc(doc_type, data)
        try:
            queries.document_save_draft(doc_id, html)
        except Exception:
            pass  # S3 fail is non-fatal, HTML is in DB
        _jobs[job_id]["status"] = "done"
    except Exception as e:
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = f"{type(e).__name__}: {e}"


async def _run_refine_job(job_id: str, doc_id: int, doc_type: str,
                          current_html: str, prompt: str, project_id: int):
    """Background task: refine doc, save to DB+S3, update job status."""
    try:
        data = queries.kickoff_project_data(project_id)
        html = await generate_document.refine_doc(doc_type, current_html, prompt, data)
        try:
            queries.document_save_draft(doc_id, html)
        except Exception:
            pass
        _jobs[job_id]["status"] = "done"
    except Exception as e:
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = f"{type(e).__name__}: {e}"

REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "15"))

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Nexora Monitor", version="0.2.0")


_BUILD_ID = os.environ.get("RENDER_GIT_COMMIT", "")[:7] or "local"


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    try:
        resp = templates.TemplateResponse(
            request,
            "dashboard.html",
            {"refresh_seconds": REFRESH_SECONDS, "build_id": _BUILD_ID},
        )
        # Defeat proxy/browser caching so new builds always land immediately.
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp
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


@app.post("/api/promote_insight")
def promote_insight(payload: dict):
    insight_id = payload.get("insight_id")
    project_id = payload.get("project_id", 9)
    if not insight_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "insight_id required"})
    try:
        result = queries.promote_insight(int(insight_id), int(project_id))
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    if not result.get("ok"):
        return JSONResponse(status_code=409, content=result)
    return result


@app.post("/api/dismiss_insight")
def dismiss_insight(payload: dict):
    insight_id = payload.get("insight_id")
    project_id = payload.get("project_id", 9)
    if not insight_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "insight_id required"})
    try:
        result = queries.dismiss_insight(int(insight_id), int(project_id))
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    return result


@app.post("/api/regenerate_insights")
def regenerate_insights(payload: dict | None = None):
    """Run refresh_insights.py on the server and return the result."""
    project_id = (payload or {}).get("project_id", 9)
    script = os.path.join(FATHER_DIR, "refresh_insights.py")
    cmd = (
        f"cd {FATHER_DIR} && source venv/bin/activate && "
        f"export $(grep -v '^#' .env | xargs) && "
        f"python3 {script}"
    )
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            return JSONResponse(status_code=500, content={
                "ok": False,
                "error": result.stderr[-2000:] if result.stderr else "non-zero exit",
                "stdout": result.stdout[-2000:] if result.stdout else "",
            })
        return {"ok": True, "output": result.stdout[-3000:] if result.stdout else ""}
    except subprocess.TimeoutExpired:
        return JSONResponse(status_code=504, content={"ok": False, "error": "Script timed out after 300s"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


# ── Project Documents ──

@app.get("/api/documents")
def list_documents(project_id: int = 9):
    try:
        return {"ok": True, "documents": queries.documents_list(project_id)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/api/documents/{doc_id}")
def get_document(doc_id: int):
    try:
        doc = queries.document_get(doc_id)
        if not doc:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Not found"})
        return {"ok": True, "document": doc}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/documents/generate")
async def generate_doc_endpoint(payload: dict):
    """Fire-and-forget: kicks off generation in background, returns job_id immediately."""
    doc_id = payload.get("doc_id")
    project_id = payload.get("project_id", 9)
    if not doc_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "doc_id required"})

    doc = queries.document_get(int(doc_id))
    if not doc:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})

    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status": "running", "doc_id": int(doc_id), "error": None}
    asyncio.create_task(_run_generate_job(job_id, int(doc_id), doc["doc_type"], int(project_id)))
    return {"ok": True, "job_id": job_id}


@app.post("/api/documents/refine")
async def refine_doc_endpoint(payload: dict):
    """Fire-and-forget: kicks off refinement in background, returns job_id immediately."""
    doc_id = payload.get("doc_id")
    prompt = payload.get("prompt", "").strip()
    project_id = payload.get("project_id", 9)
    html = payload.get("html", "")

    if not doc_id or not prompt:
        return JSONResponse(status_code=400, content={"ok": False, "error": "doc_id and prompt required"})

    doc = queries.document_get(int(doc_id))
    if not doc:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})

    current_html = html or doc.get("draft_html", "")
    if not current_html:
        return JSONResponse(status_code=400, content={"ok": False, "error": "No draft to refine. Generate first."})

    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status": "running", "doc_id": int(doc_id), "error": None}
    asyncio.create_task(_run_refine_job(job_id, int(doc_id), doc["doc_type"], current_html, prompt, int(project_id)))
    return {"ok": True, "job_id": job_id}


@app.get("/api/jobs/{job_id}")
def get_job_status(job_id: str):
    """Poll this to check if a generate/refine job is done."""
    job = _jobs.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Job not found"})
    return {"ok": True, "status": job["status"], "doc_id": job["doc_id"], "error": job.get("error")}


@app.post("/api/documents/finalize")
def finalize_doc_endpoint(payload: dict):
    doc_id = payload.get("doc_id")
    if not doc_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "doc_id required"})
    try:
        result = queries.document_finalize(int(doc_id))
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/api/documents/{doc_id}/versions")
def list_doc_versions(doc_id: int):
    try:
        return {"ok": True, "versions": queries.document_versions(doc_id)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/api/documents/versions/{version_id}/html")
def get_version_html(version_id: int):
    try:
        html = queries.document_version_html(version_id)
        return {"ok": True, "html": html}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


# ── Kick-off Presentation ──

@app.post("/api/kickoff/generate")
async def kickoff_generate(payload: dict):
    project_id = payload.get("project_id", 9)
    try:
        data = queries.kickoff_project_data(int(project_id))
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"DB error: {e}"})

    if not data.get("project"):
        return JSONResponse(status_code=404, content={"ok": False, "error": f"Project {project_id} not found"})

    try:
        html = await generate_kickoff.generate_html(data)
    except Exception as e:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"Generation failed: {e}"})

    return {
        "ok": True,
        "html": html,
        "project_name": data["project"].get("name", "Unknown"),
    }


@app.post("/api/kickoff/refine")
async def kickoff_refine(payload: dict):
    current_html = payload.get("html", "")
    prompt = payload.get("prompt", "").strip()
    project_id = payload.get("project_id", 9)

    if not current_html:
        return JSONResponse(status_code=400, content={"ok": False, "error": "html is required"})
    if not prompt:
        return JSONResponse(status_code=400, content={"ok": False, "error": "prompt is required"})

    try:
        data = queries.kickoff_project_data(int(project_id))
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"DB error: {e}"})

    try:
        html = await generate_kickoff.refine_html(current_html, prompt, data)
    except Exception as e:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"Refinement failed: {e}"})

    return {"ok": True, "html": html}


# ── Living Project Plan (Gantt) ──

@app.get("/api/projects/{project_id}/plan")
def get_project_plan(project_id: int, version: str = "latest"):
    try:
        if version == "latest":
            payload = queries.plan_get_latest(project_id)
        else:
            payload = queries.plan_get_version(int(version))
        if not payload:
            return {"ok": True, "plan": None}
        return {"ok": True, "plan": payload}
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/api/projects/{project_id}/plan/versions")
def list_project_plan_versions(project_id: int):
    try:
        return {"ok": True, "versions": queries.plan_list_versions(project_id)}
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/projects/{project_id}/plan/regenerate")
def regenerate_project_plan(project_id: int):
    try:
        result = queries.plan_create_placeholder(project_id, generated_by="placeholder")
        if not result.get("ok"):
            return JSONResponse(status_code=400, content=result)
        return result
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/projects/{project_id}/plan/{plan_id}/approve")
def approve_project_plan(project_id: int, plan_id: int):
    try:
        return queries.plan_approve_final(plan_id)
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


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
