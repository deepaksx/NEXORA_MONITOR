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

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import asyncio
import uuid

import queries
import generate_kickoff
import generate_document
import migrations
import auth

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


# ── Auth middleware ──

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    # Strip any proxy root path (e.g. /monitor)
    rp = request.scope.get("root_path") or ""
    if rp and path.startswith(rp):
        path_nx = path[len(rp):] or "/"
    else:
        path_nx = path
    if auth.is_public(path_nx):
        return await call_next(request)
    user = auth.current_user(request)
    if not user:
        if path_nx.startswith("/api/"):
            return JSONResponse(status_code=401, content={"ok": False, "error": "auth required"})
        # HTML: redirect to /login with the ?next= target.
        nxt = path_nx
        if request.url.query:
            nxt = f"{nxt}?{request.url.query}"
        return RedirectResponse(url=f"{rp}/login?next={nxt}", status_code=302)
    # Attach user to request.state for handlers.
    request.state.user = user
    return await call_next(request)


def _require_perm(request: Request, key: str):
    user = getattr(request.state, "user", None) or auth.current_user(request)
    if not user or not auth.has_permission(user, key):
        return JSONResponse(status_code=403, content={"ok": False, "error": f"permission '{key}' required"})
    return None


# ── Login / logout ──

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/", error: str = None, notice: str = None):
    if auth.current_user(request):
        return RedirectResponse(url=(request.scope.get("root_path") or "") + (next or "/"), status_code=302)
    return templates.TemplateResponse(
        request, "login.html",
        {"error": error, "notice": notice, "next_url": next,
         "base": request.scope.get("root_path") or "",
         "build_id": _BUILD_ID},
    )


@app.post("/login")
def login_submit(request: Request,
                 username: str = Form(...), password: str = Form(...),
                 next: str = Form("/")):
    u = auth.verify_password(username, password)
    if not u:
        return templates.TemplateResponse(
            request, "login.html",
            {"error": "Invalid username or password", "next_url": next,
             "base": request.scope.get("root_path") or "", "build_id": _BUILD_ID},
            status_code=401,
        )
    auth.touch_last_login(u["id"])
    token = auth.issue_session(u["id"])
    rp = request.scope.get("root_path") or ""
    resp = RedirectResponse(url=rp + (next or "/"), status_code=302)
    resp.set_cookie(
        key=auth.COOKIE_NAME, value=token,
        max_age=auth.COOKIE_MAX_AGE, httponly=True, samesite="lax",
        secure=(request.url.scheme == "https"),
        path=(rp or "/"),
    )
    return resp


@app.post("/logout")
def logout(request: Request):
    rp = request.scope.get("root_path") or ""
    resp = RedirectResponse(url=rp + "/login", status_code=302)
    resp.delete_cookie(auth.COOKIE_NAME, path=(rp or "/"))
    return resp


# ── Current-user / password self-change ──

@app.get("/api/me")
def api_me(request: Request):
    u = request.state.user
    try:
        doc_catalog = queries.doc_types_catalog()
    except Exception:
        doc_catalog = []
    return {
        "ok": True,
        "user": {
            "id": u["id"], "username": u["username"], "role": u["role"],
            "permissions": u.get("permissions") or [],
            "must_reset": bool(u.get("must_reset")),
            "all_permissions": auth.ALL_PERMISSIONS,
            "doc_types": doc_catalog,
        },
    }


@app.post("/api/me/password")
def api_me_password(request: Request, payload: dict):
    new_pw = (payload or {}).get("new_password", "")
    r = auth.self_change_password(request.state.user["id"], new_pw)
    code = 200 if r.get("ok") else 400
    return JSONResponse(status_code=code, content=r)


# ── Users admin CRUD (admin only) ──

@app.get("/api/users")
def api_list_users(request: Request):
    err = _require_perm(request, "users_admin")
    if err: return err
    return {"ok": True, "users": auth.list_users(), "all_permissions": auth.ALL_PERMISSIONS}


@app.post("/api/users")
def api_create_user(request: Request, payload: dict):
    err = _require_perm(request, "users_admin")
    if err: return err
    r = auth.create_user(
        username=payload.get("username", "").strip(),
        password=payload.get("password", ""),
        role=payload.get("role", "user"),
        permissions=payload.get("permissions") or [],
        must_reset=True,
    )
    return JSONResponse(status_code=200 if r.get("ok") else 400, content=r)


@app.patch("/api/users/{user_id}")
def api_update_user(request: Request, user_id: int, payload: dict):
    err = _require_perm(request, "users_admin")
    if err: return err
    r = auth.update_user(
        user_id,
        role=payload.get("role"),
        permissions=payload.get("permissions"),
        is_active=payload.get("is_active"),
        password=payload.get("password"),
    )
    return JSONResponse(status_code=200 if r.get("ok") else 400, content=r)


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
def api_status(request: Request, project_id: int | None = None):
    err = _require_perm(request, "pipeline")
    if err: return err
    try:
        state = queries.dashboard_state(project_id=project_id)
    except Exception as e:  # noqa: BLE001
        return JSONResponse(
            status_code=500,
            content={"error": f"{type(e).__name__}: {e}", "overall": "red"},
        )
    return state


@app.post("/api/promote_insight")
def promote_insight(request: Request, payload: dict):
    err = _require_perm(request, "risk_register_write")
    if err: return err
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
def dismiss_insight(request: Request, payload: dict):
    err = _require_perm(request, "risk_register_write")
    if err: return err
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
def regenerate_insights(request: Request, payload: dict | None = None):
    err = _require_perm(request, "risk_register_write")
    if err: return err
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
def list_documents(request: Request, project_id: int = 9):
    user = request.state.user
    try:
        docs = queries.documents_list(project_id)
        # Filter to docs this user is allowed to view.
        docs = [d for d in docs if auth.has_doc_permission(user, "view", d.get("doc_type", ""))]
        return {"ok": True, "documents": docs}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/api/documents/{doc_id}")
def get_document(request: Request, doc_id: int):
    try:
        doc = queries.document_get(doc_id)
        if not doc:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Not found"})
        if not auth.has_doc_permission(request.state.user, "view", doc.get("doc_type", "")):
            return JSONResponse(status_code=403, content={"ok": False, "error": "no view access for this document"})
        return {"ok": True, "document": doc}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/documents/generate")
async def generate_doc_endpoint(request: Request, payload: dict):
    """Fire-and-forget: kicks off generation in background, returns job_id immediately."""
    doc_id = payload.get("doc_id")
    project_id = payload.get("project_id") or 9
    if not doc_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "doc_id required"})

    doc = queries.document_get(int(doc_id))
    if not doc:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})
    if not auth.has_doc_permission(request.state.user, "write", doc.get("doc_type", "")):
        return JSONResponse(status_code=403, content={"ok": False, "error": "no edit access for this document"})
    if not project_id:
        project_id = doc.get("project_id") or 9

    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status": "running", "doc_id": int(doc_id), "error": None}
    asyncio.create_task(_run_generate_job(job_id, int(doc_id), doc["doc_type"], int(project_id)))
    return {"ok": True, "job_id": job_id}


@app.post("/api/documents/refine")
async def refine_doc_endpoint(request: Request, payload: dict):
    """Fire-and-forget: kicks off refinement in background, returns job_id immediately."""
    doc_id = payload.get("doc_id")
    prompt = payload.get("prompt", "").strip()
    project_id = payload.get("project_id") or 9
    html = payload.get("html", "")

    if not doc_id or not prompt:
        return JSONResponse(status_code=400, content={"ok": False, "error": "doc_id and prompt required"})

    doc = queries.document_get(int(doc_id))
    if not doc:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})
    if not auth.has_doc_permission(request.state.user, "write", doc.get("doc_type", "")):
        return JSONResponse(status_code=403, content={"ok": False, "error": "no edit access for this document"})
    # Fallback to the document's own project_id if client didn't send one.
    if not project_id:
        project_id = doc.get("project_id") or 9

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
def finalize_doc_endpoint(request: Request, payload: dict):
    doc_id = payload.get("doc_id")
    if not doc_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "doc_id required"})
    doc = queries.document_get(int(doc_id))
    if not doc:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})
    if not auth.has_doc_permission(request.state.user, "approve", doc.get("doc_type", "")):
        return JSONResponse(status_code=403, content={"ok": False, "error": "no approval rights for this document"})
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
async def kickoff_generate(request: Request, payload: dict):
    err = _require_perm(request, "documents_write")
    if err: return err
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
async def kickoff_refine(request: Request, payload: dict):
    err = _require_perm(request, "documents_write")
    if err: return err
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
def get_project_plan(request: Request, project_id: int, version: str = "latest"):
    err = _require_perm(request, "project_plan")
    if err: return err
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
def list_project_plan_versions(request: Request, project_id: int):
    err = _require_perm(request, "project_plan")
    if err: return err
    try:
        return {"ok": True, "versions": queries.plan_list_versions(project_id)}
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/projects/{project_id}/plan/regenerate")
def regenerate_project_plan(request: Request, project_id: int):
    err = _require_perm(request, "project_plan_write")
    if err: return err
    try:
        result = queries.plan_create_placeholder(project_id, generated_by="placeholder")
        if not result.get("ok"):
            return JSONResponse(status_code=400, content=result)
        return result
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/projects/{project_id}/plan/{plan_id}/approve")
def approve_project_plan(request: Request, project_id: int, plan_id: int):
    err = _require_perm(request, "project_plan_approve")
    if err: return err
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
