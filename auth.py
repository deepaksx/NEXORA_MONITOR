"""
Nexora Monitor — session-based authentication and per-area access control.

Design:
  - Login form at /login posts username+password -> bcrypt verify.
  - Signed cookie (itsdangerous) carries user_id + issued_at; 7-day TTL.
  - FastAPI middleware redirects unauthenticated to /login (for HTML) or
    returns 401 JSON (for /api/*).
  - `require_permission(key)` dependency enforces per-area access on POST
    endpoints. Admin role bypasses all checks.

Permission keys:
  pipeline, documents, documents_write, risk_register, risk_register_write,
  project_plan, project_plan_write, users_admin
"""

from __future__ import annotations

import os
import secrets
import time
from typing import Optional

import bcrypt
import psycopg2.extras
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

import queries

COOKIE_NAME = "nexora_session"
COOKIE_MAX_AGE = 7 * 24 * 3600  # 7 days

ALL_PERMISSIONS = [
    "pipeline",
    "documents", "documents_write", "documents_approve",   # broad — all documents
    "risk_register", "risk_register_write",
    "project_plan", "project_plan_write", "project_plan_approve",
    "users_admin",
]


def _valid_perm(p: str) -> bool:
    if p in ALL_PERMISSIONS:
        return True
    if not isinstance(p, str):
        return False
    parts = p.split(":")
    return (len(parts) == 3 and parts[0] == "doc"
            and parts[2] in ("view", "write", "approve")
            and all(c.isalnum() or c in "_-" for c in parts[1]))


def has_doc_permission(user: dict, action: str, doc_type: str) -> bool:
    """
    True if `user` is allowed to `action` (view|write|approve) document type
    `doc_type`. Admin always wins. Otherwise the broad `documents{,_write,_approve}`
    permission acts as a wildcard, OR a specific `doc:{doc_type}:{action}` key
    grants access to just that doc.
    """
    if not user:
        return False
    if user.get("role") == "admin":
        return True
    perms = user.get("permissions") or []
    if isinstance(perms, str):
        import json as _json
        try: perms = _json.loads(perms)
        except Exception: perms = []
    perms = set(perms)
    broad = {"view": "documents", "write": "documents_write", "approve": "documents_approve"}.get(action)
    if broad and broad in perms:
        return True
    return f"doc:{doc_type}:{action}" in perms

PUBLIC_PATHS = {"/login", "/logout", "/healthz", "/readyz"}
PUBLIC_PREFIXES = ("/static/",)


def _secret() -> str:
    s = os.environ.get("MONITOR_SESSION_SECRET")
    if not s:
        # Ephemeral fallback so the app doesn't refuse to boot; sessions don't
        # survive restarts in this mode. Production must set the env var.
        s = secrets.token_urlsafe(32)
        os.environ["MONITOR_SESSION_SECRET"] = s
    return s


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(_secret(), salt="nexora-monitor-session")


# ── DB access ──────────────────────────────────────────────────────────────

def _fetch_user(username: str) -> Optional[dict]:
    return queries._fetch_one("agent_brain", """
        SELECT id, username, password_hash, role, permissions,
               is_active, must_reset, last_login_at
        FROM monitor_users WHERE LOWER(username) = LOWER(%s)
    """, [username])


def get_user_by_id(user_id: int) -> Optional[dict]:
    u = queries._fetch_one("agent_brain", """
        SELECT id, username, role, permissions, is_active, must_reset, last_login_at
        FROM monitor_users WHERE id = %s
    """, [user_id])
    return u or None


def touch_last_login(user_id: int):
    conn = queries._conn("agent_brain")
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE monitor_users SET last_login_at = now() WHERE id = %s", [user_id])
        conn.commit()
    finally:
        conn.close()


def verify_password(username: str, password: str) -> Optional[dict]:
    u = _fetch_user(username)
    if not u or not u.get("is_active"):
        return None
    try:
        ok = bcrypt.checkpw(password.encode("utf-8"), u["password_hash"].encode("utf-8"))
    except Exception:
        return None
    return u if ok else None


# ── User admin CRUD ───────────────────────────────────────────────────────

def list_users() -> list[dict]:
    rows = queries._fetch("agent_brain", """
        SELECT id, username, role, permissions, is_active, must_reset,
               created_at, last_login_at
        FROM monitor_users ORDER BY is_active DESC, username
    """)
    for r in rows:
        r["created_at"]    = queries._iso(r.get("created_at"))
        r["last_login_at"] = queries._iso(r.get("last_login_at"))
    return rows


def create_user(username: str, password: str, role: str,
                permissions: list[str], must_reset: bool = True) -> dict:
    if role not in ("admin", "user"):
        return {"ok": False, "error": "role must be admin or user"}
    if not username or not password:
        return {"ok": False, "error": "username and password required"}
    perms = [p for p in (permissions or []) if _valid_perm(p)]
    h = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode()
    import json
    conn = queries._conn("agent_brain")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO monitor_users (username, password_hash, role, permissions, must_reset)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            RETURNING id
        """, [username.strip(), h, role, json.dumps(perms), must_reset])
        uid = cur.fetchone()["id"]
        conn.commit()
        return {"ok": True, "id": uid}
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return {"ok": False, "error": "username already exists"}
    finally:
        conn.close()


def update_user(user_id: int, *, role: str = None, permissions: list[str] = None,
                is_active: bool = None, password: str = None) -> dict:
    sets = []
    args: list = []
    if role is not None:
        if role not in ("admin", "user"):
            return {"ok": False, "error": "invalid role"}
        sets.append("role = %s"); args.append(role)
    if permissions is not None:
        import json
        perms = [p for p in permissions if _valid_perm(p)]
        sets.append("permissions = %s::jsonb"); args.append(json.dumps(perms))
    if is_active is not None:
        sets.append("is_active = %s"); args.append(bool(is_active))
    if password:
        h = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode()
        sets.append("password_hash = %s"); args.append(h)
        sets.append("must_reset = TRUE")
    if not sets:
        return {"ok": False, "error": "nothing to update"}
    args.append(user_id)
    conn = queries._conn("agent_brain")
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE monitor_users SET {', '.join(sets)} WHERE id = %s", args)
            if cur.rowcount == 0:
                return {"ok": False, "error": "user not found"}
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


def self_change_password(user_id: int, new_password: str) -> dict:
    if len(new_password) < 6:
        return {"ok": False, "error": "password must be at least 6 characters"}
    h = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt()).decode()
    conn = queries._conn("agent_brain")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE monitor_users SET password_hash = %s, must_reset = FALSE WHERE id = %s",
                [h, user_id],
            )
            if cur.rowcount == 0:
                return {"ok": False, "error": "user not found"}
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


# ── Session cookie helpers ────────────────────────────────────────────────

def issue_session(user_id: int) -> str:
    return _serializer().dumps({"uid": int(user_id), "iat": int(time.time())})


def read_session(token: str) -> Optional[dict]:
    if not token:
        return None
    try:
        payload = _serializer().loads(token, max_age=COOKIE_MAX_AGE)
        uid = int(payload.get("uid", 0))
        if not uid:
            return None
        u = get_user_by_id(uid)
        if not u or not u.get("is_active"):
            return None
        return u
    except (BadSignature, SignatureExpired, Exception):
        return None


def current_user(request) -> Optional[dict]:
    return read_session(request.cookies.get(COOKIE_NAME))


def has_permission(user: dict, key: str) -> bool:
    if not user:
        return False
    if user.get("role") == "admin":
        return True
    perms = user.get("permissions") or []
    if isinstance(perms, str):
        import json
        try: perms = json.loads(perms)
        except Exception: perms = []
    return key in perms


def is_public(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    return any(path.startswith(p) for p in PUBLIC_PREFIXES)
