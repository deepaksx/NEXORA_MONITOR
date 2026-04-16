"""
Idempotent schema migrations for Nexora Monitor.

Run at FastAPI startup. Uses CREATE TABLE IF NOT EXISTS so it is safe to
re-run any number of times.

Tables added in npm_projects DB:
  - project_plans        (living project plan versions, with cached_render)
  - project_plan_tasks   (Gantt tasks per plan version)
"""

from __future__ import annotations

import logging

import queries

log = logging.getLogger("nexora.migrations")

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS project_plans (
        id              SERIAL PRIMARY KEY,
        project_id      INT  NOT NULL,
        version         INT  NOT NULL,
        status          TEXT NOT NULL CHECK (status IN ('draft','final','superseded')),
        generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        generated_by    TEXT NOT NULL,
        source_summary  TEXT,
        notes           TEXT,
        cached_render   JSONB,
        UNIQUE (project_id, version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS project_plan_tasks (
        id                SERIAL PRIMARY KEY,
        plan_id           INT  NOT NULL REFERENCES project_plans(id) ON DELETE CASCADE,
        task_key          TEXT NOT NULL,
        parent_key        TEXT,
        name              TEXT NOT NULL,
        sap_phase         TEXT,
        workstream        TEXT,
        owner             TEXT,
        start_date        DATE NOT NULL,
        end_date          DATE NOT NULL,
        percent_complete  NUMERIC(5,2) DEFAULT 0,
        is_milestone      BOOLEAN NOT NULL DEFAULT FALSE,
        depends_on        TEXT[],
        source_refs       JSONB,
        UNIQUE (plan_id, task_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_plans_project_status ON project_plans(project_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_plan_tasks_plan ON project_plan_tasks(plan_id)",
]


_DDL_BRAIN = [
    """
    CREATE TABLE IF NOT EXISTS monitor_users (
        id            SERIAL PRIMARY KEY,
        username      TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role          TEXT NOT NULL CHECK (role IN ('admin','user')),
        permissions   JSONB NOT NULL DEFAULT '[]',
        is_active     BOOLEAN NOT NULL DEFAULT TRUE,
        must_reset    BOOLEAN NOT NULL DEFAULT FALSE,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_login_at TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_monitor_users_active ON monitor_users(is_active)",
]


def _seed_admin(cur):
    """Seed a single admin if the table is empty. Idempotent."""
    import os, bcrypt
    cur.execute("SELECT COUNT(*) FROM monitor_users")
    row = cur.fetchone()
    count = row[0] if isinstance(row, (tuple, list)) else row.get("count")
    if count and int(count) > 0:
        return
    username = os.environ.get("MONITOR_ADMIN_USERNAME", "deepak")
    password = os.environ.get("MONITOR_ADMIN_PASSWORD", "ChangeMe!2026")
    h = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode()
    cur.execute(
        "INSERT INTO monitor_users (username, password_hash, role, permissions, must_reset) "
        "VALUES (%s, %s, 'admin', '[]'::jsonb, TRUE)",
        [username, h],
    )
    log.info("migrations: seeded initial admin user '%s' (must reset on first login)", username)


def run():
    # Plans schema in npm_projects
    conn = queries._conn("npm_projects")
    try:
        with conn.cursor() as cur:
            for stmt in _DDL:
                cur.execute(stmt)
        conn.commit()
        log.info("migrations: project_plans schema ensured")
    finally:
        conn.close()

    # Users schema in agent_brain
    conn = queries._conn("agent_brain")
    try:
        with conn.cursor() as cur:
            for stmt in _DDL_BRAIN:
                cur.execute(stmt)
            _seed_admin(cur)
        conn.commit()
        log.info("migrations: monitor_users schema ensured")
    finally:
        conn.close()
