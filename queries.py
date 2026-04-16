"""
Nexora Monitor — database queries.

Read-only. Never writes. Each function returns plain dicts suitable for JSON serialisation.

Three databases (all on the same PostgreSQL host):
  - nxsys        → raw M365 data + graph_sync_state
  - npm_projects → project intelligence layer (projects, ke_*, risks, etc.)
  - agent_brain  → Father's memory (tasks, failures, health_checks, conversations)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "npm")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

FRESHNESS_WARN_MIN = int(os.environ.get("FRESHNESS_WARN_MIN", "120"))
FRESHNESS_CRIT_MIN = int(os.environ.get("FRESHNESS_CRIT_MIN", "360"))


def _conn(dbname: str):
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=dbname,
        connect_timeout=5,
    )


def _fetch(dbname: str, sql: str, params: tuple | list | None = None) -> list[dict]:
    with _conn(dbname) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or [])
            if cur.description is None:
                return []
            return [dict(r) for r in cur.fetchall()]


def _fetch_one(dbname: str, sql: str, params: tuple | list | None = None) -> dict:
    rows = _fetch(dbname, sql, params)
    return rows[0] if rows else {}


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _age_minutes(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - value
        return round(delta.total_seconds() / 60, 1)
    return None


def _fmt_age(age_min: float | None) -> str:
    if age_min is None:
        return "—"
    if age_min < 1:
        return f"{round(age_min * 60)}s"
    if age_min < 60:
        return f"{round(age_min)}m"
    if age_min < 1440:
        return f"{age_min / 60:.1f}h"
    return f"{age_min / 1440:.1f}d"


def _freshness_severity(age_min: float | None) -> str:
    if age_min is None:
        return "unknown"
    if age_min >= FRESHNESS_CRIT_MIN:
        return "red"
    if age_min >= FRESHNESS_WARN_MIN:
        return "yellow"
    return "green"


# ─────────────────────────────────────────────────────────────
#  Graph-sync stages (5 rows from nxsys.graph_sync_state)
# ─────────────────────────────────────────────────────────────

def graph_sync_stages() -> list[dict]:
    # Skip the 'documents' sync_type — leftover from an earlier schema, never actually used.
    sql = """
        SELECT sync_type, status, last_sync_at, last_success_at,
               LEFT(COALESCE(error_message,''), 500) AS error_message
        FROM graph_sync_state
        WHERE sync_type <> 'documents'
        ORDER BY
          CASE sync_type
            WHEN 'emails' THEN 1
            WHEN 'calendar' THEN 2
            WHEN 'teams_chats' THEN 3
            WHEN 'sharepoint' THEN 4
            ELSE 99
          END
    """
    rows = _fetch("nxsys", sql)
    out = []
    for r in rows:
        age = _age_minutes(r["last_success_at"])
        severity = _freshness_severity(age)
        # 'success' and 'partial_error' (= known unfixable on-prem mailbox 404s) are both OK.
        # Only real error statuses escalate severity.
        if r["status"] == "error":
            severity = "red"
        elif r["status"] not in ("success", "idle", "partial_error", "running") and severity == "green":
            severity = "yellow"
        out.append({
            "sync_type": r["sync_type"],
            "status": r["status"],
            "last_sync_at": _iso(r["last_sync_at"]),
            "last_success_at": _iso(r["last_success_at"]),
            "age_minutes": age,
            "severity": severity,
            "error_preview": (r["error_message"] or "").strip() or None,
        })
    # Fireflies sync runs in a different repo (nxsys-working-bot) and writes to
    # its own cursor table — not graph_sync_state. Surface it as an extra row so
    # it shows up in the same panel with the same thresholds logic downstream.
    out = [r for r in out if r.get("sync_type") != "fireflies"]
    ff = fireflies_stage()
    if ff is not None:
        out.append(ff)
    return out


def fireflies_stage() -> dict | None:
    """Synthetic stage row for the Fireflies transcript sync.

    Shape matches graph_sync_stages() rows so the Graph Sync — Stages table,
    pipeline severity roll-up, and issues() handler all pick it up with no
    template or caller changes.

    fireflies_sync_state is owned by the nxsys-working-bot repo, so the schema
    isn't controlled here — read defensively and pick the most recent datetime
    column as the cursor watermark.

    Thresholds are tighter than FRESHNESS_*: the sync job runs hourly, so a
    2h-old cursor means at least one cycle has failed.
    """
    try:
        rows = _fetch("nxsys", "SELECT * FROM fireflies_sync_state LIMIT 20")
    except Exception as e:  # noqa: BLE001
        # Most likely: table doesn't exist, or permissions. Either way it's a
        # real monitoring gap — surface as red with the DB error preview.
        return {
            "sync_type": "fireflies",
            "status": "error",
            "last_sync_at": None,
            "last_success_at": None,
            "age_minutes": None,
            "severity": "red",
            "error_preview": f"fireflies_sync_state unreadable: {str(e)[:240]}",
        }

    if not rows:
        return {
            "sync_type": "fireflies",
            "status": "empty",
            "last_sync_at": None,
            "last_success_at": None,
            "age_minutes": None,
            "severity": "yellow",
            "error_preview": "fireflies_sync_state has no rows — sync job has never written cursor state",
        }

    # Find the most recent datetime across every column of every row.
    latest: datetime | None = None
    for r in rows:
        for v in r.values():
            if isinstance(v, datetime):
                if latest is None or v > latest:
                    latest = v

    age = _age_minutes(latest)
    if age is None:
        return {
            "sync_type": "fireflies",
            "status": "no_timestamp",
            "last_sync_at": None,
            "last_success_at": None,
            "age_minutes": None,
            "severity": "yellow",
            "error_preview": "fireflies_sync_state has no datetime column — cannot determine cursor age",
        }

    if age >= 360:      # 6h+ → multiple missed hourly cycles
        severity = "red"
        status = "stale"
        err = f"Cursor hasn't advanced in {_fmt_age(age)} — hourly sync has missed multiple cycles"
    elif age >= 120:    # 2h+ → at least one missed cycle
        severity = "yellow"
        status = "stale"
        err = f"Cursor is {_fmt_age(age)} old — at least one hourly cycle has been skipped"
    else:
        severity = "green"
        status = "success"
        err = None

    return {
        "sync_type": "fireflies",
        "status": status,
        "last_sync_at": _iso(latest),
        "last_success_at": _iso(latest),
        "age_minutes": age,
        "severity": severity,
        "error_preview": err,
    }


# ─────────────────────────────────────────────────────────────
#  Data freshness — 8 tables in nxsys
# ─────────────────────────────────────────────────────────────

FRESHNESS_SOURCES = [
    # label,                   table,                 data_col,                    sync_col,      warn_min, crit_min
    ("emails",              "emails",              "received_at",               "synced_at",   None,     None),
    ("email_attachments",   "email_attachments",   "created_at",                "created_at",  None,     None),
    ("teams_chats",         "teams_chats",         "last_modified_datetime",    "synced_at",   None,     None),
    ("calendar_events",     "calendar_events",     "start_time",                "synced_at",   None,     None),
    # Meetings & transcripts are bursty — only arrive when a meeting happens.
    # Use a 3-day / 7-day threshold so a quiet evening doesn't cry wolf.
    ("meetings",            "meetings",            "date",                      "created_at",  4320,     10080),
    ("meeting_transcripts", "meeting_transcripts", "created_at",                "created_at",  4320,     10080),
    ("sharepoint_files",    "sharepoint_files",    "last_modified_datetime",    "synced_at",   None,     None),
    ("document_embeddings", "document_embeddings", "created_at",                "created_at",  None,     None),
]


def _severity_with_thresholds(age_min: float | None,
                              warn: int | None, crit: int | None) -> str:
    if age_min is None:
        return "unknown"
    warn = warn if warn is not None else FRESHNESS_WARN_MIN
    crit = crit if crit is not None else FRESHNESS_CRIT_MIN
    if age_min >= crit:
        return "red"
    if age_min >= warn:
        return "yellow"
    return "green"


def data_freshness() -> list[dict]:
    out = []
    for label, table, data_col, sync_col, warn_min, crit_min in FRESHNESS_SOURCES:
        sql = (
            f"SELECT COUNT(*) AS rows, "
            f"       MAX({data_col}) AS latest_data, "
            f"       MAX({sync_col}) AS latest_sync "
            f"FROM {table}"
        )
        try:
            r = _fetch_one("nxsys", sql)
        except Exception as e:  # noqa: BLE001
            out.append({
                "source": label, "rows": None, "latest_data": None,
                "latest_sync": None, "age_minutes": None,
                "severity": "red", "error": str(e),
            })
            continue
        age = _age_minutes(r.get("latest_sync"))
        out.append({
            "source": label,
            "rows": r.get("rows"),
            "latest_data": _iso(r.get("latest_data")),
            "latest_sync": _iso(r.get("latest_sync")),
            "age_minutes": age,
            "severity": _severity_with_thresholds(age, warn_min, crit_min),
            "error": None,
        })
    return out


# ─────────────────────────────────────────────────────────────
#  Active projects (npm_projects)
# ─────────────────────────────────────────────────────────────

def active_projects() -> list[dict]:
    sql = """
        SELECT id, name, project_code, phase, status, health_score,
               target_go_live, go_live_date, updated_at, activated_at
        FROM projects
        WHERE is_active = TRUE
        ORDER BY COALESCE(updated_at, activated_at) DESC NULLS LAST
    """
    rows = _fetch("npm_projects", sql)

    # Per-project entity counts from the deep-dive pillars
    entity_tables = [
        ("gl_accounts",       "sap_gl_accounts"),
        ("business_process",  "sap_business_processes"),
        ("landscape_changes", "sap_landscape_changes"),
        ("cost_centers",      "sap_cost_centers"),
        ("profit_centers",    "sap_profit_centers"),
        ("materials",         "sap_materials"),
        ("customers",         "sap_customers"),
        ("vendors",           "sap_vendors"),
    ]

    for proj in rows:
        counts = {}
        for key, table in entity_tables:
            try:
                r = _fetch_one(
                    "npm_projects",
                    f"SELECT COUNT(*) AS n FROM {table} WHERE project_id = %s",
                    (proj["id"],),
                )
                counts[key] = r.get("n") or 0
            except Exception:
                counts[key] = None
        try:
            r = _fetch_one(
                "npm_projects",
                "SELECT COUNT(*) AS n FROM risks WHERE project_id = %s",
                (proj["id"],),
            )
            counts["risks"] = r.get("n") or 0
        except Exception:
            counts["risks"] = None

        proj["entity_counts"] = counts
        proj["target_go_live"] = _iso(proj.get("target_go_live"))
        proj["go_live_date"]   = _iso(proj.get("go_live_date"))
        proj["updated_at"]     = _iso(proj.get("updated_at"))
        proj["activated_at"]   = _iso(proj.get("activated_at"))

    return rows


# ─────────────────────────────────────────────────────────────
#  Father agent health (agent_brain)
# ─────────────────────────────────────────────────────────────

def father_health() -> dict:
    out: dict[str, Any] = {"tasks": {}, "failures_24h": 0, "conversations_24h": 0,
                            "health_checks": [], "recent_failures": [], "severity": "green"}
    try:
        task_rows = _fetch("agent_brain",
                           "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
        out["tasks"] = {r["status"]: r["n"] for r in task_rows}
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
        out["severity"] = "red"
        return out

    try:
        r = _fetch_one("agent_brain",
                       "SELECT COUNT(*) AS n FROM failures "
                       "WHERE created_at > NOW() - INTERVAL '24 hours' "
                       "  AND (resolved IS NULL OR resolved = FALSE)")
        out["failures_24h"] = r.get("n") or 0
    except Exception:
        pass

    try:
        out["recent_failures"] = _fetch("agent_brain",
            "SELECT failure_id, agent_id, error_type, "
            "       LEFT(COALESCE(error_message,''),200) AS error_message, "
            "       resolved, created_at "
            "FROM failures "
            "WHERE created_at > NOW() - INTERVAL '24 hours' "
            "ORDER BY created_at DESC LIMIT 15")
        for f in out["recent_failures"]:
            f["created_at"] = _iso(f.get("created_at"))
    except Exception:
        pass

    try:
        r = _fetch_one("agent_brain",
                       "SELECT COUNT(*) AS n FROM conversations "
                       "WHERE created_at > NOW() - INTERVAL '24 hours'")
        out["conversations_24h"] = r.get("n") or 0
    except Exception:
        pass

    try:
        checks = _fetch("agent_brain",
            "SELECT DISTINCT ON (component) "
            "  component, status, response_time_ms, checked_at "
            "FROM health_checks "
            "ORDER BY component, checked_at DESC")
        for c in checks:
            c["checked_at"] = _iso(c.get("checked_at"))
            c["age_minutes"] = _age_minutes(
                _parse_iso(c["checked_at"]) if c["checked_at"] else None
            )
        out["health_checks"] = checks
    except Exception:
        pass

    # derive severity
    if out["failures_24h"] > 10:
        out["severity"] = "red"
    elif out["failures_24h"] > 0:
        out["severity"] = "yellow"
    if any(c.get("status") not in ("ok", "healthy", "success", None) for c in out["health_checks"]):
        out["severity"] = "red"
    return out


# ─────────────────────────────────────────────────────────────
#  Insights (ke_insights — ephemeral, refreshed every run)
# ─────────────────────────────────────────────────────────────

def all_projects() -> list[dict]:
    sql = """
        SELECT id, name, project_code, is_active, phase, status
        FROM projects
        ORDER BY is_active DESC, name
    """
    return [dict(r) for r in _fetch("npm_projects", sql)]


def insights_list(project_id: int = 9) -> list[dict]:
    sql = """
        SELECT id, insight_type, title,
               LEFT(COALESCE(description,''), 500) AS description,
               severity, status, detected_by, created_at
        FROM ke_insights
        WHERE project_id = %s
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                          WHEN 'medium' THEN 3 ELSE 4 END,
            insight_type,
            created_at DESC
    """
    rows = _fetch("npm_projects", sql, [project_id])
    for r in rows:
        r["created_at"] = _iso(r.get("created_at"))
    return rows


# ─────────────────────────────────────────────────────────────
#  Risk Register (permanent — items never deleted)
# ─────────────────────────────────────────────────────────────

def risk_register_list(project_id: int = 9) -> list[dict]:
    sql = """
        SELECT risk_code, title, category, probability, impact,
               risk_score, owner, status,
               LEFT(COALESCE(mitigation_plan,''), 300) AS mitigation_plan,
               raised_by, created_at, updated_at
        FROM risk_register
        WHERE project_id = %s
        ORDER BY
            CASE status WHEN 'open' THEN 1 WHEN 'mitigated' THEN 2
                        WHEN 'accepted' THEN 3 WHEN 'false_positive' THEN 4
                        ELSE 5 END,
            CASE impact WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3 WHEN 'low' THEN 4
                        ELSE 5 END,
            risk_code
    """
    rows = _fetch("npm_projects", sql, [project_id])
    for r in rows:
        r["created_at"] = _iso(r.get("created_at"))
        r["updated_at"] = _iso(r.get("updated_at"))
    return rows


# ─────────────────────────────────────────────────────────────
#  Promote insight → risk register (write operation)
# ─────────────────────────────────────────────────────────────

INSIGHT_TYPE_TO_CATEGORY = {
    "risk": "schedule",
    "conflict": "scope",
    "process_gap": "scope",
    "resource_risk": "resource",
    "governance_gap": "compliance",
    "recommendation": "scope",
    "overdue_milestone": "schedule",
    "overdue_deliverable": "schedule",
}


def promote_insight(insight_id: int, project_id: int = 9) -> dict:
    """Promote a ke_insight to a permanent risk_register entry."""
    conn = _conn("npm_projects")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Read the insight
        cur.execute(
            "SELECT * FROM ke_insights WHERE id = %s AND project_id = %s",
            [insight_id, project_id],
        )
        insight = cur.fetchone()
        if not insight:
            return {"ok": False, "error": f"Insight {insight_id} not found"}

        # Check if already promoted
        cur.execute(
            "SELECT risk_code FROM risk_register WHERE source_insight_id = %s",
            [insight_id],
        )
        existing = cur.fetchone()
        if existing:
            return {"ok": False, "error": f"Already promoted as {existing['risk_code']}",
                    "risk_code": existing["risk_code"]}

        # Get next RISK-NNN code
        cur.execute(
            "SELECT COALESCE(MAX(CAST(SUBSTRING(risk_code FROM 6) AS INTEGER)), 0) + 1 AS next_num "
            "FROM risk_register WHERE project_id = %s",
            [project_id],
        )
        next_num = cur.fetchone()["next_num"]
        risk_code = f"RISK-{next_num:03d}"

        # Map severity to impact
        sev = (insight.get("severity") or "medium").lower()
        impact = "high" if sev in ("critical", "high") else "medium"

        # Map insight type to category
        category = INSIGHT_TYPE_TO_CATEGORY.get(
            insight.get("insight_type", ""), "scope"
        )

        # Insert into risk_register
        wcur = conn.cursor()
        wcur.execute("""
            INSERT INTO risk_register
            (project_id, risk_code, title, description, category,
             probability, impact, risk_score, owner, status,
             source_insight_id, raised_by, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s,
                    'high', %s, %s, 'Project Manager', 'open',
                    %s, 'deepak', NOW(), NOW())
        """, [
            project_id,
            risk_code,
            insight["title"],
            insight.get("description") or "",
            category,
            impact,
            6 if impact == "high" else 4,  # risk_score: high=6, medium=4
            insight_id,
        ])

        # Mark insight as promoted
        wcur.execute(
            "UPDATE ke_insights SET status = 'promoted' WHERE id = %s",
            [insight_id],
        )

        conn.commit()
        return {"ok": True, "risk_code": risk_code, "title": insight["title"]}

    finally:
        conn.close()


def dismiss_insight(insight_id: int, project_id: int = 9) -> dict:
    """Dismiss an insight — will not be promoted."""
    conn = _conn("npm_projects")
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE ke_insights SET status = 'dismissed' WHERE id = %s AND project_id = %s",
            [insight_id, project_id],
        )
        conn.commit()
        return {"ok": True, "dismissed": cur.rowcount > 0}
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
#  Project Documents
# ─────────────────────────────────────────────────────────────

def doc_types_catalog() -> list[dict]:
    """All distinct (doc_type, name, phase) tuples across projects — used to
    populate the per-document permission editor."""
    return _fetch("npm_projects", """
        SELECT DISTINCT doc_type, name, phase
        FROM project_documents
        ORDER BY phase, name
    """)


def documents_list(project_id: int = 9) -> list[dict]:
    sql = """
        SELECT id, name, phase, doc_type, sort_order, status,
               LENGTH(COALESCE(draft_html,'')) AS draft_len,
               LENGTH(COALESCE(final_html,'')) AS final_len,
               updated_at
        FROM project_documents
        WHERE project_id = %s
        ORDER BY
            CASE phase WHEN 'ongoing' THEN 0 WHEN 'prepare' THEN 1
                       WHEN 'explore' THEN 2 WHEN 'realize' THEN 3
                       WHEN 'deploy' THEN 4 WHEN 'run' THEN 5 ELSE 9 END,
            sort_order, name
    """
    rows = _fetch("npm_projects", sql, [project_id])
    for r in rows:
        r["updated_at"] = _iso(r.get("updated_at"))
    return rows


def document_get(doc_id: int) -> dict:
    return _fetch_one("npm_projects", """
        SELECT id, project_id, name, phase, doc_type, status,
               draft_html, final_html, updated_at
        FROM project_documents WHERE id = %s
    """, [doc_id])


def _next_version(cur, doc_id: int, version_type: str) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS next_ver FROM document_versions "
        "WHERE document_id = %s AND version_type = %s",
        [doc_id, version_type],
    )
    row = cur.fetchone()
    # Handle both dict cursor and tuple cursor
    if isinstance(row, dict):
        return row["next_ver"]
    return row[0]


def _upload_s3(s3_key: str, html: str, bucket: str = "nxsys-drive"):
    """Upload HTML content to S3."""
    import boto3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket, Key=s3_key,
        Body=html.encode("utf-8"),
        ContentType="text/html",
    )


def _download_s3(s3_key: str, bucket: str = "nxsys-drive") -> str:
    """Download HTML content from S3."""
    import boto3
    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=s3_key)
    return resp["Body"].read().decode("utf-8")


def document_save_draft(doc_id: int, html: str):
    conn = _conn("npm_projects")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get doc metadata for S3 path
        cur.execute("SELECT project_id, doc_type FROM project_documents WHERE id = %s", [doc_id])
        doc = cur.fetchone()
        if not doc:
            return {"ok": False, "error": "Document not found"}

        # Get next draft version
        version = _next_version(cur, doc_id, "draft")

        # S3 key: projects/{project_id}/documents/{doc_type}/draft_v{N}.html
        s3_key = f"projects/{doc['project_id']}/documents/{doc['doc_type']}/draft_v{version}.html"

        # Upload to S3
        _upload_s3(s3_key, html)

        # Record version in DB
        cur.execute("""
            INSERT INTO document_versions (document_id, version, version_type, s3_key)
            VALUES (%s, %s, 'draft', %s)
        """, [doc_id, version, s3_key])

        # Update current draft in project_documents
        cur.execute("""
            UPDATE project_documents
            SET draft_html = %s, status = 'draft', updated_at = NOW()
            WHERE id = %s
        """, [html, doc_id])

        conn.commit()
        return {"ok": True, "version": version, "s3_key": s3_key}
    finally:
        conn.close()


def document_finalize(doc_id: int):
    conn = _conn("npm_projects")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get doc with current draft
        cur.execute("""
            SELECT id, project_id, doc_type, draft_html
            FROM project_documents WHERE id = %s AND draft_html IS NOT NULL AND draft_html != ''
        """, [doc_id])
        doc = cur.fetchone()
        if not doc:
            return {"ok": False, "error": "No draft to finalize"}

        # Get next final version
        version = _next_version(cur, doc_id, "final")

        # S3 key
        s3_key = f"projects/{doc['project_id']}/documents/{doc['doc_type']}/final_v{version}.html"

        # Upload to S3
        _upload_s3(s3_key, doc["draft_html"])

        # Record version
        cur.execute("""
            INSERT INTO document_versions (document_id, version, version_type, s3_key)
            VALUES (%s, %s, 'final', %s)
        """, [doc_id, version, s3_key])

        # Update project_documents
        cur.execute("""
            UPDATE project_documents
            SET final_html = draft_html, status = 'final', updated_at = NOW()
            WHERE id = %s
        """, [doc_id])

        conn.commit()
        return {"ok": True, "version": version, "s3_key": s3_key}
    finally:
        conn.close()


def document_versions(doc_id: int) -> list[dict]:
    rows = _fetch("npm_projects", """
        SELECT id, version, version_type, s3_key, created_by, notes, created_at
        FROM document_versions
        WHERE document_id = %s
        ORDER BY version_type, version DESC
    """, [doc_id])
    for r in rows:
        r["created_at"] = _iso(r.get("created_at"))
    return rows


def document_version_html(version_id: int) -> str:
    row = _fetch_one("npm_projects", """
        SELECT s3_key, s3_bucket FROM document_versions WHERE id = %s
    """, [version_id])
    if not row:
        raise ValueError("Version not found")
    return _download_s3(row["s3_key"], row.get("s3_bucket", "nxsys-drive"))


# ─────────────────────────────────────────────────────────────
#  Kick-off presentation data (read-only, multi-DB)
# ─────────────────────────────────────────────────────────────

def kickoff_project_data(project_id: int) -> dict:
    """Gather all data needed to generate a kick-off presentation for a project."""

    project = _fetch_one("npm_projects", """
        SELECT id, name, client, description, phase, status,
               start_date, target_go_live, methodology, project_code
        FROM projects WHERE id = %s
    """, [project_id])

    milestones = _fetch("npm_projects", """
        SELECT title, phase, target_date, status
        FROM ke_milestones WHERE project_id = %s
        ORDER BY target_date
    """, [project_id])
    for m in milestones:
        m["target_date"] = _iso(m.get("target_date"))

    risks = _fetch("npm_projects", """
        SELECT risk_code, title, category, probability, impact,
               risk_score, owner, status,
               LEFT(COALESCE(mitigation_plan,''), 500) AS mitigation_plan
        FROM risk_register WHERE project_id = %s
        ORDER BY risk_code
    """, [project_id])

    resources = _fetch("npm_projects", """
        SELECT DISTINCT role, workstream, responsibility
        FROM project_resources WHERE project_id = %s
        ORDER BY workstream, role
    """, [project_id])

    systems = _fetch("npm_projects", """
        SELECT system_role, sap_product, host_url
        FROM sap_systems WHERE project_id = %s
        ORDER BY system_role
    """, [project_id])

    deliverables = _fetch("npm_projects", """
        SELECT phase, title
        FROM ke_deliverables WHERE project_id = %s
        ORDER BY phase
    """, [project_id])

    # Meeting summaries from nxsys DB (cross-DB query)
    meetings = []
    try:
        project_name = project.get("name", "")
        keyword = project_name.split("–")[0].split("-")[0].strip() if project_name else ""
        if keyword and len(keyword) >= 3:
            meetings = _fetch("nxsys", """
                SELECT m.title, m.date,
                       LEFT(COALESCE(mt.summary,''), 800) AS summary,
                       LEFT(COALESCE(mt.action_items,''), 800) AS action_items
                FROM meetings m
                LEFT JOIN meeting_transcripts mt ON mt.meeting_id = m.id
                WHERE m.title ILIKE %s
                ORDER BY m.date DESC LIMIT 6
            """, [f"%{keyword}%"])
            for mt in meetings:
                mt["date"] = _iso(mt.get("date"))
    except Exception:
        pass

    # Scope/contract memories from agent_brain
    memories = []
    try:
        memories = _fetch("agent_brain", """
            SELECT content, category FROM memory
            WHERE content ILIKE %s OR category IN ('scope', 'contract')
            ORDER BY created_at DESC LIMIT 10
        """, [f"%{project.get('name', 'xxx').split()[0]}%"])
    except Exception:
        pass

    return {
        "project": project,
        "milestones": milestones,
        "risks": risks,
        "resources": resources,
        "systems": systems,
        "deliverables": deliverables,
        "meetings": meetings,
        "memories": memories,
    }


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        # psycopg returns tz-aware already; the _iso helper keeps +00:00
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
#  Pipeline stage health roll-up — for the top flow diagram
# ─────────────────────────────────────────────────────────────

def _worst(*severities: str) -> str:
    order = {"green": 0, "unknown": 1, "yellow": 2, "red": 3}
    return max(severities, key=lambda s: order.get(s, 1))


def pipeline_stages(stages: list[dict], freshness: list[dict],
                    projects: list[dict], father: dict) -> list[dict]:
    """
    8 logical pipeline stages for the top-of-page flow diagram.
    Each gets a severity based on the underlying data already computed.
    """
    by_stage = {s["sync_type"]: s for s in stages}
    by_src   = {f["source"]:     f for f in freshness}

    def sev(key: str, src: dict | None) -> str:
        return (src or {}).get("severity", "unknown")

    # 1. Humans — can't measure directly; use "are emails flowing?" as proxy
    humans_sev = by_src.get("emails", {}).get("severity", "unknown")

    # 2. M365 / Fireflies — blend two signals:
    #   (a) meeting + transcript data freshness (bursty — only when meetings
    #       happen, so a lenient 3d/7d threshold is still right for the data
    #       itself), and
    #   (b) the fireflies sync JOB health from fireflies_sync_state, which has
    #       tight 2h/6h thresholds because the job runs hourly. This is the
    #       signal that catches "a meeting happened today but the sync job
    #       never picked it up" — the exact case the lenient data-age
    #       threshold would otherwise hide.
    def _meeting_sev(src: dict | None) -> str:
        if not src:
            return "unknown"
        age = src.get("age_minutes")
        if age is None:
            return "yellow"
        if age >= 10080:    # 7 days
            return "red"
        if age >= 4320:     # 3 days
            return "yellow"
        return "green"

    ff_sev = by_stage.get("fireflies", {}).get("severity", "unknown")
    m365_sev = _worst(
        _meeting_sev(by_src.get("meetings")),
        _meeting_sev(by_src.get("meeting_transcripts")),
        ff_sev,
    )

    # 3. Graph Sync — worst of the 5 sync stages
    gs_sev = _worst(*[s.get("severity", "unknown") for s in stages] or ["unknown"])

    # 4. nxsys DB — worst of raw data freshness
    nxsys_sev = _worst(
        by_src.get("emails", {}).get("severity", "unknown"),
        by_src.get("teams_chats", {}).get("severity", "unknown"),
        by_src.get("calendar_events", {}).get("severity", "unknown"),
        by_src.get("sharepoint_files", {}).get("severity", "unknown"),
    )

    # 5. Embeddings
    emb_sev = by_src.get("document_embeddings", {}).get("severity", "unknown")

    # 6. npm_projects — green if at least one active project exists; projects
    # don't update hourly, so use a week-scale threshold before yellow/red.
    if projects:
        ages = [_age_minutes(_parse_iso(p.get("updated_at"))) for p in projects]
        most_recent = min([a for a in ages if a is not None], default=None)
        if most_recent is None:
            proj_sev = "yellow"
        elif most_recent >= 20160:   # 14 days
            proj_sev = "red"
        elif most_recent >= 10080:   # 7 days
            proj_sev = "yellow"
        else:
            proj_sev = "green"
    else:
        proj_sev = "red"

    # 7. Father
    father_sev = father.get("severity", "unknown")

    # 8. Reports — proxy: tasks completed in last 24h
    reports_sev = "green" if (father.get("tasks", {}).get("completed", 0) or 0) > 0 else "yellow"

    return [
        {"key": "humans",      "label": "Humans",         "severity": humans_sev},
        {"key": "m365",        "label": "M365/Fireflies", "severity": m365_sev},
        {"key": "graph_sync",  "label": "Graph Sync",     "severity": gs_sev},
        {"key": "nxsys",       "label": "nxsys DB",       "severity": nxsys_sev},
        {"key": "embeddings",  "label": "Embeddings",     "severity": emb_sev},
        {"key": "projects",    "label": "Projects DB",    "severity": proj_sev},
        {"key": "father",      "label": "Father Agent",   "severity": father_sev},
        {"key": "reports",     "label": "Reports",        "severity": reports_sev},
    ]


# ─────────────────────────────────────────────────────────────
#  Issues panel — aggregate red flags
# ─────────────────────────────────────────────────────────────

def issues(stages: list[dict], freshness: list[dict], father: dict) -> list[dict]:
    out: list[dict] = []

    for s in stages:
        # Only surface error previews when the stage severity is actually a problem.
        # 'partial_error' with green severity = known unfixable on-prem mailbox 404s,
        # not actionable, don't spam the issues panel with them.
        if s.get("error_preview") and s.get("severity") in ("yellow", "red"):
            out.append({
                "severity": s["severity"],
                "source": f"graph_sync.{s['sync_type']}",
                "message": s["error_preview"][:300],
                "at": s.get("last_sync_at"),
            })
        if s.get("severity") == "red":
            out.append({
                "severity": "red",
                "source": f"graph_sync.{s['sync_type']}",
                "message": f"Stage {s['sync_type']} has not succeeded recently "
                           f"(last success {s.get('last_success_at') or 'never'})",
                "at": s.get("last_success_at"),
            })

    for f in freshness:
        if f.get("error"):
            out.append({
                "severity": "red",
                "source": f"freshness.{f['source']}",
                "message": f"Query failed: {f['error'][:200]}",
                "at": None,
            })
            continue
        age = f.get("age_minutes")
        if age is None:
            continue  # no data yet — not necessarily a problem, don't spam issues
        if f.get("severity") == "red":
            out.append({
                "severity": "red",
                "source": f"freshness.{f['source']}",
                "message": f"Sync lag {_fmt_age(age)}",
                "at": f.get("latest_sync"),
            })
        elif f.get("severity") == "yellow":
            out.append({
                "severity": "yellow",
                "source": f"freshness.{f['source']}",
                "message": f"Sync lag {_fmt_age(age)}",
                "at": f.get("latest_sync"),
            })

    for rf in father.get("recent_failures") or []:
        out.append({
            "severity": "red" if not rf.get("resolved") else "yellow",
            "source": f"father.{rf.get('agent_id') or 'unknown'}",
            "message": f"{rf.get('error_type') or 'failure'}: {rf.get('error_message') or ''}",
            "at": rf.get("created_at"),
        })

    sev_order = {"red": 0, "yellow": 1, "green": 2, "unknown": 3}
    out.sort(key=lambda i: (sev_order.get(i["severity"], 3), i.get("at") or ""))
    return out


# ─────────────────────────────────────────────────────────────
#  Entry point — one call returns the whole dashboard state
# ─────────────────────────────────────────────────────────────

def dashboard_state(project_id: int | None = None) -> dict:
    started = datetime.now(timezone.utc)
    errors: list[str] = []

    def safe(label: str, fn, default):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            errors.append(f"{label}: {e}")
            return default

    # Resolve project_id: use the provided one, or default to first active project
    all_proj = safe("all_projects", all_projects, [])
    if project_id is None:
        active = [p for p in all_proj if p.get("is_active")]
        project_id = active[0]["id"] if active else 9

    stages    = safe("graph_sync_stages", graph_sync_stages, [])
    freshness = safe("data_freshness",    data_freshness,    [])
    projects  = safe("active_projects",   active_projects,   [])
    father    = safe("father_health",     father_health,     {"severity": "unknown"})
    insights  = safe("insights_list",     lambda: insights_list(project_id),     [])
    risks     = safe("risk_register_list",lambda: risk_register_list(project_id),[])

    pipeline  = pipeline_stages(stages, freshness, projects, father)
    issues_l  = issues(stages, freshness, father)

    overall = _worst(*[p["severity"] for p in pipeline]) if pipeline else "unknown"

    return {
        "generated_at": started.isoformat(),
        "overall": overall,
        "pipeline": pipeline,
        "stages": stages,
        "freshness": freshness,
        "projects": projects,
        "father": father,
        "issues": issues_l,
        "insights": insights,
        "risk_register": risks,
        "all_projects": all_proj,
        "selected_project_id": project_id,
        "thresholds": {
            "warn_minutes": FRESHNESS_WARN_MIN,
            "crit_minutes": FRESHNESS_CRIT_MIN,
        },
        "query_errors": errors,
    }


# ─────────────────────────────────────────────────────────────
#  Living Project Plan (Gantt) — versioned with cached render
# ─────────────────────────────────────────────────────────────

import json as _json
from datetime import date as _date, timedelta as _timedelta


_SAP_PHASES = [
    ("discover",  "Discover"),
    ("prepare",   "Prepare"),
    ("explore",   "Explore"),
    ("realize",   "Realize"),
    ("deploy",    "Deploy"),
    ("run",       "Run (Hypercare)"),
]


def _distribute_phases(start: _date, end: _date, go_live: _date | None = None) -> list[dict]:
    """Split [start, end] into the six SAP Activate phases.

    If `go_live` is provided, the 5 pre-go-live phases (Discover → Deploy) span
    [start, go_live] proportionally, and Run (Hypercare) covers (go_live, end].
    Otherwise, falls back to simple proportional weights across the full span.
    """
    # Pre-go-live weights (Discover, Prepare, Explore, Realize, Deploy).
    pre_weights = [0.09, 0.13, 0.28, 0.33, 0.17]  # normalized to 1.0
    full_weights = pre_weights + [0.10]           # + Run

    spans: list[dict] = []
    if go_live and start < go_live <= end:
        total = max((go_live - start).days, 5)
        cursor = start
        for (key, label), w in zip(_SAP_PHASES[:5], pre_weights):
            dur = max(int(total * w), 1)
            phase_end = cursor + _timedelta(days=dur - 1)
            if phase_end > go_live:
                phase_end = go_live
            spans.append({"key": key, "label": label, "start": cursor, "end": phase_end})
            cursor = phase_end + _timedelta(days=1)
        # Anchor Deploy to end exactly on go-live.
        spans[-1]["end"] = go_live
        # Run / Hypercare: day after go-live → project end.
        run_start = go_live + _timedelta(days=1)
        run_end   = end if end > run_start else run_start
        spans.append({"key": "run", "label": _SAP_PHASES[5][1],
                      "start": run_start, "end": run_end})
    else:
        total_days = max((end - start).days, 6)
        cursor = start
        for (key, label), w in zip(_SAP_PHASES, full_weights):
            dur = max(int(total_days * w), 1)
            phase_end = cursor + _timedelta(days=dur - 1)
            if phase_end > end:
                phase_end = end
            spans.append({"key": key, "label": label, "start": cursor, "end": phase_end})
            cursor = phase_end + _timedelta(days=1)
            if cursor > end:
                cursor = end
        if spans:
            spans[-1]["end"] = end
    return spans


def _placeholder_tasks(project: dict, milestones: list[dict]) -> list[dict]:
    """Build a seed Gantt task list from project dates + SAP phases + ke_milestones."""
    def _to_date(v):
        if v is None: return None
        if isinstance(v, _date): return v
        if isinstance(v, str): return _date.fromisoformat(v[:10])
        return None

    start = _to_date(project.get("start_date")) or _date.today()
    end   = _to_date(project.get("target_go_live")) or (start + _timedelta(days=180))
    if end <= start:
        end = start + _timedelta(days=180)

    # Resolve Go-Live: prefer a ke_milestones row titled literally "Go-Live"
    # (or "Phase One Go-Live"). Fallback to projects.target_go_live.
    go_live = None
    for m in (milestones or []):
        title = (m.get("title") or "").strip().lower()
        if title in ("go-live", "go live", "phase one go-live", "phase 1 go-live"):
            d = _to_date(m.get("target_date"))
            if d and (go_live is None or d < go_live):
                go_live = d
    if go_live is None:
        go_live = _to_date(project.get("target_go_live")) or end
    # Make sure end is at least go-live + a hypercare tail.
    if end < go_live:
        end = go_live + _timedelta(days=30)

    tasks: list[dict] = []
    prev_phase_key = None
    for span in _distribute_phases(start, end, go_live=go_live):
        tasks.append({
            "task_key":         f"phase_{span['key']}",
            "parent_key":       None,
            "name":             span["label"],
            "sap_phase":        span["key"],
            "workstream":       None,
            "owner":            "NXSYS PMO",
            "start_date":       span["start"].isoformat(),
            "end_date":         span["end"].isoformat(),
            "percent_complete": 0,
            "is_milestone":     False,
            "depends_on":       [prev_phase_key] if prev_phase_key else [],
            "source_refs":      {"seed": "placeholder"},
        })
        # Deploy's phase-gate IS the Go-Live milestone — don't create a duplicate.
        if span["key"] == "deploy":
            tasks.append({
                "task_key":         "go_live",
                "parent_key":       "phase_deploy",
                "name":             "Go-Live",
                "sap_phase":        "deploy",
                "workstream":       None,
                "owner":            "Client + NXSYS",
                "start_date":       go_live.isoformat(),
                "end_date":         go_live.isoformat(),
                "percent_complete": 0,
                "is_milestone":     True,
                "depends_on":       ["phase_deploy"],
                "source_refs":      {"seed": "placeholder", "type": "go_live"},
            })
        else:
            tasks.append({
                "task_key":         f"gate_{span['key']}",
                "parent_key":       f"phase_{span['key']}",
                "name":             f"{span['label']} — Phase Gate",
                "sap_phase":        span["key"],
                "workstream":       None,
                "owner":            "Steering Committee",
                "start_date":       span["end"].isoformat(),
                "end_date":         span["end"].isoformat(),
                "percent_complete": 0,
                "is_milestone":     True,
                "depends_on":       [f"phase_{span['key']}"],
                "source_refs":      {"seed": "placeholder"},
            })
        prev_phase_key = f"phase_{span['key']}"

    # Note: ke_milestones are intentionally NOT dumped into the placeholder.
    # They are auto-extracted from documents and contain many duplicates /
    # micro-entries that clutter the Gantt. A placeholder v1 should be the
    # clean SAP Activate skeleton (phases + gates only). Father-driven
    # generation will layer in curated tasks from validated meetings later.
    return tasks


def _json_default(o):
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return str(o)


def _build_cached_render(plan_row: dict, tasks: list[dict]) -> dict:
    """Shape the payload the frontend needs — served as-is from cached_render."""
    return {
        "plan_id":       plan_row["id"],
        "project_id":    plan_row["project_id"],
        "version":       plan_row["version"],
        "status":        plan_row["status"],
        "generated_at":  _iso(plan_row.get("generated_at")),
        "generated_by":  plan_row.get("generated_by"),
        "source_summary": plan_row.get("source_summary"),
        "tasks":         tasks,
    }


def plan_get_latest(project_id: int) -> dict | None:
    """Latest plan for a project (any status), served from cached_render."""
    row = _fetch_one("npm_projects", """
        SELECT id, project_id, version, status, generated_at, generated_by,
               source_summary, cached_render
        FROM project_plans
        WHERE project_id = %s
        ORDER BY version DESC
        LIMIT 1
    """, [project_id])
    if not row:
        return None
    cached = row.get("cached_render")
    if cached:
        if isinstance(cached, str):
            cached = _json.loads(cached)
        return cached
    # Fallback: rebuild from tasks table if cache missing.
    tasks = plan_get_tasks(row["id"])
    return _build_cached_render(row, tasks)


def plan_get_version(plan_id: int) -> dict | None:
    row = _fetch_one("npm_projects", """
        SELECT id, project_id, version, status, generated_at, generated_by,
               source_summary, cached_render
        FROM project_plans WHERE id = %s
    """, [plan_id])
    if not row:
        return None
    cached = row.get("cached_render")
    if cached:
        if isinstance(cached, str):
            cached = _json.loads(cached)
        return cached
    tasks = plan_get_tasks(plan_id)
    return _build_cached_render(row, tasks)


def plan_get_tasks(plan_id: int) -> list[dict]:
    rows = _fetch("npm_projects", """
        SELECT task_key, parent_key, name, sap_phase, workstream, owner,
               start_date, end_date, percent_complete, is_milestone,
               depends_on, source_refs
        FROM project_plan_tasks
        WHERE plan_id = %s
        ORDER BY start_date, task_key
    """, [plan_id])
    for r in rows:
        r["start_date"] = r["start_date"].isoformat() if r.get("start_date") else None
        r["end_date"]   = r["end_date"].isoformat() if r.get("end_date") else None
        r["depends_on"] = list(r.get("depends_on") or [])
    return rows


def plan_list_versions(project_id: int) -> list[dict]:
    rows = _fetch("npm_projects", """
        SELECT id, version, status, generated_at, generated_by,
               source_summary,
               (SELECT COUNT(*) FROM project_plan_tasks t WHERE t.plan_id = p.id) AS task_count
        FROM project_plans p
        WHERE project_id = %s
        ORDER BY version DESC
    """, [project_id])
    for r in rows:
        r["generated_at"] = _iso(r.get("generated_at"))
    return rows


def plan_create_placeholder(project_id: int, generated_by: str = "placeholder") -> dict:
    """Create a new draft plan version seeded from project metadata + ke_milestones."""
    project = _fetch_one("npm_projects", """
        SELECT id, name, start_date, target_go_live, methodology
        FROM projects WHERE id = %s
    """, [project_id])
    if not project:
        return {"ok": False, "error": f"project {project_id} not found"}

    milestones = _fetch("npm_projects", """
        SELECT title, phase, target_date
        FROM ke_milestones WHERE project_id = %s
        ORDER BY target_date
    """, [project_id])

    tasks = _placeholder_tasks(project, milestones)

    conn = _conn("npm_projects")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Next version number
        cur.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM project_plans WHERE project_id=%s",
            [project_id],
        )
        next_v = cur.fetchone()["v"]

        source_summary = (
            f"Placeholder seeded from project start {project.get('start_date')} "
            f"→ go-live {project.get('target_go_live')} "
            f"across 6 SAP Activate phases + phase gates. "
            f"({len(milestones)} ke_milestones available in project data but not auto-imported — "
            f"curated tasks will be added by Father-driven regeneration.)"
        )

        cur.execute("""
            INSERT INTO project_plans
                (project_id, version, status, generated_by, source_summary, cached_render)
            VALUES (%s, %s, 'draft', %s, %s, NULL)
            RETURNING id, project_id, version, status, generated_at, generated_by, source_summary
        """, [project_id, next_v, generated_by, source_summary])
        plan_row = dict(cur.fetchone())

        # Insert tasks
        for t in tasks:
            cur.execute("""
                INSERT INTO project_plan_tasks
                    (plan_id, task_key, parent_key, name, sap_phase, workstream, owner,
                     start_date, end_date, percent_complete, is_milestone,
                     depends_on, source_refs)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, [
                plan_row["id"], t["task_key"], t["parent_key"], t["name"],
                t["sap_phase"], t["workstream"], t["owner"],
                t["start_date"], t["end_date"], t["percent_complete"],
                t["is_milestone"], t["depends_on"],
                _json.dumps(t["source_refs"]) if t.get("source_refs") else None,
            ])

        # Build and store cached_render
        cached = _build_cached_render(plan_row, tasks)
        cur.execute(
            "UPDATE project_plans SET cached_render=%s WHERE id=%s",
            [_json.dumps(cached, default=_json_default), plan_row["id"]],
        )

        conn.commit()
        return {"ok": True, "plan_id": plan_row["id"], "version": next_v, "tasks": len(tasks)}
    finally:
        conn.close()


def plan_approve_final(plan_id: int) -> dict:
    """Flip a draft to final, supersede any prior final for the same project. Atomic."""
    conn = _conn("npm_projects")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, project_id, status FROM project_plans WHERE id=%s", [plan_id])
        row = cur.fetchone()
        if not row:
            return {"ok": False, "error": "plan not found"}
        if row["status"] == "final":
            return {"ok": True, "already_final": True, "plan_id": plan_id}

        cur.execute("""
            UPDATE project_plans SET status='superseded'
            WHERE project_id=%s AND status='final' AND id<>%s
        """, [row["project_id"], plan_id])
        cur.execute("UPDATE project_plans SET status='final' WHERE id=%s", [plan_id])

        # Refresh cached_render.status
        cur.execute("""
            SELECT id, project_id, version, status, generated_at, generated_by, source_summary
            FROM project_plans WHERE id=%s
        """, [plan_id])
        plan_row = dict(cur.fetchone())
        tasks = plan_get_tasks(plan_id)
        cached = _build_cached_render(plan_row, tasks)
        cur.execute(
            "UPDATE project_plans SET cached_render=%s WHERE id=%s",
            [_json.dumps(cached), plan_id],
        )
        conn.commit()
        return {"ok": True, "plan_id": plan_id, "status": "final"}
    finally:
        conn.close()
