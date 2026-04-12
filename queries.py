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
    return out


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

    # 2. M365 / Fireflies — meetings + transcripts. These don't arrive on a
    # fixed cadence (only when a meeting actually happens), so we use a
    # much more lenient threshold: warn after 3 days, red after 7 days.
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

    m365_sev = _worst(
        _meeting_sev(by_src.get("meetings")),
        _meeting_sev(by_src.get("meeting_transcripts")),
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
