"""
Nexora Monitor — Generic Document Generator.

Generates HTML documents for different doc_types using Claude API + project data.
Each doc_type has its own system prompt defining the document structure.
"""

import os
import re

import anthropic
import psycopg2

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 16000

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "npm")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")


def _get_logo() -> str:
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER,
                            password=DB_PASSWORD, dbname="npm_projects")
    try:
        cur = conn.cursor()
        cur.execute("SELECT content FROM presentation_assets WHERE key = 'nxsys_logo'")
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


# ── Shared CSS for all documents (light theme, NXSYS branding) ──

DOC_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Inter', sans-serif; background: #fff; color: #333; line-height: 1.6; }
.doc { max-width: 900px; margin: 0 auto; padding: 40px 60px; }
.doc-header { border-bottom: 3px solid #C8102E; padding-bottom: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: flex-start; }
.doc-header .logo-img { height: 50px; }
.doc-header .doc-meta { text-align: right; font-size: 12px; color: #888; }
.doc-header .doc-meta .doc-title { font-size: 24px; font-weight: 800; color: #1a1a1a; margin-bottom: 4px; }
.doc-header .doc-meta .doc-subtitle { font-size: 14px; color: #666; }
h1 { font-size: 22px; font-weight: 800; color: #1a1a1a; margin: 28px 0 12px; border-left: 4px solid #C8102E; padding-left: 16px; }
h2 { font-size: 17px; font-weight: 700; color: #333; margin: 20px 0 8px; }
h3 { font-size: 14px; font-weight: 600; color: #444; margin: 14px 0 6px; }
p { margin: 8px 0; font-size: 13px; }
table { width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 12.5px; }
th { background: #fef2f2; color: #C8102E; font-weight: 600; text-align: left; padding: 10px 14px; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid #C8102E; }
td { padding: 10px 14px; border-bottom: 1px solid #f0f0f0; }
tr:hover td { background: #fafafa; }
ul, ol { margin: 8px 0 8px 24px; font-size: 13px; }
li { margin: 4px 0; }
.info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin: 12px 0; }
.info-box { background: #f8f9fa; border-radius: 8px; padding: 16px 20px; border-left: 4px solid #C8102E; }
.info-box h3 { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #999; margin: 0 0 4px; border: none; padding: 0; }
.info-box p { font-size: 14px; font-weight: 600; color: #1a1a1a; margin: 0; }
.info-box .sm { font-size: 11px; color: #666; margin-top: 4px; }
.badge { display: inline-block; padding: 3px 10px; border-radius: 50px; font-size: 10px; font-weight: 700; text-transform: uppercase; }
.badge-red { background: #FFEBEE; color: #E53935; }
.badge-green { background: #E8F5E9; color: #43A047; }
.badge-blue { background: #E3F2FD; color: #1E88E5; }
.badge-amber { background: #FFF8E1; color: #F57F17; }
.callout { padding: 14px 18px; border-radius: 8px; margin: 12px 0; font-size: 13px; }
.callout-info { background: #E3F2FD; border-left: 4px solid #1E88E5; }
.callout-warn { background: #FFF8E1; border-left: 4px solid #F9A825; }
.callout-red { background: #FFEBEE; border-left: 4px solid #E53935; }
.signature-block { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; display: grid; grid-template-columns: 1fr 1fr; gap: 40px; }
.signature-box { border-bottom: 1px solid #333; padding-bottom: 40px; }
.signature-box .label { font-size: 11px; color: #888; text-transform: uppercase; margin-top: 8px; }
.doc-footer { margin-top: 40px; padding-top: 16px; border-top: 1px solid #e5e7eb; font-size: 10px; color: #bbb; display: flex; justify-content: space-between; }
@media print { .doc { padding: 20px 40px; } }
"""

# ── Per-document-type system prompts ──

DOC_PROMPTS = {
    "system_landscape": """Generate a System Landscape Document for an SAP S/4HANA implementation project.

Sections:
1. Document header with NXSYS logo and document meta (title, project, date, version, status)
2. Executive Summary — 2-3 paragraph overview
3. System Landscape Overview — table of all SAP systems (SID, role, product, environment)
4. Environment Strategy — DEV/QAS/PRD/SBX/DR purpose and transport path
5. Infrastructure Architecture — hosting (ECS/private cloud), network, VPN requirements
6. Integration Architecture — BTP, DRC, MBC, third-party systems
7. Security & Access — authorization strategy, VPN, SSO requirements
8. Appendix — version history table, approval section with signature blocks""",

    "sap_boq": """Generate an SAP Bill of Quantities (BOQ) Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta
2. Executive Summary
3. Licensing Summary — table of SAP products/modules, license type, quantity, status
4. Module Breakdown — for each in-scope module: module name, scope description, key deliverables
5. FUE/Named User Summary — user type breakdown table
6. Cloud Services — ECS, BTP, integration suite details
7. Professional Services — workstream effort breakdown table (consultant roles, estimated days per phase)
8. Assumptions & Exclusions
9. Approval section with signature blocks""",

    "project_charter": """Generate a Project Charter Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta
2. Project Overview — name, code, client, SI partner, start date, target go-live
3. Project Purpose & Business Case — why this project, strategic objectives
4. Scope Summary — in-scope modules (only what's confirmed, no out-of-scope items), entities, phases
5. Key Milestones — table with milestone, phase, target date
6. Project Organization — governance structure, steering committee, project board, roles
7. Communication Plan — meeting cadence, reporting, escalation path
8. Success Criteria — measurable outcomes
9. Assumptions, Constraints & Dependencies
10. Approval & Sign-off — signature blocks for both parties""",

    "project_scope": """Generate a Project Scope Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta
2. Project Background
3. Scope Definition — detailed per-module scope: for each module list specific processes, transactions, configurations included
4. Entity Scope — table of entities/legal entities in scope with go-live wave
5. Deliverables per Phase — table of key deliverables by SAP Activate phase
6. Integration Scope — systems to integrate, middleware, APIs
7. Data Migration Scope — objects to migrate, source systems, approach
8. Training Scope — end-user training, super-user training, documentation
9. Change Control Process — how scope changes are managed
10. Approval section with signature blocks""",

    "kickoff_ppt": """This is a kick-off presentation. Use the kick-off presentation generator instead.""",

    "contract": """Generate a Contract Summary Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta
2. Contract Overview — parties, effective date, contract reference, governing law
3. Scope of Services — high-level summary of contracted services per module
4. Commercial Terms — pricing structure summary (without revealing actual amounts — use placeholders)
5. Timeline & Milestones — contracted delivery timeline and key dates
6. Roles & Obligations — NXSYS obligations vs Client obligations table
7. Service Levels — SLA commitments, response times, availability
8. Change Control — how contract amendments are managed
9. Warranty & Liability — warranty period, limitation of liability summary
10. Approval section with signature blocks""",

    "sow": """Generate a Statement of Work (SOW) Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta
2. Project Background & Objectives
3. Scope of Work — detailed per-module deliverables and activities
4. Deliverables Table — deliverable name, description, acceptance criteria, phase, due date
5. Project Approach — SAP Activate methodology, phased delivery
6. Resource Plan — NXSYS resources by role and workstream, estimated effort per phase
7. Client Responsibilities — what the client must provide (resources, data, access, decisions)
8. Assumptions & Prerequisites
9. Acceptance Criteria — how deliverables are reviewed and approved
10. Approval section with signature blocks""",

    "risk_register_doc": """Generate a Risk Register Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta
2. Risk Management Approach — methodology, risk scoring matrix (probability x impact)
3. Risk Scoring Matrix — visual 3x3 or 5x5 grid showing risk levels
4. Active Risks Table — risk ID, title, category, probability, impact, risk score, owner, status, mitigation plan, target date
5. Risk Trends — summary of new risks, closed risks, escalated risks
6. Dependencies Register — key dependencies with owner and status
7. Issues Log — active issues requiring resolution
8. Review cadence and next review date
NOTE: Only include neutral project-level risks. Never include risks that expose NXSYS internal issues.""",

    "project_plan": """Generate a Living Project Plan Document for an SAP S/4HANA implementation.

Sections:
1. Document header with NXSYS logo and meta — mark as "Living Document — Updated Regularly"
2. Project Summary — name, client, methodology, current phase, overall status
3. Phase Overview — SAP Activate phases with date ranges and current status
4. Key Milestones — comprehensive table with milestone, phase, target date, actual date, status, owner
5. Current Phase Activities — what's happening now, key workstreams active
6. Resource Allocation — current team deployment by workstream
7. Upcoming Deadlines — next 30 days view
8. Decision Log — key decisions made with date and who decided
9. Change Log — document revision history
10. Next Review Date""",
}

# ── Shared rules appended to all prompts ──

DOC_RULES = """

## FORMAT
- Output a COMPLETE standalone HTML document
- Use this CSS framework inside a <style> tag — copy it exactly:
""" + f"```css\n{DOC_CSS}\n```" + """
- Wrap all content in <div class="doc">
- Start with <div class="doc-header"> containing the logo and document meta
- Use %%LOGO_URI%% as src for logo images
- End with <div class="doc-footer">
- NO JavaScript — pure HTML+CSS only

## RULES
- All content from PROJECT DATA only — do not invent facts
- If data is missing, write "To be confirmed"
- NEVER show out-of-scope modules — only what IS in scope
- NEVER include NXSYS internal risks or staffing issues
- Client-facing document — professional, positive framing
- Return ONLY the HTML. No markdown fences, no commentary.
"""

REFINE_PROMPT = """You are refining an HTML document for NXSYS Consulting. Apply the user's changes.
Return the COMPLETE updated HTML. Preserve all CSS and structure. Use %%LOGO_URI%% for logo src.
No JavaScript. Return ONLY the HTML.

## CURRENT HTML:

"""


def _format_project_data(data: dict) -> str:
    """Same formatter as generate_kickoff.py"""
    lines = []
    proj = data.get("project", {})
    lines.append(f"## PROJECT: {proj.get('name', 'Unknown')}")
    lines.append(f"Client: {proj.get('client', 'Unknown')}")
    lines.append(f"Description: {proj.get('description', 'N/A')}")
    lines.append(f"Phase: {proj.get('phase', 'N/A')} | Status: {proj.get('status', 'N/A')}")
    lines.append(f"Start: {proj.get('start_date', 'TBD')} | Go-Live: {proj.get('target_go_live', 'TBD')}")
    lines.append(f"Methodology: {proj.get('methodology', 'SAP Activate')}")
    lines.append("")

    for m in data.get("memories", []):
        lines.append(f"SCOPE: {m.get('content', '')}")
    lines.append("")

    if data.get("milestones"):
        lines.append("## MILESTONES")
        for m in data["milestones"]:
            lines.append(f"- {m.get('title','?')} | {m.get('phase','?')} | {m.get('target_date','TBD')} | {m.get('status','?')}")
        lines.append("")

    if data.get("risks"):
        lines.append("## RISKS")
        for r in data["risks"]:
            lines.append(f"- {r.get('risk_code','?')}: {r.get('title','?')} | {r.get('category','?')} | P:{r.get('probability','?')} I:{r.get('impact','?')}")
        lines.append("")

    if data.get("resources"):
        lines.append("## RESOURCES")
        for r in data["resources"]:
            lines.append(f"- {r.get('role','?')} | {r.get('workstream','?')} | {r.get('responsibility','')[:120]}")
        lines.append("")

    if data.get("systems"):
        lines.append("## SYSTEMS")
        for s in data["systems"]:
            lines.append(f"- {s.get('system_role','?')}: {s.get('sap_product','?')}")
        lines.append("")

    if data.get("deliverables"):
        lines.append("## DELIVERABLES")
        for d in data["deliverables"]:
            lines.append(f"- [{d.get('phase','?')}] {d.get('title','?')}")
        lines.append("")

    if data.get("meetings"):
        lines.append("## RECENT MEETINGS")
        for mt in data["meetings"]:
            lines.append(f"### {mt.get('title','')} ({mt.get('date','?')})")
            if mt.get("summary"): lines.append(mt["summary"])
            lines.append("")

    return "\n".join(lines)


def _extract_html(text: str) -> str:
    text = re.sub(r'^```html?\s*\n', '', text.strip())
    text = re.sub(r'\n```\s*$', '', text.strip())
    if '<html' not in text.lower() and '<!doctype' not in text.lower():
        raise ValueError("Response does not contain valid HTML")
    return text


async def generate_doc(doc_type: str, project_data: dict) -> str:
    if doc_type == "kickoff_ppt":
        # Delegate to the kickoff generator
        import generate_kickoff
        return await generate_kickoff.generate_html(project_data)

    prompt_template = DOC_PROMPTS.get(doc_type)
    if not prompt_template:
        raise ValueError(f"Unknown document type: {doc_type}")

    client = anthropic.AsyncAnthropic()
    logo_uri = _get_logo()
    formatted_data = _format_project_data(project_data)

    system = prompt_template + DOC_RULES

    response = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": f"Generate the document:\n\n{formatted_data}"}],
    )

    html = _extract_html(response.content[0].text)
    return html.replace("%%LOGO_URI%%", logo_uri)


async def refine_doc(doc_type: str, current_html: str, prompt: str, project_data: dict) -> str:
    if doc_type == "kickoff_ppt":
        import generate_kickoff
        return await generate_kickoff.refine_html(current_html, prompt, project_data)

    client = anthropic.AsyncAnthropic()
    logo_uri = _get_logo()
    formatted_data = _format_project_data(project_data)
    current_for_claude = current_html.replace(logo_uri, "%%LOGO_URI%%")

    response = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=[
            {"type": "text", "text": REFINE_PROMPT + current_for_claude, "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": f"\n\n## PROJECT DATA:\n\n{formatted_data}"},
        ],
        messages=[{"role": "user", "content": prompt}],
    )

    html = _extract_html(response.content[0].text)
    return html.replace("%%LOGO_URI%%", logo_uri)
