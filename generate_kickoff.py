"""
Nexora Monitor — Kick-off Presentation Generator.

Plain HTML slides (no JS) — scrollable, printable, with NXSYS branding.
Logo fetched from npm_projects.presentation_assets.
Claude generates a complete HTML document with embedded CSS.
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
    """Fetch the NXSYS logo data URI from the DB."""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER,
                            password=DB_PASSWORD, dbname="npm_projects")
    try:
        cur = conn.cursor()
        cur.execute("SELECT content FROM presentation_assets WHERE key = 'nxsys_logo'")
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


SYSTEM_PROMPT = """You are a professional SAP consulting presentation designer for NXSYS Consulting.
Generate a complete, standalone HTML kick-off meeting presentation from the project data provided.

## MANDATORY CSS — USE THIS EXACTLY

Place this inside a <style> tag. Do NOT modify the design system.

```css
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

:root {
  --primary: #C8102E;
  --accent: #ff4d6d;
  --risk-high: #E53935;
  --risk-medium: #FB8C00;
  --risk-low: #43A047;
  --phase-prepare: #7E57C2;
  --phase-explore: #1E88E5;
  --phase-realize: #00897B;
  --phase-deploy: #F4511E;
  --phase-run: #43A047;
}

* { margin: 0; padding: 0; box-sizing: border-box; }
html { scroll-snap-type: y mandatory; scroll-behavior: smooth; }
body { font-family: 'Inter', sans-serif; background: #f0f2f5; color: #333; overflow-y: auto; height: 100vh; scroll-snap-type: y mandatory; }

.slide {
  width: 100vw; height: 100vh; background: #fff; overflow: hidden;
  page-break-after: always; position: relative; border-bottom: 1px solid #e5e7eb;
  scroll-snap-align: start; scroll-snap-stop: always;
}

/* Nav arrows */
.nav-dot { position: fixed; right: 24px; top: 50%; transform: translateY(-50%); z-index: 1000; display: flex; flex-direction: column; gap: 8px; }
.nav-dot button { width: 10px; height: 10px; border-radius: 50%; border: 2px solid rgba(0,0,0,0.2); background: transparent; cursor: pointer; padding: 0; transition: all 0.2s; }
.nav-dot button:hover { border-color: #C8102E; transform: scale(1.2); }
.nav-dot button.active { background: #C8102E; border-color: #C8102E; }
.nav-arrows { position: fixed; right: 24px; bottom: 24px; z-index: 1000; display: flex; flex-direction: column; gap: 8px; }
.nav-arrows button { width: 44px; height: 44px; border-radius: 50%; border: 1px solid rgba(0,0,0,0.1); background: #fff; cursor: pointer; font-size: 18px; color: #666; box-shadow: 0 2px 8px rgba(0,0,0,0.1); transition: all 0.2s; }
.nav-arrows button:hover { background: #C8102E; color: #fff; border-color: #C8102E; }
.slide-counter-fixed { position: fixed; top: 24px; right: 24px; z-index: 1000; background: rgba(255,255,255,0.9); padding: 6px 14px; border-radius: 50px; font-size: 12px; font-weight: 600; color: #666; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
.slide-header {
  background: linear-gradient(135deg, #0D1B2A 0%, #0A2463 100%);
  color: #fff; padding: 28px 60px 22px; position: relative;
}
.slide-header::after {
  content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px;
  background: linear-gradient(90deg, var(--primary), var(--accent));
}
.slide-header h1 { font-size: 28px; font-weight: 800; }
.slide-header h2 { font-size: 13px; font-weight: 400; opacity: 0.6; margin-top: 4px; }
.slide-number { position: absolute; top: 28px; right: 60px; font-size: 13px; opacity: 0.3; color: #fff; }
.slide-body { padding: 32px 60px 40px; }
.slide-footer {
  position: absolute; bottom: 0; left: 0; right: 0; padding: 10px 60px;
  display: flex; justify-content: space-between; font-size: 10px;
  color: #bbb; border-top: 1px solid #f0f0f0;
}

/* Title slide */
.title-slide {
  display: flex; flex-direction: column; justify-content: center; align-items: center;
  text-align: center; background: linear-gradient(135deg, #0D1B2A 0%, #0A2463 50%, #0D1B2A 100%);
  color: #fff;
}
.title-slide .logo-img { height: 80px; margin-bottom: 30px; }
.title-slide h1 { font-size: 46px; font-weight: 900; line-height: 1.15; max-width: 800px; color: #fff; }
.title-slide .subtitle { font-size: 18px; font-weight: 300; margin-top: 12px; opacity: 0.7; }
.title-slide .badge {
  display: inline-block; background: var(--primary); color: #fff;
  padding: 8px 24px; border-radius: 50px; font-size: 11px; font-weight: 700;
  margin-top: 20px; letter-spacing: 1.5px; text-transform: uppercase;
}
.title-slide .date { margin-top: 36px; font-size: 13px; opacity: 0.35; border-top: 1px solid rgba(255,255,255,0.15); padding-top: 20px; }

/* Tables */
table { width: 100%; border-collapse: collapse; font-size: 12.5px; margin-top: 12px; }
th { background: #fef2f2; color: var(--primary); font-weight: 600; text-align: left; padding: 10px 14px; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid var(--primary); }
td { padding: 10px 14px; border-bottom: 1px solid #f0f0f0; color: #444; }
tr:hover td { background: #fafafa; }

/* Info boxes */
.info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 12px; }
.info-box { background: #f8f9fa; border-radius: 10px; padding: 18px 22px; border-left: 4px solid var(--primary); }
.info-box.gold { border-left-color: var(--accent); }
.info-box.green { border-left-color: var(--risk-low); }
.info-box.red { border-left-color: var(--risk-high); }
.info-box.purple { border-left-color: var(--phase-prepare); }
.info-box h3 { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #999; margin-bottom: 6px; }
.info-box p { font-size: 15px; font-weight: 700; color: #1a1a1a; }
.info-box .sm { font-size: 11px; font-weight: 400; color: #666; margin-top: 4px; }

/* Modules */
.module-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-top: 14px; }
.module-card { background: linear-gradient(135deg, var(--primary), var(--accent)); color: #fff; border-radius: 10px; padding: 18px; text-align: center; }
.module-card h3 { font-size: 17px; font-weight: 800; }
.module-card p { font-size: 11px; opacity: 0.85; margin-top: 3px; }
.module-card.tech { background: linear-gradient(135deg, #37474F, #546E7A); }
.module-card.btp { background: linear-gradient(135deg, #1565C0, #42A5F5); }
.module-card.drc { background: linear-gradient(135deg, #6A1B9A, #AB47BC); }

/* Phase badges */
.pb { display: inline-block; padding: 3px 10px; border-radius: 50px; font-size: 10px; font-weight: 700; text-transform: uppercase; color: #fff; }
.pb-prepare { background: var(--phase-prepare); }
.pb-explore { background: var(--phase-explore); }
.pb-realize { background: var(--phase-realize); }
.pb-deploy { background: var(--phase-deploy); }
.pb-run { background: var(--phase-run); }

.sb { display: inline-block; padding: 3px 10px; border-radius: 50px; font-size: 10px; font-weight: 700; text-transform: uppercase; }
.sb-open { background: #FFEBEE; color: var(--risk-high); }
.sb-planned { background: #E3F2FD; color: #1E88E5; }
.sb-done { background: #E8F5E9; color: var(--risk-low); }

.rh { color: var(--risk-high); font-weight: 700; }

/* Section titles */
.st { font-size: 14px; font-weight: 800; color: #1a1a1a; margin-bottom: 2px; }
.ss { font-size: 11px; color: #999; margin-bottom: 10px; }

/* Timeline */
.timeline { display: flex; align-items: flex-start; margin-top: 20px; position: relative; padding: 0 10px; }
.timeline::before {
  content: ''; position: absolute; top: 22px; left: 10px; right: 10px; height: 4px; border-radius: 2px;
  background: linear-gradient(90deg, var(--phase-prepare) 0% 15%, var(--phase-explore) 15% 42%, var(--phase-realize) 42% 60%, var(--phase-deploy) 60% 85%, var(--phase-run) 85%);
}
.tl-item { flex: 1; text-align: center; position: relative; padding-top: 36px; }
.tl-item::before { content: ''; position: absolute; top: 16px; left: 50%; transform: translateX(-50%); width: 14px; height: 14px; border-radius: 50%; border: 3px solid #fff; box-shadow: 0 0 0 2px #ccc; background: #fff; }
.tl-item.active::before { background: var(--primary); box-shadow: 0 0 0 2px var(--primary); }
.tl-item h4 { font-size: 13px; font-weight: 700; color: #1a1a1a; }
.tl-item .dates { font-size: 11px; color: #888; margin-top: 2px; }
.tl-item .desc { font-size: 10px; color: #aaa; margin-top: 4px; line-height: 1.4; }

/* Callout */
.callout { padding: 16px 20px; border-radius: 10px; font-size: 13px; margin-top: 14px; }
.callout-warn { background: #FFF8E1; border-left: 4px solid #F9A825; }
.callout-blue { background: #E3F2FD; border-left: 4px solid #1E88E5; }

/* Org cards */
.org-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 10px; }
.org-card { background: #f8f9fa; border-radius: 8px; padding: 14px; border-top: 3px solid var(--primary); }
.org-card.client { border-top-color: var(--accent); }
.org-card h4 { font-size: 11px; font-weight: 700; color: #1a1a1a; }
.org-card p { font-size: 10px; color: #666; margin-top: 3px; }

/* Next steps */
.ns { list-style: none; margin-top: 12px; }
.ns li { padding: 12px 16px; background: #f8f9fa; border-radius: 8px; margin-bottom: 8px; border-left: 4px solid var(--primary); font-size: 13px; display: flex; justify-content: space-between; align-items: center; }
.ns li .owner { font-size: 10px; color: var(--primary); font-weight: 700; text-transform: uppercase; }
.ns li .due { font-size: 10px; color: #888; }
.ns li.critical { border-left-color: var(--risk-high); }

/* Governance */
.gov-flow { display: flex; align-items: center; gap: 12px; margin-top: 14px; }
.gov-box { flex: 1; background: #f8f9fa; border-radius: 10px; padding: 16px; text-align: center; border: 2px solid #e5e7eb; }
.gov-box.active { border-color: var(--primary); background: #fef2f2; }
.gov-box h4 { font-size: 12px; font-weight: 700; color: #1a1a1a; }
.gov-box p { font-size: 10px; color: #666; margin-top: 3px; }
.gov-arrow { font-size: 18px; color: #ccc; }

/* Architecture */
.arch-box { flex: 1; border: 2px solid var(--primary); border-radius: 8px; padding: 12px; text-align: center; background: #fef2f2; }
.arch-box h4 { font-size: 13px; font-weight: 700; color: var(--primary); }
.arch-box p { font-size: 10px; color: #666; margin-top: 2px; }
.arch-box.gold { border-color: var(--accent); }
.arch-box.dashed { border-style: dashed; opacity: 0.6; }
.arch-connector { text-align: center; color: #ccc; font-size: 14px; margin: 6px 0; }

/* RACI */
.raci td { text-align: center; font-size: 11px; }
.raci td:first-child { text-align: left; font-weight: 600; color: #1a1a1a; }
.raci .r { background: #fef2f2; color: var(--primary); font-weight: 700; }
.raci .a { background: #FFF8E1; color: #F57F17; font-weight: 700; }
.raci .c { background: #F3E5F5; color: #6A1B9A; }
.raci .i { background: #f5f5f5; color: #999; }

/* Escalation */
.esc-path { display: flex; gap: 0; margin-top: 10px; }
.esc-box { flex: 1; padding: 14px; text-align: center; }
.esc-box h4 { font-size: 10px; font-weight: 700; text-transform: uppercase; }
.esc-box .role { font-size: 13px; font-weight: 700; color: #1a1a1a; margin-top: 4px; }
.esc-box .detail { font-size: 10px; color: #888; }
.esc-l1 { background: #E8F5E9; border-radius: 10px 0 0 10px; border: 1px solid #C8E6C9; }
.esc-l1 h4 { color: var(--risk-low); }
.esc-l2 { background: #FFF3E0; border: 1px solid #FFE0B2; }
.esc-l2 h4 { color: var(--risk-medium); }
.esc-l3 { background: #FFEBEE; border-radius: 0 10px 10px 0; border: 1px solid #FFCDD2; }
.esc-l3 h4 { color: var(--risk-high); }

@media print {
  .slide { height: auto; min-height: 100vh; border-bottom: none; }
}
```

## LOGO

Use this exact img tag for the NXSYS logo wherever needed (title slide, closing slide):
```html
<img src="%%LOGO_URI%%" alt="NXSYS" class="logo-img">
```

## SLIDE STRUCTURE (9 slides)

1. **Title Slide** — class="slide title-slide". Logo image, project name as white h1, subtitle, red badge "CONFIDENTIAL", date. Dark navy background.
2. **Program Purpose & Objectives** — 4-6 objectives in .info-grid > .info-box cards
3. **Phase 1 Scope & Target Architecture** — Two-column flex: left = .module-card grid (ONLY in-scope modules, NO out-of-scope items); right = architecture using .arch-box + .arch-connector
4. **Delivery Approach & Timeline** — .timeline + milestones table with .pb and .sb badges
5. **Governance & Decision Structure** — .gov-flow + .esc-path escalation + .info-grid communication discipline
6. **Roles & Responsibilities** — .org-grid cards for teams + .raci time commitment table
7. **Risks & Dependencies** — Risk table + .info-box grid for dependencies
8. **Next Steps & Commitments** — ul.ns with .critical items + .callout-blue commitment
9. **Thank You** — class="slide title-slide" with logo image, "Thank You", contact info

## NAVIGATION (add this AFTER all slides, before </body>)

Include this exact navigation HTML and inline script after the last slide:

```html
<div class="slide-counter-fixed"><span id="curSlide">1</span> / <span id="totSlide">9</span></div>
<div class="nav-dot" id="navDots"></div>
<div class="nav-arrows">
  <button onclick="goSlide(-1)" title="Previous">&uarr;</button>
  <button onclick="goSlide(1)" title="Next">&darr;</button>
</div>
<script>
(function(){
  var slides = document.querySelectorAll('.slide');
  var dotsContainer = document.getElementById('navDots');
  var curEl = document.getElementById('curSlide');
  var totEl = document.getElementById('totSlide');
  totEl.textContent = slides.length;
  for (var i = 0; i < slides.length; i++) {
    var b = document.createElement('button');
    b.dataset.idx = i;
    b.onclick = function() { slides[this.dataset.idx].scrollIntoView({behavior:'smooth'}); };
    dotsContainer.appendChild(b);
  }
  function currentIdx() {
    var y = window.scrollY + window.innerHeight/2;
    for (var i = 0; i < slides.length; i++) {
      var r = slides[i].getBoundingClientRect();
      if (r.top + window.scrollY <= y && r.bottom + window.scrollY >= y) return i;
    }
    return 0;
  }
  function updateDots() {
    var idx = currentIdx();
    curEl.textContent = idx + 1;
    var dots = dotsContainer.querySelectorAll('button');
    for (var i = 0; i < dots.length; i++) dots[i].classList.toggle('active', i === idx);
  }
  window.goSlide = function(delta) {
    var next = Math.max(0, Math.min(slides.length - 1, currentIdx() + delta));
    slides[next].scrollIntoView({behavior:'smooth'});
  };
  document.addEventListener('keydown', function(e) {
    if (e.key === 'ArrowDown' || e.key === 'PageDown' || e.key === ' ') { e.preventDefault(); goSlide(1); }
    if (e.key === 'ArrowUp' || e.key === 'PageUp') { e.preventDefault(); goSlide(-1); }
    if (e.key === 'Home') { slides[0].scrollIntoView({behavior:'smooth'}); }
    if (e.key === 'End') { slides[slides.length-1].scrollIntoView({behavior:'smooth'}); }
  });
  window.addEventListener('scroll', updateDots, { passive: true });
  updateDots();
})();
</script>
```

## RULES
- Output a COMPLETE standalone HTML document with <!DOCTYPE html>, <head>, <style>, <body>
- All CSS must be inside the HTML in a <style> tag — no external files
- Include the navigation HTML + script block above (AFTER the last slide, before </body>)
- Use %%LOGO_URI%% as the src for all logo img tags — it will be replaced after generation
- All content from PROJECT DATA only. Don't invent.
- Return ONLY the HTML. No markdown fences, no commentary.

## CRITICAL — POSITIVE FRAMING (CLIENT-FACING DOCUMENT)
- NEVER show out-of-scope modules (PP, SD, etc.) — no grey cards, no strikethrough, no "Out of Scope" section
- Only display what IS in scope as strong, positive cards
- Do not use .module-card.out class — it does not exist
- The client should feel they are getting a comprehensive solution, not that they are missing something

## CRITICAL — RISK FILTERING (CLIENT-FACING DOCUMENT)
This presentation is shared with the CLIENT. You MUST filter risks:
- NEVER include risks that expose NXSYS internal issues: staffing gaps, consultant unavailability, internal tooling not set up, NXSYS resource problems
- ONLY include neutral project-level risks: data migration complexity, business resource availability, scope control, integration dependencies, timeline pressure, client-side dependencies
- If a risk blames NXSYS, either SKIP it entirely or REFRAME it neutrally (e.g. "No confirmed SF consultant" → "HR workstream resource confirmation pending")
- The client should never see anything that undermines NXSYS credibility
"""

REFINE_SYSTEM_PROMPT = """You are refining an HTML kick-off presentation for NXSYS Consulting.

Apply the user's changes. Rules:
- Return the COMPLETE updated HTML document
- Preserve ALL CSS and class names exactly
- NO JavaScript — pure HTML+CSS only
- Use %%LOGO_URI%% as src for logo images
- Return ONLY the HTML, no commentary

## CURRENT HTML:

"""


def _format_project_data(data: dict) -> str:
    lines = []
    proj = data.get("project", {})
    lines.append("## PROJECT OVERVIEW")
    lines.append(f"Name: {proj.get('name', 'Unknown')}")
    lines.append(f"Client: {proj.get('client', 'Unknown')}")
    lines.append(f"Description: {proj.get('description', 'N/A')}")
    lines.append(f"Phase: {proj.get('phase', 'N/A')}")
    lines.append(f"Status: {proj.get('status', 'N/A')}")
    lines.append(f"Start Date: {proj.get('start_date', 'TBD')}")
    lines.append(f"Target Go-Live: {proj.get('target_go_live', 'TBD')}")
    lines.append(f"Methodology: {proj.get('methodology', 'SAP Activate')}")
    lines.append("")

    mems = data.get("memories", [])
    if mems:
        lines.append("## CONFIRMED SCOPE DECISIONS")
        for m in mems:
            lines.append(f"- {m.get('content', '')}")
        lines.append("")

    ms = data.get("milestones", [])
    if ms:
        lines.append("## KEY MILESTONES")
        for m in ms:
            lines.append(f"- {m.get('title', '?')} | Phase: {m.get('phase', '?')} | Target: {m.get('target_date', 'TBD')} | Status: {m.get('status', '?')}")
        lines.append("")

    risks = data.get("risks", [])
    if risks:
        lines.append("## RISK REGISTER")
        for r in risks:
            lines.append(f"- {r.get('risk_code', '?')}: {r.get('title', '?')} | {r.get('category', '?')} | P:{r.get('probability', '?')} I:{r.get('impact', '?')} | Owner: {r.get('owner', '?')}")
            if r.get("mitigation_plan"):
                lines.append(f"  Mitigation: {r['mitigation_plan']}")
        lines.append("")

    res = data.get("resources", [])
    if res:
        lines.append("## PROJECT RESOURCES")
        for r in res:
            lines.append(f"- {r.get('role', '?')} | {r.get('workstream', '?')} | {r.get('responsibility', '')[:150]}")
        lines.append("")

    sys_list = data.get("systems", [])
    if sys_list:
        lines.append("## SAP SYSTEM LANDSCAPE")
        for s in sys_list:
            lines.append(f"- {s.get('system_role', '?')}: {s.get('sap_product', '?')}")
        lines.append("")

    deliv = data.get("deliverables", [])
    if deliv:
        lines.append("## KEY DELIVERABLES")
        for d in deliv:
            lines.append(f"- [{d.get('phase', '?')}] {d.get('title', '?')}")
        lines.append("")

    mtgs = data.get("meetings", [])
    if mtgs:
        lines.append("## RECENT MEETINGS")
        for mt in mtgs:
            lines.append(f"### {mt.get('title', 'Meeting')} ({mt.get('date', '?')})")
            if mt.get("summary"):
                lines.append(f"Summary: {mt['summary']}")
            if mt.get("action_items"):
                lines.append(f"Actions: {mt['action_items']}")
            lines.append("")

    return "\n".join(lines)


def _extract_html(text: str) -> str:
    text = re.sub(r'^```html?\s*\n', '', text.strip())
    text = re.sub(r'\n```\s*$', '', text.strip())
    if '<html' not in text.lower() and '<!doctype' not in text.lower():
        raise ValueError("Response does not contain valid HTML")
    return text


def _inject_logo(html: str, logo_uri: str) -> str:
    return html.replace("%%LOGO_URI%%", logo_uri)


async def generate_html(project_data: dict) -> str:
    client = anthropic.AsyncAnthropic()
    logo_uri = _get_logo()
    formatted_data = _format_project_data(project_data)

    response = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Generate the kick-off presentation for:\n\n{formatted_data}",
        }],
    )

    html = _extract_html(response.content[0].text)
    return _inject_logo(html, logo_uri)


async def refine_html(current_html: str, prompt: str, project_data: dict) -> str:
    client = anthropic.AsyncAnthropic()
    logo_uri = _get_logo()
    formatted_data = _format_project_data(project_data)

    # Strip the logo data URI before sending to Claude (saves tokens)
    current_for_claude = current_html.replace(logo_uri, "%%LOGO_URI%%")

    response = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=[
            {
                "type": "text",
                "text": REFINE_SYSTEM_PROMPT + current_for_claude,
                "cache_control": {"type": "ephemeral"},
            },
            {
                "type": "text",
                "text": f"\n\n## PROJECT DATA:\n\n{formatted_data}",
            },
        ],
        messages=[{
            "role": "user",
            "content": prompt,
        }],
    )

    html = _extract_html(response.content[0].text)
    return _inject_logo(html, logo_uri)
