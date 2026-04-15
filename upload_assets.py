"""One-time script to upload presentation assets to the DB."""
import base64
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SKILL_DIR = os.environ.get("SKILL_DIR", r"C:\Users\DeepakSaxena\.claude\skills\animated-presentations\templates")
LOGO_PATH = os.environ.get("LOGO_PATH", r"D:\Dev\AgentSDK\Color logo with background.png")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "npm")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

def upsert(cur, key, content_type, content, description):
    cur.execute("""
        INSERT INTO presentation_assets (key, content_type, content, description, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET content = EXCLUDED.content, content_type = EXCLUDED.content_type,
            description = EXCLUDED.description, updated_at = NOW()
    """, [key, content_type, content, description])
    print(f"  {key}: {len(content)} chars")

def main():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname="npm_projects")
    cur = conn.cursor()

    # Logo as base64 data URI
    print("Uploading logo...")
    with open(LOGO_PATH, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    logo_uri = f"data:image/png;base64,{b64}"
    upsert(cur, "nxsys_logo", "image/png", logo_uri, "NXSYS logo - Color with background (PNG, base64 data URI)")

    # CSS template
    print("Uploading CSS template...")
    css_path = os.path.join(SKILL_DIR, "presentation-styles.css")
    with open(css_path, "r") as f:
        css = f.read()
    upsert(cur, "presentation_css", "text/css", css, "Animated presentation CSS framework (3351 lines) from animated-presentations skill")

    # JS engine
    print("Uploading JS engine...")
    js_path = os.path.join(SKILL_DIR, "presentation-script.js")
    with open(js_path, "r") as f:
        js = f.read()
    upsert(cur, "presentation_js", "text/javascript", js, "Presentation engine JS (523 lines) - keyboard nav, presenter mode, counters")

    # HTML template shell
    print("Uploading HTML template...")
    html_path = os.path.join(SKILL_DIR, "index-template.html")
    with open(html_path, "r") as f:
        html = f.read()
    upsert(cur, "presentation_html_template", "text/html", html, "HTML shell template with placeholders for slides")

    conn.commit()
    cur.close()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
