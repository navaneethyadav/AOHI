#!/usr/bin/env python3
"""
api/generate_report_pro.py

Generate a professional PDF summary of incidents/rca.

Usage:
  python api/generate_report_pro.py --out ../data/AOHI_Final_Report.pdf --name "Navaneeth Kaku" --api http://127.0.0.1:8000/rca

Notes:
 - Uses PNG logo at data/aohi_logo.png if present (preferred).
 - If logo PDF exists it will be ignored to avoid PIL issues. Use PNG.
 - Tries to fetch RCA from --api URL (requests). If that fails, falls back to local rca.json in project root.
 - Avoids RenderPM/rlPyCairo by not using vector render backends.
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

# reportlab / pillow imports
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import mm

# Try optional requests (for calling local API)
try:
    import requests
except Exception:
    requests = None

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGO_PNG = DATA_DIR / "aohi_logo.png"
LOCAL_RCA = ROOT / "rca.json"


def safe_load_rca(api_url: str | None = None) -> Dict[str, Any]:
    """
    Prefer API fetch if api_url provided and requests available.
    Otherwise try local rca.json. Return structure with 'incidents' list.
    """
    if api_url and requests:
        try:
            resp = requests.get(api_url, timeout=8)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Warning: failed to fetch {api_url}: {e}", file=sys.stderr)

    # fallback to local file
    if LOCAL_RCA.exists():
        try:
            return json.loads(LOCAL_RCA.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Warning: failed to parse local {LOCAL_RCA}: {e}", file=sys.stderr)

    # last fallback: empty structure
    return {"incidents": []}


def ensure_styles():
    styles = getSampleStyleSheet()
    # Code style: replace or add (safe)
    code_style = ParagraphStyle(name="Code", fontName="Courier", fontSize=8, leading=10)
    try:
        # some ReportLab versions provide .byName
        if hasattr(styles, "byName") and "Code" in styles.byName:
            styles.byName["Code"] = code_style
        else:
            # try add safely
            try:
                styles.add(code_style)
            except Exception:
                # ignore if already present
                pass
    except Exception:
        # final fallback, ignore
        pass

    # Add or safe-replace a 'Small' style for table small text
    small_style = ParagraphStyle(name="Small", fontSize=8, leading=9)
    try:
        if hasattr(styles, "byName") and "Small" in styles.byName:
            styles.byName["Small"] = small_style
        else:
            try:
                styles.add(small_style)
            except Exception:
                pass
    except Exception:
        pass

    return styles


def build_pdf(output: Path, rca_json: Dict[str, Any], author_name: str | None = None):
    doc = SimpleDocTemplate(str(output), pagesize=A4,
                            rightMargin=18 * mm, leftMargin=18 * mm,
                            topMargin=20 * mm, bottomMargin=20 * mm)
    styles = ensure_styles()
    story: List[Any] = []

    # Header: logo + title
    if LOGO_PNG.exists():
        try:
            img = Image(str(LOGO_PNG))
            img.drawHeight = 20 * mm
            img.drawWidth = img.drawHeight * (img.imageWidth / img.imageHeight if img.imageHeight else 3.0)
            story.append(img)
            story.append(Spacer(1, 6))
        except Exception as e:
            print(f"Warning: failed to add logo: {e}", file=sys.stderr)

    title_text = "AOHI — Adaptive Operational Health Intelligence\nProfessional Incident Report"
    story.append(Paragraph(title_text, styles["Title"]))

    if author_name:
        story.append(Paragraph(f"<b>Prepared for:</b> {author_name}", styles["Normal"]))
    story.append(Spacer(1, 6))

    # Summary
    incidents = rca_json.get("incidents", [])
    total_incidents = len(incidents)
    # try to compute failed tx count if present by summing evidence.failed_count where available
    total_failed = 0
    for inc in incidents:
        for rc in inc.get("root_causes", []):
            ev = rc.get("evidence", {})
            if isinstance(ev, dict) and "failed_count" in ev:
                try:
                    total_failed += int(ev.get("failed_count", 0))
                except Exception:
                    pass

    summary = f"Generated: {output.name}<br/>Total incidents detected: {total_incidents}<br/>Total failed transactions (sum evidence): {total_failed}"
    story.append(Paragraph(summary, styles["Normal"]))
    story.append(Spacer(1, 8))

    # Build each incident
    for inc in incidents:
        bucket = inc.get("incident_bucket", "<unknown>")
        detected_by = inc.get("detected_by") or []
        root_causes = inc.get("root_causes") or []
        playbooks = inc.get("playbooks") or []

        story.append(Paragraph(f"<b>Incident:</b> {bucket}", styles["Heading3"]))
        story.append(Paragraph(f"<b>Detected by:</b> {', '.join(detected_by) if detected_by else 'N/A'}", styles["Normal"]))
        story.append(Spacer(1, 4))

        # Root causes table
        if root_causes:
            table_data = [["Root cause", "Description", "Evidence"]]
            for rc in root_causes:
                name = rc.get("root_cause") or rc.get("root_cause", "") or rc.get("name") or ""
                desc = rc.get("description") or ""
                ev = rc.get("evidence") or ""
                ev_text = json.dumps(ev) if isinstance(ev, (dict, list)) else str(ev)
                table_data.append([Paragraph(str(name), styles["Code"]),
                                   Paragraph(str(desc), styles["Normal"]),
                                   Paragraph(ev_text, styles["Small"])])

            t = Table(table_data, colWidths=[60 * mm, 70 * mm, 40 * mm], repeatRows=1)
            t.setStyle(TableStyle([
                ("GRID", (0, 0), (-1, -1), 0.25, colors.gray),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            story.append(t)
            story.append(Spacer(1, 6))

        # Playbooks
        if playbooks:
            story.append(Paragraph("<b>Suggested playbooks:</b>", styles["Normal"]))
            for p in playbooks:
                owner = p.get("owner", "")
                priority = p.get("priority", "")
                steps = p.get("steps", "")
                story.append(Paragraph(f"- <b>{owner}</b> [{priority}] : {steps}", styles["Normal"]))
            story.append(Spacer(1, 8))

        story.append(Spacer(1, 6))

    # Footer note
    story.append(Spacer(1, 12))
    story.append(Paragraph("End of report.", styles["Normal"]))

    # Build document
    doc.build(story)


def main(argv: List[str] | None = None):
    parser = argparse.ArgumentParser(description="Generate AOHI professional report PDF.")
    parser.add_argument("--out", "-o", required=True, help="Output PDF path")
    parser.add_argument("--api", help="Optional RCA API URL (e.g. http://127.0.0.1:8000/rca)")
    parser.add_argument("--name", help="Name to include in report header")
    args = parser.parse_args(argv)

    out_path = Path(args.out).resolve()
    # Ensure parent dir exists
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rca_json = safe_load_rca(args.api)

    try:
        build_pdf(out_path, rca_json, author_name=args.name)
        print(f"Report written to {out_path}")
        return 0
    except Exception as e:
        # Print minimal traceback to stdout for subprocess caller
        import traceback
        tb = traceback.format_exc()
        print("ERROR building report:", file=sys.stderr)
        print(tb, file=sys.stderr)
        return 2


if __name__ == "__main__":
    rc = main()
    # if script is run directly, exit code
    sys.exit(rc)
