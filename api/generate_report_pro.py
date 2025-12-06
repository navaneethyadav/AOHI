# api/generate_report_pro.py
"""
Simple professional report generator for AOHI.
Usage:
  python api/generate_report_pro.py --out ../data/AOHI_Final_Report.pdf --name "Your Name" --api http://127.0.0.1:8000/rca
"""

import argparse
import json
from pathlib import Path
import sys
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader

# Optional requests (only used if --api provided)
try:
    import requests
except Exception:
    requests = None

ROOT = Path(__file__).parent.parent.resolve()
DATA_DIR = ROOT / "data"
DEFAULT_LOGO_PNG = DATA_DIR / "aohi_logo.png"

def load_rca_from_api(api_url: str):
    if requests is None:
        raise RuntimeError("requests library not available to call API")
    r = requests.get(api_url, timeout=10)
    r.raise_for_status()
    return r.json()

def load_local_rca():
    # prefer rca_saved.json or incidents_full.json
    candidates = [DATA_DIR / "rca.json", DATA_DIR / "incidents_full.json", DATA_DIR / "incidents.json"]
    for p in candidates:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
    return {"rca": []}

def add_logo_to_story(story, styles, logo_path):
    # add a small logo if PNG is available; ImageReader handles PNG/JPEG.
    if logo_path.exists():
        try:
            ir = ImageReader(str(logo_path))
            img = Image(ir, width=80*mm, height=24*mm)
            story.append(img)
            story.append(Spacer(1, 6))
            return
        except Exception:
            # fallthrough to text title
            pass
    # fallback: text title
    story.append(Paragraph("<b>AOHI - Adaptive Operational Health Intelligence</b>", styles["Title"]))
    story.append(Spacer(1, 6))

def build_pdf(out_path: Path, name: str = None, api_url: str = None):
    styles = getSampleStyleSheet()
    # Avoid duplicate style error
    if "Code" not in styles.byName:
        styles.add(ParagraphStyle(name="Code", fontName="Courier", fontSize=8, leading=10))

    doc = SimpleDocTemplate(str(out_path), pagesize=A4, rightMargin=20*mm, leftMargin=20*mm, topMargin=20*mm, bottomMargin=20*mm)
    story = []

    # Logo or title
    add_logo_to_story(story, styles, DEFAULT_LOGO_PNG)

    # Header
    if name:
        story.append(Paragraph(f"<b>Prepared for:</b> {name}", styles["Normal"]))
    story.append(Paragraph(f"<b>Generated:</b> {Path().resolve().name}", styles["Normal"]))
    story.append(Spacer(1, 12))

    # Load RCA/incidents
    rca_data = None
    if api_url:
        try:
            rca_data = load_rca_from_api(api_url)
        except Exception as e:
            # fallback to local
            rca_data = None

    if rca_data is None:
        rca_data = load_local_rca()

    # rca_data structure may vary. Try to normalize
    items = []
    if isinstance(rca_data, dict):
        # if top-level has 'rca' or 'incidents' keys
        if "rca" in rca_data and isinstance(rca_data["rca"], list):
            items = rca_data["rca"]
        elif "incidents" in rca_data and isinstance(rca_data["incidents"], list):
            items = rca_data["incidents"]
        else:
            # maybe it's already a list-like dict (single entry)
            # try to convert dict->list
            items = [rca_data]
    elif isinstance(rca_data, list):
        items = rca_data
    else:
        items = [rca_data]

    if len(items) == 0:
        story.append(Paragraph("No incidents or RCA data found.", styles["Normal"]))
    else:
        # Render items with simple formatting
        for idx, it in enumerate(items, start=1):
            story.append(Paragraph(f"<b>Incident #{idx}</b>", styles["Heading3"]))
            # if item contains 'incident_bucket' / 'detected_by' / 'root_causes' etc (your format)
            if isinstance(it, dict):
                # show bucket/time if present
                if it.get("incident_bucket"):
                    story.append(Paragraph(f"<b>Bucket:</b> {it.get('incident_bucket')}", styles["Normal"]))
                # detected_by
                det = it.get("detected_by") or it.get("detector") or it.get("payload", {}).get("detector")
                if det:
                    if isinstance(det, (list, tuple)):
                        det_str = ", ".join(map(str, det))
                    else:
                        det_str = str(det)
                    story.append(Paragraph(f"<b>Detected by:</b> {det_str}", styles["Normal"]))
                # root_causes
                rcs = it.get("root_causes") or it.get("payload", {}).get("root_causes")
                if rcs and isinstance(rcs, list):
                    for rc in rcs:
                        rc_name = rc.get("root_cause", rc.get("root_cause", "root_cause"))
                        desc = rc.get("description", "")
                        evidence = rc.get("evidence", {})
                        story.append(Paragraph(f"<b>Root cause:</b> {rc_name}", styles["Normal"]))
                        story.append(Paragraph(str(desc), styles["Code"]))
                        if evidence:
                            story.append(Paragraph(f"<b>Evidence:</b> {json.dumps(evidence, default=str)}", styles["Code"]))
                # payload fallback
                payload = it.get("payload")
                if payload and not rcs:
                    story.append(Paragraph(f"{json.dumps(payload, default=str)}", styles["Code"]))
            else:
                story.append(Paragraph(str(it), styles["Normal"]))
            story.append(Spacer(1, 8))

    # Footer / metadata
    story.append(Spacer(1, 12))
    story.append(Paragraph("End of report", styles["Normal"]))

    # Create parent directory if needed
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Build PDF
    doc.build(story)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output PDF path")
    parser.add_argument("--name", required=False, help="Optional report name")
    parser.add_argument("--api", required=False, help="Optional RCA API URL to fetch data (e.g. http://127.0.0.1:8000/rca)")
    args = parser.parse_args()

    out_path = Path(args.out).resolve()
    try:
        build_pdf(out_path, name=args.name, api_url=args.api)
        print(f"Report written to {out_path}")
        sys.exit(0)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"ERROR: {str(e)}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
