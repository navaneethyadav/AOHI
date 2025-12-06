# generate_report_pro.py
"""
Standalone report generator for AOHI.
Run: python generate_report_pro.py --name "Navaneeth Kaku"
"""

import os
import json
import argparse
import traceback

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

def try_import_run_rca():
    try:
        from rca_engine.engine import run_rca
        return run_rca
    except Exception as e:
        return None

def load_rca_data():
    # 1) try run_rca if available
    run_rca = try_import_run_rca()
    if run_rca:
        try:
            tx = os.path.join(os.path.dirname(__file__), "data", "transactions.csv")
            sys_metrics = os.path.join(os.path.dirname(__file__), "data", "system_metrics.csv")
            crm = os.path.join(os.path.dirname(__file__), "data", "crm_events.csv")
            r = run_rca(tx, sys_metrics, crm)
            if isinstance(r, dict):
                return r
        except Exception:
            print("run_rca failed:", traceback.format_exc())

    # 2) fallback: try common files
    for fname in ("rca.json", "incidents_full.json", "incidents.json"):
        p = os.path.join(DATA_DIR, fname)
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf8") as fh:
                    return json.load(fh)
            except Exception:
                pass

    # 3) last resort: build minimal empty structure
    return {"incidents": []}

# PDF generation (reportlab)
def generate_logo(path, title="AOHI"):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        print("Pillow not installed, skipping logo:", e)
        return False
    img = Image.new("RGBA", (400, 80), (30,40,80,255))
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("arial.ttf", 36)
    except Exception:
        font = ImageFont.load_default()
    w,h = draw.textsize(title, font=font)
    draw.text(((400-w)/2, (80-h)/2), title, fill=(255,255,255,255), font=font)
    img.save(path)
    return True

def build_pdf(output_path, incidents_obj, logo_path=None, author_name=None):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import inch
    except Exception as e:
        raise RuntimeError("reportlab is required: pip install reportlab") from e

    c = canvas.Canvas(output_path, pagesize=letter)
    width, height = letter
    # header
    if logo_path and os.path.exists(logo_path):
        try:
            c.drawImage(logo_path, inch*0.5, height-inch*1.25, width=inch*1.5, preserveAspectRatio=True, mask='auto')
        except Exception:
            pass
    c.setFont("Helvetica-Bold", 16)
    c.drawString(inch*2.1, height-inch*0.75, "AOHI — Adaptive Operational Health Intelligence")
    c.setFont("Helvetica", 9)
    import datetime
    c.drawString(inch*0.5, height-inch*1.5, f"Generated: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if author_name:
        c.drawString(inch*0.5, height-inch*1.65, f"Prepared by: {author_name}")

    incidents = incidents_obj.get("incidents", []) if isinstance(incidents_obj, dict) else incidents_obj
    total_incidents = len(incidents)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(inch*0.5, height-inch*2.0, "Summary")
    c.setFont("Helvetica", 10)
    y = height - inch*2.2
    c.drawString(inch*0.5, y, f"Total incidents detected: {total_incidents}")
    y -= inch*0.25
    c.setFont("Helvetica", 9)
    for i, inc in enumerate(incidents):
        if y < inch*1.5:
            c.showPage()
            y = height - inch*0.5
            c.setFont("Helvetica", 9)
        bucket = inc.get("incident_bucket", "(unknown)")
        detected = ", ".join(inc.get("detected_by", [])) if inc.get("detected_by") else ""
        c.setFont("Helvetica-Bold", 10)
        c.drawString(inch*0.5, y, f"{i+1}. Incident: {bucket}")
        y -= inch*0.15
        c.setFont("Helvetica", 9)
        c.drawString(inch*0.6, y, f"Detected by: {detected}")
        y -= inch*0.16
        for rc in inc.get("root_causes", []):
            rc_name = rc.get("root_cause", "")
            desc = rc.get("description", "")
            c.drawString(inch*0.6, y, f"- {rc_name}: {desc}")
            y -= inch*0.14
            evidence = rc.get("evidence")
            if evidence:
                ev_text = json.dumps(evidence, ensure_ascii=False)
                c.drawString(inch*0.7, y, f"  Evidence: {ev_text}")
                y -= inch*0.14
        for p in inc.get("playbooks", []):
            owner = p.get("owner", "")
            priority = p.get("priority", "")
            steps = p.get("steps", "")
            c.drawString(inch*0.6, y, f"Suggested playbooks: {owner} | {priority} : {steps}")
            y -= inch*0.16
        y -= inch*0.06
    c.showPage()
    c.save()
    return output_path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="Navaneeth Kaku")
    parser.add_argument("--no-logo", action="store_true")
    args = parser.parse_args()

    incidents = load_rca_data()
    logo_path = os.path.join(DATA_DIR, "aohi_logo.png")
    if not args.no_logo:
        try:
            generate_logo(logo_path, title="AOHI")
        except Exception:
            pass
        if not os.path.exists(logo_path):
            logo_path = None
    else:
        logo_path = None

    out_pdf = os.path.join(DATA_DIR, "AOHI_Final_Report.pdf")
    try:
        build_pdf(out_pdf, incidents, logo_path=logo_path, author_name=args.name)
    except Exception as e:
        print("PDF generation failed:", e)
        print("Install reportlab (pip install reportlab) and Pillow for logo (pip install pillow)")
        return 2

    print("Report generated:", out_pdf)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
