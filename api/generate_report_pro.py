import argparse
from datetime import datetime

import pandas as pd
import requests

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

API_BASE = "http://127.0.0.1:8000"


# -----------------------------
# API CALLS
# -----------------------------

def get_live_incidents():
    """Fetch fresh incidents from AOHI API with force_run=True."""
    resp = requests.get(
        f"{API_BASE}/incidents",
        params={"force_run": "true"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_live_rca():
    """Fetch RCA from AOHI API."""
    resp = requests.get(f"{API_BASE}/rca", timeout=30)
    resp.raise_for_status()
    return resp.json()


# -----------------------------
# FLATTEN INCIDENTS
# -----------------------------

def flatten_incidents(inc_json):
    """Convert incidents JSON into a pandas DataFrame."""
    rows = []

    for det in inc_json.get("incidents", []):
        det_name = det.get("detector", "")
        for r in det.get("result", []):
            rows.append(
                {
                    "timestamp": r.get("timestamp"),
                    "detector": det_name,
                    "failed": r.get("failed", ""),
                    "failed_count": r.get("failed_count", ""),
                    "zscore": str(r.get("zscore", "")),
                    "current_revenue": r.get("current_revenue", ""),
                    "baseline": r.get("baseline", ""),
                    "country": r.get("country", ""),
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df


# -----------------------------
# BUILD PDF
# -----------------------------

def build_report(out_file: str, name: str) -> None:
    """Generate AOHI PDF report using live API data."""
    try:
        inc_json = get_live_incidents()
    except Exception as e:
        inc_json = {}
        print(f"[WARN] Failed to fetch incidents: {e}")

    try:
        rca_json = get_live_rca()
    except Exception as e:
        rca_json = {}
        print(f"[WARN] Failed to fetch RCA: {e}")

    df = flatten_incidents(inc_json)

    styles = getSampleStyleSheet()
    story = []

    # Title + header
    story.append(Paragraph("AOHI - Adaptive Operational Health Intelligence", styles["Title"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(f"Prepared for: <b>{name}</b>", styles["Normal"]))
    story.append(Paragraph(f"Generated: <b>{datetime.now()}</b>", styles["Normal"]))
    story.append(Spacer(1, 15))

    # Section 1 — Overview
    story.append(Paragraph("Section 1 — Overview", styles["Heading2"]))
    story.append(Paragraph(f"• Total incident records: {len(df)}", styles["Normal"]))

    if not df.empty:
        detectors_str = ", ".join(sorted(df["detector"].dropna().unique()))
        story.append(Paragraph(f"• Detectors involved: {detectors_str}", styles["Normal"]))

        t_min = df["timestamp"].min()
        t_max = df["timestamp"].max()
        story.append(
            Paragraph(
                f"• Time range (incidents): {t_min} → {t_max}",
                styles["Normal"],
            )
        )
    else:
        story.append(Paragraph("• Detectors involved: N/A", styles["Normal"]))
        story.append(Paragraph("• Time range (incidents): N/A", styles["Normal"]))

    story.append(Spacer(1, 15))

    # Section 2 — Incident Summary
    story.append(Paragraph("Section 2 — Incident Summary", styles["Heading2"]))

    if df.empty:
        story.append(Paragraph("No incidents available.", styles["Normal"]))
    else:
        table_data = [
            [
                "Timestamp",
                "Detector",
                "Country",
                "Failed/Failed_Count",
                "Z-Score",
                "Revenue",
                "Baseline",
            ]
        ]

        for _, r in df.iterrows():
            table_data.append(
                [
                    str(r["timestamp"]),
                    r.get("detector", ""),
                    r.get("country", ""),
                    f"{r.get('failed', '')}/{r.get('failed_count', '')}",
                    r.get("zscore", ""),
                    str(r.get("current_revenue", "")),
                    str(r.get("baseline", "")),
                ]
            )

        table = Table(table_data, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(table)

    story.append(Spacer(1, 20))

    # Section 3 — RCA
    story.append(Paragraph("Section 3 — Root Cause Analysis (RCA)", styles["Heading2"]))

    rca_list = rca_json.get("results", {}).get("results", []) if rca_json else []

    if not rca_list:
        story.append(Paragraph("No RCA results available.", styles["Normal"]))
    else:
        for idx, r in enumerate(rca_list, start=1):
            story.append(Paragraph(f"Root Cause #{idx}: {r.get('root_cause', '')}", styles["Normal"]))
            story.append(Paragraph(f"Confidence: {r.get('confidence', '')}", styles["Normal"]))
            story.append(Paragraph(f"Recommendation: {r.get('recommendation', '')}", styles["Normal"]))

            evidence = r.get("evidence", {})
            if evidence:
                story.append(Paragraph(f"Evidence: {evidence}", styles["Normal"]))
            story.append(Spacer(1, 10))

    story.append(Spacer(1, 20))
    story.append(Paragraph("End of report.", styles["Normal"]))

    doc = SimpleDocTemplate(out_file, pagesize=A4)
    doc.build(story)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    build_report(args.out, args.name)
