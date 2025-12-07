import argparse
import datetime as dt
import json
from typing import Any, Dict, List

import pandas as pd
import requests

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)


API_BASE = "http://127.0.0.1:8000"


# ---------------------------------------------------
# Helpers to call your own API
# ---------------------------------------------------
def fetch_incidents() -> Dict[str, Any]:
    """Call /incidents from the local API."""
    resp = requests.get(f"{API_BASE}/incidents", params={"force_run": "true"}, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_rca() -> Dict[str, Any]:
    """Call /rca from the local API."""
    resp = requests.get(f"{API_BASE}/rca", timeout=60)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------
# Flatten JSON into pandas structures
# ---------------------------------------------------
def flatten_incidents(incidents_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Turn incidents JSON into a flat DataFrame.

    Columns:
      - timestamp
      - detector
      - country
      - failed
      - failed_count
      - zscore
      - current_revenue
      - baseline
    """
    rows: List[Dict[str, Any]] = []

    for det in incidents_json.get("incidents", []):
        det_name = det.get("detector", "")
        for item in det.get("result", []):
            rows.append(
                {
                    "timestamp": item.get("timestamp"),
                    "detector": det_name,
                    "country": item.get("country"),
                    "failed": item.get("failed"),
                    "failed_count": item.get("failed_count"),
                    "zscore": item.get("zscore"),
                    "current_revenue": item.get("current_revenue"),
                    "baseline": item.get("baseline"),
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df


def flatten_rca(rca_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten RCA JSON into a list of dicts:
      - root_cause
      - confidence
      - recommendation
      - evidence
    """
    results = rca_json.get("results", {}).get("results", [])
    rows: List[Dict[str, Any]] = []

    for r in results:
        rows.append(
            {
                "root_cause": r.get("root_cause"),
                "confidence": r.get("confidence"),
                "recommendation": r.get("recommendation"),
                "evidence": r.get("evidence", {}),
            }
        )

    return rows


# ---------------------------------------------------
# PDF building
# ---------------------------------------------------
def build_pdf(
    out_path: str,
    name: str,
    incidents_json: Dict[str, Any],
    rca_json: Dict[str, Any],
) -> None:
    # Document + styles
    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        rightMargin=36,
        leftMargin=36,
        topMargin=40,
        bottomMargin=36,
    )

    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="TitleBig",
            parent=styles["Title"],
            fontSize=20,
            leading=24,
            spaceAfter=12,
        )
    )
    styles.add(
        ParagraphStyle(
            name="SectionHeader",
            parent=styles["Heading2"],
            fontSize=14,
            leading=18,
            spaceBefore=12,
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BodySmall",
            parent=styles["Normal"],
            fontSize=9,
            leading=12,
        )
    )

    elements: List[Any] = []

    now = dt.datetime.now()

    # -------------------------
    # Cover / Header
    # -------------------------
    title = Paragraph("AOHI - Adaptive Operational Health Intelligence", styles["TitleBig"])
    prepared_for = Paragraph(f"Prepared for: <b>{name}</b>", styles["BodySmall"])
    generated = Paragraph(f"Generated: {now}", styles["BodySmall"])

    elements.extend([title, prepared_for, generated, Spacer(1, 12)])

    # -------------------------
    # Section 1 – Overview
    # -------------------------
    df_inc = flatten_incidents(incidents_json)
    total_records = len(df_inc)

    if total_records > 0:
        detectors_involved = ", ".join(
            sorted(df_inc["detector"].dropna().unique())
        )
        t_min = df_inc["timestamp"].min()
        t_max = df_inc["timestamp"].max()
        time_range_text = f"{t_min} → {t_max}"
    else:
        detectors_involved = "N/A"
        time_range_text = "N/A"

    elements.append(Paragraph("Section 1 — Overview", styles["SectionHeader"]))
    elements.append(
        Paragraph(f"• Total incident records: <b>{total_records}</b>", styles["BodySmall"])
    )
    elements.append(
        Paragraph(f"• Detectors involved: {detectors_involved}", styles["BodySmall"])
    )
    elements.append(
        Paragraph(f"• Time range (incidents): {time_range_text}", styles["BodySmall"])
    )
    elements.append(Spacer(1, 16))

    # -------------------------
    # Section 2 – Incident Summary
    # -------------------------
    elements.append(Paragraph("Section 2 — Incident Summary", styles["SectionHeader"]))

    if total_records == 0:
        elements.append(Paragraph("No incidents available.", styles["BodySmall"]))
    else:
        # Header row
        table_header = [
            "Timestamp",
            "Detector",
            "Country",
            "Failed / Count",
            "Z-Score",
            "Revenue",
            "Baseline",
        ]
        table_data: List[List[str]] = [table_header]

        df_sorted = df_inc.sort_values("timestamp")

        for _, row in df_sorted.iterrows():
            ts = (
                row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                if pd.notna(row["timestamp"])
                else ""
            )

            failed_parts: List[str] = []
            if pd.notna(row.get("failed")):
                failed_parts.append(str(row["failed"]))
            if pd.notna(row.get("failed_count")):
                failed_parts.append(str(int(row["failed_count"])))
            failed_str = " / ".join(failed_parts) if failed_parts else ""

            z = "" if pd.isna(row.get("zscore")) else str(row["zscore"])
            rev = (
                "" if pd.isna(row.get("current_revenue")) else f"{row['current_revenue']:.2f}"
            )
            base = "" if pd.isna(row.get("baseline")) else f"{row['baseline']:.2f}"

            table_data.append(
                [
                    ts,
                    row.get("detector", "") or "",
                    row.get("country", "") or "",
                    failed_str,
                    z,
                    rev,
                    base,
                ]
            )

        table = Table(table_data, repeatRows=1)

        table.setStyle(
            TableStyle(
                [
                    # Header style
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                    # Body style
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ALIGN", (0, 1), (0, -1), "LEFT"),
                    ("ALIGN", (1, 1), (-1, -1), "LEFT"),
                    # Grid
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ]
            )
        )

        elements.append(table)

    elements.append(Spacer(1, 18))

    # -------------------------
    # Section 3 – RCA
    # -------------------------
    elements.append(
        Paragraph("Section 3 — Root Cause Analysis (RCA)", styles["SectionHeader"])
    )

    rca_rows = flatten_rca(rca_json)
    if not rca_rows:
        elements.append(Paragraph("No RCA results available.", styles["BodySmall"]))
    else:
        for idx, rc in enumerate(rca_rows, start=1):
            elements.append(
                Paragraph(f"Root Cause #{idx}: <b>{rc['root_cause']}</b>", styles["BodySmall"])
            )
            elements.append(
                Paragraph(f"Confidence: {rc['confidence']}", styles["BodySmall"])
            )
            elements.append(
                Paragraph(f"Recommendation: {rc['recommendation']}", styles["BodySmall"])
            )
            ev_str = json.dumps(rc.get("evidence", {}), indent=2)
            elements.append(
                Paragraph(
                    f"Evidence: <font face='Courier'>{ev_str}</font>",
                    styles["BodySmall"],
                )
            )
            elements.append(Spacer(1, 8))

    elements.append(Spacer(1, 12))
    elements.append(Paragraph("End of report.", styles["BodySmall"]))

    # Build the PDF
    doc.build(elements)


# ---------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output PDF path")
    parser.add_argument("--name", default="AOHI User", help="Name for the report")
    args = parser.parse_args()

    # Fetch data from API
    incidents_json = fetch_incidents()
    rca_json = fetch_rca()

    build_pdf(
        out_path=args.out,
        name=args.name,
        incidents_json=incidents_json,
        rca_json=rca_json,
    )


if __name__ == "__main__":
    main()
