"""
FastAPI backend for AOHI (Adaptive Operational Health Intelligence).

Exposes:
    - /health         : basic API health check
    - /incidents      : run all detectors and return incidents JSON
    - /rca            : simple rule-based RCA on top of incidents
    - /report_pro     : generate a PDF report using generate_report_pro.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from subprocess import run
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

# --- Detectors ---
from detectors import ewma, geo, latency, revenue, seasonal_zscore
from detectors import run_all_detectors, run_extra_detectors

# -------------------------------------------------------------------
# Basic app setup
# -------------------------------------------------------------------

app = FastAPI(
    title="AOHI API",
    version="0.1",
    description="AOHI (Adaptive Operational Health Intelligence) backend API",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # fine for local dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger("aohi.api")
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TRANSACTIONS_CSV = DATA_DIR / "transactions.csv"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def sanitize_for_json(obj: Any) -> Any:
    """
    Convert values coming from detectors into JSON-safe types.
    Keeps "inf" as string if needed.
    """
    import numpy as np
    import pandas as pd

    if obj is None:
        return None

    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            pass

    if isinstance(obj, (np.generic, pd.Timestamp)):
        try:
            return obj.item()
        except Exception:
            return str(obj)

    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(x) for x in obj]

    if isinstance(obj, dict):
        return {str(k): sanitize_for_json(v) for k, v in obj.items()}

    return obj


def run_all_incident_detectors() -> Dict[str, Any]:
    """
    Call all detector functions and bundle their results.
    """
    if not TRANSACTIONS_CSV.exists():
        raise FileNotFoundError(f"Transactions CSV not found at {TRANSACTIONS_CSV}")

    csv_path = str(TRANSACTIONS_CSV)
    incidents: List[Dict[str, Any]] = []

    def add_detector(name: str, func):
        try:
            raw_result = func(csv_path)
            incidents.append(
                {
                    "detector": name,
                    "result": sanitize_for_json(raw_result),
                }
            )
        except Exception as e:
            # Don't crash whole API if one detector fails
            logger.warning("Detector %s failed: %s", name, e)

    # Base detectors
    add_detector("detectors.ewma.detect_ewma_failed", ewma.detect_ewma_failed)
    add_detector("detectors.geo.detect_geo_failures", geo.detect_geo_failures)
    add_detector("detectors.latency.detect_latency_spike", latency.detect_latency_spike)
    add_detector("detectors.revenue.detect_revenue_drop", revenue.detect_revenue_drop)
    add_detector(
        "detectors.seasonal_zscore.detect_failed_tx_spike",
        seasonal_zscore.detect_failed_tx_spike,
    )

    # Meta-detectors
    add_detector(
        "detectors.run_all_detectors.detect_ewma_failed",
        run_all_detectors.detect_ewma_failed,
    )
    add_detector(
        "detectors.run_extra_detectors.detect_geo_failures",
        run_extra_detectors.detect_geo_failures,
    )

    return {"incidents": incidents}


def compute_simple_rca(incidents: Dict[str, Any]) -> Dict[str, Any]:
    """
    Very small rule-based RCA engine.
    """
    results: List[Dict[str, Any]] = []
    detectors_used: List[str] = []

    inc_list = incidents.get("incidents", [])

    for item in inc_list:
        name = item.get("detector")
        if name:
            detectors_used.append(name)

    # Rule: many geo failures in IN => regional failure in IN
    geo_items = [
        inc for inc in inc_list if inc.get("detector") == "detectors.geo.detect_geo_failures"
    ]
    total_obs = 0
    country = None

    if geo_items:
        for row in geo_items[0].get("result", []):
            if row.get("country") == "IN":
                country = "IN"
                total_obs += 1

    if country and total_obs > 0:
        results.append(
            {
                "root_cause": "Regional failures in IN",
                "confidence": 0.9,
                "evidence": {
                    "country": country,
                    "observations": total_obs,
                },
                "recommendation": "Investigate services in IN, check network, CDN, and regional gateways.",
            }
        )

    return {
        "results": {
            "results": results,
            "detectors_used": detectors_used,
        }
    }


# -------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "AOHI", "version": "0.1"}


@app.get("/incidents")
def get_incidents(force_run: bool = Query(False)) -> JSONResponse:
    try:
        payload = run_all_incident_detectors()
        return JSONResponse(payload)
    except Exception as e:
        logger.exception("Failed to compute incidents: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to compute incidents: {e}")


@app.get("/rca")
def get_rca() -> JSONResponse:
    try:
        incidents = run_all_incident_detectors()
        rca_payload = compute_simple_rca(incidents)
        return JSONResponse(rca_payload)
    except Exception as e:
        logger.exception("Failed to compute RCA: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to compute RCA: {e}")


@app.get("/report_pro")
def generate_report(timeout: int = 60, name: str = "AOHI User"):
    """
    Generate AOHI report by calling the local script generate_report_pro.py
    and then return the PDF file.
    """
    out_path = DATA_DIR / "AOHI_Final_Report.pdf"

    try:
        # 🔧 IMPORTANT: use the SAME Python as FastAPI (your venv),
        # not the global "python" that doesn't have pandas installed.
        run(
            [
                sys.executable,        # <--- main fix
                "-m",
                "api.generate_report_pro",
                "--out",
                str(out_path),
                "--name",
                name,
            ],
            timeout=timeout,
            check=True,
        )
    except Exception as e:
        logger.exception("Failed to generate report: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {e}")

    if not out_path.exists():
        raise HTTPException(status_code=500, detail="Report file was not created.")

    return FileResponse(
        str(out_path),
        media_type="application/pdf",
        filename="AOHI_Final_Report.pdf",
    )
