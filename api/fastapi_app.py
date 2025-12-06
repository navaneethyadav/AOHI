# api/fastapi_app.py
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse
from pathlib import Path
import pkgutil
import importlib
import inspect
import json
import subprocess
import sys
from typing import List, Dict, Any
import pandas as pd

APP_DIR = Path(__file__).parent.resolve()
ROOT = APP_DIR.parent.resolve()
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TX_FILE = DATA_DIR / "transactions.csv"
OUT_PDF = DATA_DIR / "AOHI_Final_Report.pdf"
REPORT_SCRIPT = APP_DIR / "generate_report_pro.py"
DETECTORS_DIR = ROOT / "detectors"

app = FastAPI(title="AOHI API (dynamic detectors)", version="0.1")


def load_transactions() -> pd.DataFrame:
    if not TX_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(TX_FILE, parse_dates=["timestamp"])
    except Exception:
        df = pd.read_csv(TX_FILE)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df


def discover_and_run_detectors(df: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    if not DETECTORS_DIR.exists():
        return results

    package_name = "detectors"
    for finder, modname, ispkg in pkgutil.iter_modules([str(DETECTORS_DIR)]):
        fullname = f"{package_name}.{modname}"
        try:
            mod = importlib.import_module(fullname)
        except Exception as e:
            results.append({"detector": fullname, "error": f"import error: {str(e)}"})
            continue

        for name, func in inspect.getmembers(mod, inspect.isfunction):
            if name.startswith("detect_"):
                try:
                    sig = inspect.signature(func)
                    if len(sig.parameters) == 0:
                        out = func()
                    else:
                        out = func(df)
                except Exception as e:
                    results.append({"detector": f"{fullname}.{name}", "error": f"runtime error: {str(e)}"})
                    continue

                if out is None:
                    continue
                # Try to parse json string results
                if isinstance(out, str):
                    try:
                        out = json.loads(out)
                    except Exception:
                        pass
                results.append({"detector": f"{fullname}.{name}", "output": out})
    return results


@app.get("/")
def root():
    return {"message": "AOHI API - see /docs for interactive API documentation."}


@app.get("/health")
def health():
    return {"status": "ok", "service": "AOHI", "version": "0.1"}


@app.get("/run_detectors", summary="Run Detectors")
def run_detectors():
    df = load_transactions()
    outputs = discover_and_run_detectors(df)
    return {"detectors": outputs}


@app.get("/incidents", summary="Incidents View")
def incidents_view(force_run: bool = False):
    # This simple implementation runs detectors on-demand each call (not caching)
    df = load_transactions()
    outputs = discover_and_run_detectors(df)
    # try to collate incidents output if detectors return structured outputs
    collated = []
    for o in outputs:
        if "output" in o:
            collated.append(o["output"])
    return {"incidents": collated}


@app.get("/rca", summary="Rca View")
def rca_view(force_run: bool = False):
    # If rca_engine available you can call it; for now return incidents collated
    df = load_transactions()
    outputs = discover_and_run_detectors(df)
    collated = []
    for o in outputs:
        if "output" in o:
            collated.append(o["output"])
    return {"incidents": collated}


@app.get("/report_pro", summary="Report Pro")
def report_pro(force: bool = Query(False), timeout: int = Query(30), name: str = Query(None)):
    """
    Generate professional PDF and return it.
    - force: if True, always regenerate
    - timeout: seconds to wait for script
    - name: optional name to include in report
    """
    if not REPORT_SCRIPT.exists():
        return JSONResponse({"detail": f"Report generator not found: {REPORT_SCRIPT}"}, status_code=404)

    # If output exists and not force, return it
    if OUT_PDF.exists() and not force:
        return FileResponse(str(OUT_PDF), media_type="application/pdf", filename=OUT_PDF.name)

    # Build command
    cmd = [sys.executable, str(REPORT_SCRIPT), "--out", str(OUT_PDF)]
    if name:
        cmd += ["--name", str(name)]
    # Pass api endpoint for fetching rca
    cmd += ["--api", "http://127.0.0.1:8000/rca"]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return JSONResponse({"detail": "Report generator timed out"}, status_code=504)

    if proc.returncode != 0:
        # return stderr and stdout for debugging
        return JSONResponse({"detail": "Report generator failed", "returncode": proc.returncode,
                             "stdout": proc.stdout, "stderr": proc.stderr}, status_code=500)

    if not OUT_PDF.exists():
        return JSONResponse({"detail": "Report generated but file not found", "path": str(OUT_PDF)}, status_code=500)

    return FileResponse(str(OUT_PDF), media_type="application/pdf", filename=OUT_PDF.name)
