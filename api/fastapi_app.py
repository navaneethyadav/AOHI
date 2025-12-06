# api/fastapi_app.py
"""
AOHI FastAPI app (dynamic detector loader + report endpoint)
- Discovers detectors in ./detectors and calls detect_* functions
- Endpoints: /, /health, /run_detectors, /incidents, /rca, /report_pro
"""

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
import pandas as pd
import importlib
import inspect
import os
import pkgutil
import json
import subprocess
from typing import List, Dict, Any, Optional
import time

APP_DIR = Path(__file__).parent.resolve()
ROOT = APP_DIR.parent.resolve()
DATA_DIR = ROOT / "data"
TX_FILE = DATA_DIR / "transactions.csv"
REPORT_SCRIPT = APP_DIR / "generate_report_pro.py"
OUT_PDF = DATA_DIR / "AOHI_Final_Report.pdf"
DETECTORS_DIR = ROOT / "detectors"

# Best-effort import of rca_engine helpers
try:
    import rca_engine.engine as rca_engine
except Exception:
    rca_engine = None

app = FastAPI(title="AOHI API (dynamic detectors)", version="0.1")


def load_transactions() -> pd.DataFrame:
    if not TX_FILE.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(TX_FILE, parse_dates=["timestamp"])
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

                if isinstance(out, str):
                    try:
                        parsed = json.loads(out)
                        out = parsed
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


@app.get("/run_detectors")
def run_detectors():
    df = load_transactions()
    outputs = discover_and_run_detectors(df)
    return JSONResponse({"detector_outputs": outputs})


@app.get("/incidents")
def incidents(force_run: bool = Query(False, description="Force run detectors before returning incidents")):
    df = load_transactions()
    outputs = discover_and_run_detectors(df)

    incidents_list = []
    for out in outputs:
        payload = out.get("output", out)
        if isinstance(payload, dict) and "incidents" in payload:
            incidents_list.extend(payload["incidents"])
            continue
        incidents_list.append({"detector": out.get("detector"), "payload": payload})
    return JSONResponse({"incidents": incidents_list})


@app.get("/rca")
def rca_view(force_run: bool = Query(False, description="Force run detectors/rca")):
    df = load_transactions()
    outputs = discover_and_run_detectors(df)

    if rca_engine:
        try:
            rca_results = rca_engine.run_rca(outputs, df=df)
            return JSONResponse({"rca": rca_results})
        except Exception:
            pass

    return JSONResponse({"rca": outputs})


@app.get("/report_pro")
def report_pro(
    force: bool = Query(False, description="Force regeneration"),
    timeout: int = Query(30, description="Timeout seconds for generation"),
    name: Optional[str] = Query(None, description="Optional report name/person")
):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not Path(REPORT_SCRIPT).exists():
        return JSONResponse({"detail": f"Report generator not found: {REPORT_SCRIPT}"}, status_code=404)

    if OUT_PDF.exists() and not force:
        return FileResponse(path=str(OUT_PDF), media_type="application/pdf", filename=OUT_PDF.name)

    cmd = [os.sys.executable, str(REPORT_SCRIPT), "--out", str(OUT_PDF)]
    if name:
        cmd += ["--name", name]

    if force and OUT_PDF.exists():
        try:
            OUT_PDF.unlink()
        except Exception:
            pass

    try:
        start = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        elapsed = time.time() - start
    except subprocess.TimeoutExpired as e:
        return JSONResponse({"detail": "Report generator timed out", "timeout": timeout, "stdout": e.stdout, "stderr": str(e)}, status_code=504)

    result = {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr, "elapsed_seconds": round(elapsed, 2)}

    if proc.returncode != 0:
        return JSONResponse({"detail": "Report generator failed", **result}, status_code=500)

    if not OUT_PDF.exists():
        return JSONResponse({"detail": "Report generator finished but output PDF not found", **result}, status_code=500)

    return FileResponse(path=str(OUT_PDF), media_type="application/pdf", filename=OUT_PDF.name)
