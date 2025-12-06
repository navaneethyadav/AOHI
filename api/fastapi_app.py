# api/fastapi_app.py
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List
import json
import math
import os
import importlib
import time

# Project root and data locations (adjust if your layout differs)
ROOT = Path(__file__).resolve().parents[1]  # project root
DATA_DIR = ROOT / "data"
REPORT_SCRIPT = ROOT / "api" / "generate_report_pro.py"
REPORT_OUTPUT_DEFAULT = DATA_DIR / "AOHI_FromAPI.pdf"

# Create data dir if missing
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="AOHI", version="0.1")

# allow the Vite dev server(s) to call the API (dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", "http://127.0.0.1:5173",
        "http://localhost:5174", "http://127.0.0.1:5174"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def sanitize_for_json(obj: Any) -> Any:
    """
    Convert values that JSON can't handle (NaN, inf, complex objects) into
    safe serializable values (strings or smaller structures).
    """
    try:
        if obj is None:
            return None
        if isinstance(obj, (str, bool, int)):
            return obj
        if isinstance(obj, float):
            if math.isfinite(obj):
                return obj
            return str(obj)  # "inf", "-inf", "nan"
        if isinstance(obj, dict):
            return {str(k): sanitize_for_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [sanitize_for_json(v) for v in obj]
        if isinstance(obj, Path):
            return str(obj)
        try:
            import datetime
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
        except Exception:
            pass
        try:
            json.dumps(obj)
            return obj
        except Exception:
            return str(obj)
    except Exception:
        return "<unserializable>"

@app.get("/")
def root():
    return {"detail": "AOHI API - available endpoints: /health, /incidents, /rca, /report_pro"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "AOHI", "version": "0.1"}

def discover_detectors() -> List[str]:
    dets = []
    detectors_dir = ROOT / "detectors"
    if not detectors_dir.exists():
        return dets
    for p in detectors_dir.glob("*.py"):
        if p.name.startswith("__"):
            continue
        module_name = f"detectors.{p.stem}"
        dets.append(module_name)
    dets.sort()
    return dets

def safe_call_detector(func, module_name: str) -> Dict[str, Any]:
    out = {"detector": module_name}
    try:
        try:
            res = func()
        except TypeError:
            # try module CSV_PATH if present
            try:
                mod = importlib.import_module(func.__module__)
                csv_path = getattr(mod, "CSV_PATH", None)
            except Exception:
                csv_path = None
            if not csv_path:
                default = DATA_DIR / "transactions.csv"
                csv_path = str(default) if default.exists() else None
            if csv_path:
                res = func(csv_path)
            else:
                # last resort: pass data dir path
                res = func(str(DATA_DIR))
        out["result"] = sanitize_for_json(res)
    except Exception as e:
        out["error"] = "runtime error: " + str(e)
        out["traceback"] = traceback.format_exc()
    return out

def run_detector_function(module_name: str) -> Dict[str, Any]:
    data = {"detector": module_name}
    try:
        mod = importlib.import_module(module_name)
        func = None
        chosen_name = None
        for attr in dir(mod):
            if attr.startswith("detect_") and callable(getattr(mod, attr)):
                func = getattr(mod, attr)
                chosen_name = attr
                break
        if func is None and hasattr(mod, "detect") and callable(getattr(mod, "detect")):
            func = getattr(mod, "detect")
            chosen_name = "detect"
        if func is None:
            data["error"] = "no detect function found"
            return data
        data["detector"] = f"{module_name}.{chosen_name}"
        return safe_call_detector(func, data["detector"])
    except Exception as e:
        data["error"] = "import/runtime error: " + str(e)
        data["traceback"] = traceback.format_exc()
    return data

@app.get("/run_detectors")
def run_detectors(force: bool = Query(False)):
    modules = discover_detectors()
    out = []
    for module in modules:
        info = run_detector_function(module)
        out.append(info)
    return {"detectors": sanitize_for_json(out)}

@app.get("/incidents")
def incidents(force_run: bool = Query(False)):
    try:
        d = run_detectors(force=force_run)
        return {"incidents": d.get("detectors", [])}
    except Exception:
        return JSONResponse(content={"detail": "Internal error running detectors", "traceback": traceback.format_exc()}, status_code=500)

@app.get("/rca")
def rca():
    try:
        try:
            import rca_engine.engine as engine
            if hasattr(engine, "analyze") and callable(engine.analyze):
                results = engine.analyze()
                return {"results": sanitize_for_json(results)}
            else:
                detectors_out = run_detectors()
                return {"detail": "RCA fallback results", "results": detectors_out.get("detectors", [])}
        except ModuleNotFoundError:
            detectors_out = run_detectors()
            return {"detail": "RCA fallback results", "results": detectors_out.get("detectors", [])}
    except Exception:
        return JSONResponse(content={"detail": "RCA failed", "traceback": traceback.format_exc()}, status_code=500)

@app.get("/report_pro")
def report_pro(force: bool = Query(False), timeout: int = Query(30), name: str = Query("AOHI"), api: str = Query("http://127.0.0.1:8000/rca")):
    # check script
    if not REPORT_SCRIPT.exists():
        return JSONResponse(content={"detail": "Report generator not found", "path": str(REPORT_SCRIPT)}, status_code=500)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_OUTPUT_DEFAULT

    # If force is requested, remove existing output file before running the generator.
    if force and out_path.exists():
        try:
            out_path.unlink()
            time.sleep(0.05)
        except Exception:
            pass

    # Build command (NOTE: do NOT pass --force to the script)
    cmd = [sys.executable, str(REPORT_SCRIPT), "--out", str(out_path), "--name", name, "--api", api]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(ROOT))
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        rc = proc.returncode

        data_files = []
        try:
            for p in sorted(DATA_DIR.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
                data_files.append({"name": p.name, "size": p.stat().st_size, "mtime": p.stat().st_mtime})
        except Exception:
            data_files = ["unable to list data dir", traceback.format_exc()]

        if out_path.exists():
            return FileResponse(path=str(out_path), media_type="application/pdf", filename=out_path.name)
        else:
            return JSONResponse(
                content=sanitize_for_json({
                    "detail": "Report written but PDF not found at expected path",
                    "expected_path": str(out_path),
                    "returncode": rc,
                    "stdout": stdout,
                    "stderr": stderr,
                    "data_dir_listing": data_files,
                    "cwd_used": str(ROOT),
                }),
                status_code=500,
            )
    except subprocess.TimeoutExpired as ex:
        return JSONResponse(content={"detail": "Report generator timed out", "timeout": timeout, "error": str(ex)}, status_code=500)
    except Exception:
        return JSONResponse(content=sanitize_for_json({"detail": "Report generator failed", "traceback": traceback.format_exc()}), status_code=500)