# rca_engine/engine.py
"""
Minimal RCA engine for AOHI.

This simple implementation:
- calls the detectors via the existing API helper (we import detectors directly),
  or it can be called with precomputed detector outputs.
- applies a tiny rule set:
    - if geo detector reports many failures in same country => RCA: "regional outage"
    - if revenue drop & failed transactions spike at similar time => RCA: "payment gateway / revenue issue"
- returns a list of RCA items with explanation and suggested playbook actions.
"""

from typing import Any, Dict, List
import importlib
import traceback
import math

def _safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        if math.isfinite(f):
            return f
        return None
    except Exception:
        return None

def analyze(detectors_output: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    If detectors_output is None, import detectors.run_all or call them directly.
    Expected detectors_output is a list of dicts like:
    [{"detector": "detectors.ewma.detect_ewma_failed", "result": [...]}, ...]
    Returns a dict with structured RCA results.
    """
    try:
        if detectors_output is None:
            # try to import an existing module that returns detectors output
            try:
                import api.fastapi_app as fa
                detectors_output = fa.run_detectors(force=False).get("detectors", [])
            except Exception:
                # fallback: try to load detectors modules individually
                detectors_output = []
                import pkgutil, detectors
                for finder, name, ispkg in pkgutil.iter_modules(detectors.__path__):
                    mod = importlib.import_module(f"detectors.{name}")
                    # pick first detect function
                    func = None
                    for attr in dir(mod):
                        if attr.startswith("detect_") and callable(getattr(mod, attr)):
                            func = getattr(mod, attr)
                            break
                    if func:
                        try:
                            res = func()
                        except TypeError:
                            # try with default csv path attr
                            csv_path = getattr(mod, "CSV_PATH", None)
                            if csv_path:
                                res = func(csv_path)
                            else:
                                res = []
                        detectors_output.append({"detector": f"detectors.{name}", "result": res})

        # now analyze detectors_output
        rc_results = []
        # Build quick indices
        geo_counts = {}
        revenue_drops = []
        failed_spikes = []
        for entry in detectors_output:
            det = entry.get("detector", "")
            res = entry.get("result", []) or []
            if "geo" in det:
                for r in res:
                    country = r.get("country") or r.get("country_code")
                    if country:
                        geo_counts[country] = geo_counts.get(country, 0) + 1
            if "revenue" in det:
                for r in res:
                    revenue_drops.append(r)
            if "ewma" in det or "failed" in det or "seasonal_zscore" in det:
                for r in res:
                    # store timestamp & failed count if present
                    if r.get("failed") is not None or r.get("failed_count") is not None:
                        failed_spikes.append(r)

        # Rule 1: regional outage if geo_counts has a country with many entries
        for country, cnt in geo_counts.items():
            if cnt >= 3:
                rc_results.append({
                    "root_cause": f"Regional failures in {country}",
                    "confidence": min(0.9, 0.3 + cnt * 0.1),
                    "evidence": {"country": country, "observations": cnt},
                    "recommendation": f"Investigate services in {country}, check network, CDN, and regional gateways.",
                })

        # Rule 2: revenue drop together with failed tx spike -> payment issue
        if revenue_drops and failed_spikes:
            # crude time match: check any revenue drop timestamp close to failed spike timestamp
            for r in revenue_drops:
                rts = r.get("timestamp")
                for f in failed_spikes:
                    fts = f.get("timestamp")
                    if rts and fts and str(rts)[:13] == str(fts)[:13]:  # same hour naive
                        rc_results.append({
                            "root_cause": "Revenue drop correlated with failed transactions",
                            "confidence": 0.85,
                            "evidence": {"revenue": r, "failed": f},
                            "recommendation": "Check payment gateway, merchant keys, and recent deploys affecting payments.",
                        })

        # If no rc_results found, provide a fallback summary
        if not rc_results:
            rc_results.append({
                "root_cause": "No clear RCA found",
                "confidence": 0.25,
                "evidence": {"summary": "Detectors produced findings but no rule matched."},
                "recommendation": "Inspect detector outputs manually or expand RCA rules.json with correlation rules.",
            })

        return {"results": rc_results, "detectors_used": [d.get("detector") for d in detectors_output]}
    except Exception:
        return {"detail": "RCA engine error", "traceback": traceback.format_exc()}