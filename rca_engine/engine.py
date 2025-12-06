# rca_engine/engine.py
"""
RCA engine (updated to include latency, revenue, geo detectors).
Runs detectors, buckets data, collects evidence, and maps playbooks.
"""

import json
import os
import pandas as pd

# detectors
from detectors.seasonal_zscore import detect_failed_tx_spike
from detectors.ewma import detect_ewma_failed
from detectors.latency import detect_latency_spike
from detectors.revenue import detect_revenue_drop
from detectors.geo import detect_geo_failures

RULES_PATH = os.path.join("rca_engine", "rules.json")
PLAYBOOK_PATH = os.path.join("playbooks", "playbooks.csv")

def load_rules(path=RULES_PATH):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_playbooks(path=PLAYBOOK_PATH):
    if not os.path.exists(path):
        return pd.DataFrame(columns=["root_cause","priority","owner","steps"])
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=["root_cause","priority","owner","steps"])

def bucket_df(df, ts_col="timestamp", freq="5T"):
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col])
    df["bucket"] = df[ts_col].dt.floor(freq)
    return df

def compute_failed_counts(transactions_df, freq="5T"):
    df = bucket_df(transactions_df, ts_col="timestamp", freq=freq)
    failed = df[df["status"] != "success"].groupby("bucket").size().rename("failed_count")
    all_buckets = df.groupby("bucket").size().rename("total_count")
    agg = pd.concat([all_buckets, failed], axis=1).fillna(0)
    agg["failed_count"] = agg["failed_count"].astype(int)
    return agg

def run_rca(transactions_path="data/transactions.csv",
            system_metrics_path="data/system_metrics.csv",
            crm_path="data/crm_events.csv",
            freq="5T"):
    # 1) Run core detectors (they return lists of dicts)
    try:
        z_anoms = detect_failed_tx_spike(transactions_path, freq=freq, window=6, z_thresh=1, min_failed=5)
    except Exception as e:
        print("Error running seasonal_zscore:", e)
        z_anoms = []

    try:
        ewma_anoms = detect_ewma_failed(transactions_path, freq=freq, span=6, k=2, min_failed=5)
    except Exception as e:
        print("Error running ewma:", e)
        ewma_anoms = []

    # 2) Run extra detectors (latency, revenue, geo)
    try:
        latency_anoms = detect_latency_spike(system_metrics_path, ts_col="timestamp", freq=freq, factor=1.5, prior_minutes=30, min_count=3)
    except Exception as e:
        print("Error running latency detector:", e)
        latency_anoms = []

    try:
        revenue_anoms = detect_revenue_drop(transactions_path, ts_col="timestamp", freq="1H", window=6, factor=0.7)
    except Exception as e:
        print("Error running revenue detector:", e)
        revenue_anoms = []

    try:
        geo_anoms = detect_geo_failures(transactions_path, ts_col="timestamp", country_col="country", freq=freq, threshold=5)
    except Exception as e:
        print("Error running geo detector:", e)
        geo_anoms = []

    # 3) Load data (transactions, sys, crm) and bucket
    if os.path.exists(transactions_path):
        tx_df = pd.read_csv(transactions_path, parse_dates=["timestamp"])
    else:
        tx_df = pd.DataFrame(columns=["timestamp","status"])

    if os.path.exists(system_metrics_path):
        sys_df = pd.read_csv(system_metrics_path, parse_dates=["timestamp"])
    else:
        sys_df = pd.DataFrame(columns=["timestamp","latency_ms"])

    if os.path.exists(crm_path):
        crm_df = pd.read_csv(crm_path, parse_dates=["timestamp"])
    else:
        crm_df = pd.DataFrame(columns=["timestamp","support_ticket"])

    sys_df = bucket_df(sys_df, ts_col="timestamp", freq=freq) if not sys_df.empty else sys_df
    crm_df = bucket_df(crm_df, ts_col="timestamp", freq=freq) if not crm_df.empty else crm_df

    failed_agg = compute_failed_counts(tx_df, freq=freq) if not tx_df.empty else pd.DataFrame(columns=["failed_count"])
    failed_series = failed_agg["failed_count"] if "failed_count" in failed_agg.columns else pd.Series(dtype=int)

    # 4) Collect timestamps from all detectors
    timestamps = set()
    def add_detector_ts(list_of_dicts):
        for item in list_of_dicts:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                t = pd.to_datetime(ts).floor(freq)
                timestamps.add(t)
            except Exception:
                continue

    add_detector_ts(z_anoms)
    add_detector_ts(ewma_anoms)
    add_detector_ts(latency_anoms)
    add_detector_ts(revenue_anoms)
    add_detector_ts(geo_anoms)

    if not timestamps:
        print("No incidents detected; nothing to RCA.")
        return []

    rules = load_rules()
    playbooks = load_playbooks()

    results = []
    for bucket in sorted(timestamps):
        entry = {"incident_bucket": str(bucket), "detected_by": [], "root_causes": [], "playbooks": []}

        # who detected it?
        for a in z_anoms:
            try:
                if pd.to_datetime(a["timestamp"]).floor(freq) == bucket:
                    entry["detected_by"].append("seasonal_zscore")
            except Exception:
                continue
        for b in ewma_anoms:
            try:
                if pd.to_datetime(b["timestamp"]).floor(freq) == bucket:
                    entry["detected_by"].append("ewma")
            except Exception:
                continue
        for la in latency_anoms:
            try:
                if pd.to_datetime(la["timestamp"]).floor(freq) == bucket:
                    entry["detected_by"].append("latency")
            except Exception:
                continue
        for ra in revenue_anoms:
            try:
                if pd.to_datetime(ra["timestamp"]).floor(freq) == bucket:
                    entry["detected_by"].append("revenue")
            except Exception:
                continue
        for ga in geo_anoms:
            try:
                if pd.to_datetime(ga["timestamp"]).floor(freq) == bucket:
                    entry["detected_by"].append("geo")
            except Exception:
                continue

        # existing failed_tx_spike rule (from failed counts)
        failed_trig = False
        failed_info = None
        if bucket in failed_series.index:
            fc = int(failed_series.loc[bucket])
        else:
            fc = 0
        thr = rules.get("failed_tx_spike", {}).get("threshold", 10)
        if fc >= thr:
            failed_trig = True
            failed_info = {"failed_count": fc, "threshold": thr}
            entry["root_causes"].append({
                "root_cause": "failed_tx_spike",
                "description": rules.get("failed_tx_spike", {}).get("description", "Large spike in failed transactions"),
                "evidence": failed_info
            })

        # latency evidence (attach if latency_anoms includes this bucket)
        for la in latency_anoms:
            try:
                if pd.to_datetime(la["timestamp"]).floor(freq) == bucket:
                    entry["root_causes"].append({
                        "root_cause": "payment_gateway_latency",
                        "description": rules.get("payment_gateway_latency", {}).get("description", "Latency spike"),
                        "evidence": {"latency_median": la.get("latency_median"), "baseline": la.get("baseline")}
                    })
            except Exception:
                continue

        # revenue evidence
        for ra in revenue_anoms:
            try:
                if pd.to_datetime(ra["timestamp"]).floor(freq) == bucket:
                    entry["root_causes"].append({
                        "root_cause": "revenue_drop",
                        "description": "Revenue drop detected (hourly)",
                        "evidence": {"current_revenue": ra.get("current_revenue"), "baseline": ra.get("baseline")}
                    })
            except Exception:
                continue

        # geo evidence
        for ga in geo_anoms:
            try:
                if pd.to_datetime(ga["timestamp"]).floor(freq) == bucket:
                    entry["root_causes"].append({
                        "root_cause": "geo_failure",
                        "description": f"Geo-specific failures in {ga.get('country')}",
                        "evidence": {"country": ga.get("country"), "failed_count": ga.get("failed_count")}
                    })
            except Exception:
                continue

        # attach playbooks for each root cause
        for rc in entry["root_causes"]:
            try:
                dfpb = playbooks[playbooks["root_cause"] == rc["root_cause"]] if not playbooks.empty else None
                if dfpb is not None and not dfpb.empty:
                    entry["playbooks"].append(dfpb.iloc[0].to_dict())
            except Exception:
                continue

        results.append(entry)

    # print nicely
    print("\n== RCA RESULTS ==")
    for r in results:
        print("\nIncident bucket:", r["incident_bucket"])
        print(" Detected by:", ", ".join(r["detected_by"]) if r["detected_by"] else "none")
        if not r["root_causes"]:
            print(" Root cause: NOT FOUND by current rules")
        else:
            for rc in r["root_causes"]:
                print(" Root cause:", rc["root_cause"], "-", rc.get("description"))
                print("  Evidence:", rc.get("evidence"))
            if r["playbooks"]:
                print(" Suggested playbooks:")
                for pb in r["playbooks"]:
                    print("  -", pb.get("priority",""), pb.get("owner",""), ":", pb.get("steps",""))

    return results

if __name__ == "__main__":
    run_rca()
