# data_generator/produce_with_anomalies.py
"""
Generate demo data with injected anomalies:
 - transactions.csv (with revenue drop windows and geo failure bursts)
 - system_metrics.csv (with latency spike windows)
 - crm_events.csv, web_traffic.csv left as basic samples

Run:
python -m data_generator.produce_with_anomalies
"""
import random
import csv
from datetime import datetime, timedelta
import pandas as pd
import os
import math

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

def base_time():
    # choose a base start (recent)
    return datetime(2025, 12, 4, 16, 0, 0)

def generate_transactions(start, minutes=24*24, seed=42):
    random.seed(seed)
    rows = []
    tx_id = 100000
    for m in range(minutes):
        ts = start + timedelta(minutes=m)
        # variable number of transactions per minute
        n = random.choice([0,1,2,3,4])
        for i in range(n):
            tx_id += 1
            amount = round(random.uniform(10, 120), 2)
            country = random.choices(["IN","UK","DE","US"], weights=[0.6,0.15,0.15,0.1])[0]
            status = "success"
            rows.append({"timestamp": ts.isoformat(), "tx_id": tx_id, "amount": amount, "country": country, "product_id": random.randint(100,130), "status": status})
    return pd.DataFrame(rows)

def inject_geo_failures(tx_df, start_bucket, duration_minutes=60, country="IN", failed_per_bucket=20):
    # For each 5-min bucket in window, add failed transactions for the country
    # We'll add rows that have status 'failed'
    rows = []
    buckets = [start_bucket + timedelta(minutes=5*i) for i in range(duration_minutes//5)]
    tx_id = int(tx_df["tx_id"].max()) if not tx_df.empty else 200000
    for b in buckets:
        for _ in range(failed_per_bucket):
            tx_id += 1
            rows.append({
                "timestamp": b.isoformat(),
                "tx_id": tx_id,
                "amount": round(random.uniform(20,140),2),
                "country": country,
                "product_id": random.randint(100,130),
                "status":"failed"
            })
    if rows:
        return pd.concat([tx_df, pd.DataFrame(rows)], ignore_index=True)
    return tx_df

def inject_revenue_drop(tx_df, start_bucket, duration_hours=2, reduce_by=0.1):
    # For the given hourly buckets, reduce revenue by converting many success -> failed or reducing amount
    df = tx_df.copy()
    start = start_bucket
    end = start + timedelta(hours=duration_hours)
    mask = (pd.to_datetime(df["timestamp"]) >= start) & (pd.to_datetime(df["timestamp"]) < end)
    # randomly set some successes to failed to simulate checkout failures
    idxs = df[mask & (df["status"]=="success")].sample(frac=0.7, replace=False).index if mask.any() else []
    df.loc[idxs, "status"] = "failed"
    # also optionally reduce amounts of remaining successes
    df.loc[mask & (df["status"]=="success"), "amount"] = (df.loc[mask & (df["status"]=="success"), "amount"] * reduce_by).round(2)
    return df

def generate_system_metrics(start, minutes=24*24, seed=123):
    random.seed(seed)
    rows = []
    for m in range(minutes):
        ts = start + timedelta(minutes=m)
        latency = random.uniform(50, 120)  # baseline ms
        cpu = random.uniform(10, 60)
        rows.append({"timestamp": ts.isoformat(), "latency_ms": latency, "cpu": cpu})
    return pd.DataFrame(rows)

def inject_latency_spike(sys_df, spike_start, duration_minutes=30, multiplier=6):
    df = sys_df.copy()
    start = spike_start
    end = spike_start + timedelta(minutes=duration_minutes)
    mask = (pd.to_datetime(df["timestamp"]) >= start) & (pd.to_datetime(df["timestamp"]) < end)
    df.loc[mask, "latency_ms"] = df.loc[mask, "latency_ms"] * multiplier
    return df

def save_csv(df, path, index=False):
    df.to_csv(path, index=index)

def main():
    start = base_time()
    print("Generating base transactions and system metrics...")
    tx = generate_transactions(start, minutes=24*24, seed=42)
    sysm = generate_system_metrics(start, minutes=24*24, seed=123)

    # Choose anomaly windows (you can tweak these times)
    geo_start = start + timedelta(hours=11, minutes=0)    # will produce buckets near 2025-12-05 03:xx
    revenue_start = start + timedelta(hours=10)           # an hour before geo failures
    latency_start = start + timedelta(hours=11, minutes=50)

    print("Injecting geo failures at:", geo_start)
    tx = inject_geo_failures(tx, geo_start, duration_minutes=60, country="IN", failed_per_bucket=20)

    print("Injecting revenue drop at:", revenue_start)
    tx = inject_revenue_drop(tx, revenue_start, duration_hours=2, reduce_by=0.1)

    print("Injecting latency spike at:", latency_start)
    sysm = inject_latency_spike(sysm, latency_start, duration_minutes=30, multiplier=8)

    # basic crm and web traffic (small samples)
    crm = pd.DataFrame([{"timestamp": (start + timedelta(minutes=i*30)).isoformat(), "support_ticket": random.choice([0,0,1])} for i in range(48)])
    web = pd.DataFrame([{"timestamp": (start + timedelta(minutes=i*10)).isoformat(), "visitors": random.randint(10,400)} for i in range(144)])

    # write files
    save_csv(tx, os.path.join(OUT_DIR, "transactions.csv"), index=False)
    save_csv(sysm, os.path.join(OUT_DIR, "system_metrics.csv"), index=False)
    save_csv(crm, os.path.join(OUT_DIR, "crm_events.csv"), index=False)
    save_csv(web, os.path.join(OUT_DIR, "web_traffic.csv"), index=False)

    print("Files written to", OUT_DIR)
    print("transactions rows:", len(tx))
    print("system_metrics rows:", len(sysm))

if __name__ == "__main__":
    main()
