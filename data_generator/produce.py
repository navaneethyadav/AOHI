#!/usr/bin/env python3
"""
Simple human-authored multi-source synthetic data generator for AOHI.

Generates:
 - transactions.csv
 - web_traffic.csv
 - system_metrics.csv
 - crm_events.csv

Supports anomaly injection: spike_failed_tx, traffic_drop, latency_drift, support_surge
Reproducible via --seed
"""

import argparse
import os
from datetime import datetime, timedelta
import random
import math
import pandas as pd

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def generate_timestamps(start_dt, periods, freq_seconds):
    return [start_dt + timedelta(seconds=i * freq_seconds) for i in range(periods)]

def gen_transactions(timestamps, seed, inject=None):
    random.seed(seed)
    rows = []
    tx_id = 100000
    base_amount = 50.0
    for ts in timestamps:
        count = random.choice([0, 1, 1, 2, 3])
        for _ in range(count):
            tx_id += 1
            amount = round(base_amount * (0.5 + random.random()*2.0), 2)
            status = "success" if random.random() > 0.01 else "failed"
            rows.append({
                "timestamp": ts.isoformat(),
                "tx_id": tx_id,
                "amount": amount,
                "country": random.choice(["IN","US","UK","DE"]),
                "product_id": random.randint(100,130),
                "status": status
            })

    df = pd.DataFrame(rows)
    if inject == "spike_failed_tx":
        mid = len(timestamps)//2
        extras = []
        for i in range(mid, mid+10):
            ts = timestamps[i].isoformat()
            for j in range(20):
                tx_id += 1
                extras.append({
                    "timestamp": ts,
                    "tx_id": tx_id,
                    "amount": round(base_amount * (0.5 + random.random()*2.0), 2),
                    "country": "IN",
                    "product_id": random.randint(100,130),
                    "status": "failed"
                })
        if extras:
            df = pd.concat([df, pd.DataFrame(extras)], ignore_index=True)
    return df

def gen_web_traffic(timestamps, seed, inject=None):
    random.seed(seed+1)
    rows = []
    start = timestamps[0]
    for ts in timestamps:
        secs = (ts - start).total_seconds()
        day_seconds = 24*3600
        daily = 100 + 50 * math.sin(2 * math.pi * (secs % day_seconds) / day_seconds)
        noise = random.gauss(0, 5)
        sessions = max(1, int(daily + noise))
        rows.append({"timestamp": ts.isoformat(), "page": "/home", "sessions": sessions, "users": max(1, int(sessions*0.7))})
    df = pd.DataFrame(rows)
    if inject == "traffic_drop":
        mid = len(timestamps)//3
        for i in range(mid, min(mid+8, len(df))):
            df.at[i, "sessions"] = int(df.at[i, "sessions"] * 0.2)
    return df

def gen_system_metrics(timestamps, seed, inject=None):
    random.seed(seed+2)
    rows = []
    for ts in timestamps:
        cpu = max(1.0, min(99.0, 20 + random.random()*15))
        mem = max(10.0, min(95.0, 30 + random.random()*20))
        latency = max(10.0, min(200.0, 50 + random.random()*40))
        rows.append({"timestamp": ts.isoformat(), "host": "app01", "cpu": round(cpu,2), "mem": round(mem,2), "latency_ms": round(latency,2)})
    df = pd.DataFrame(rows)
    if inject == "latency_drift":
        start = len(timestamps)//4
        for i in range(start, min(start+30, len(df))):
            df.at[i, "latency_ms"] = df.at[i, "latency_ms"] * (1 + 0.05 * (i - start))
    return df

def gen_crm_events(timestamps, seed, inject=None):
    random.seed(seed+3)
    rows = []
    uid = 2000
    for ts in timestamps:
        if random.random() < 0.05:
            uid += 1
            ev = random.choice(["signup","login","purchase","support_ticket"])
            rows.append({"timestamp": ts.isoformat(), "user_id": uid, "event_type": ev, "support_ticket": 1 if ev=="support_ticket" else 0})
    df = pd.DataFrame(rows)
    if inject == "support_surge":
        mid = len(timestamps)//2
        extras = []
        for i in range(mid, min(mid+5, len(timestamps))):
            for j in range(10):
                uid += 1
                extras.append({"timestamp": timestamps[i].isoformat(), "user_id": uid, "event_type": "support_ticket", "support_ticket":1})
        if extras:
            df = pd.concat([df, pd.DataFrame(extras)], ignore_index=True)
    return df

def write_csv(df, path):
    df.to_csv(path, index=False)

def main(output_dir, seed, inject):
    ensure_dir(output_dir)
    start = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    timestamps = generate_timestamps(start, periods=288, freq_seconds=300)

    tx = gen_transactions(timestamps, seed, inject="spike_failed_tx" if inject=="spike_failed_tx" else None)
    wt = gen_web_traffic(timestamps, seed, inject="traffic_drop" if inject=="traffic_drop" else None)
    sysm = gen_system_metrics(timestamps, seed, inject="latency_drift" if inject=="latency_drift" else None)
    crm = gen_crm_events(timestamps, seed, inject="support_surge" if inject=="support_surge" else None)

    write_csv(tx, os.path.join(output_dir, "transactions.csv"))
    write_csv(wt, os.path.join(output_dir, "web_traffic.csv"))
    write_csv(sysm, os.path.join(output_dir, "system_metrics.csv"))
    write_csv(crm, os.path.join(output_dir, "crm_events.csv"))

    print(f"Generated files in {output_dir}:")
    for f in ["transactions.csv","web_traffic.csv","system_metrics.csv","crm_events.csv"]:
        print(" -", f)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AOHI synthetic data generator")
    parser.add_argument("--output", "-o", default="data", help="output directory")
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    parser.add_argument("--inject", type=str, default=None, choices=[None,"spike_failed_tx","traffic_drop","latency_drift","support_surge"], help="anomaly to inject")
    args = parser.parse_args()
    main(args.output, args.seed, args.inject)
