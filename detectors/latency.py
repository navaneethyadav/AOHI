# detectors/latency.py
"""
Simple latency spike detector.
Looks at system_metrics.csv bucketed by freq and flags buckets where
median latency > baseline_median * factor (baseline = prior window median).
"""

import pandas as pd

def detect_latency_spike(csv_path="data/system_metrics.csv", ts_col="timestamp", freq="5T",
                         prior_minutes=30, factor=1.5, min_count=3):
    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No system metrics found.")
        return []

    df['bucket'] = df[ts_col].dt.floor(freq)
    agg = df.groupby('bucket')['latency_ms'].agg(['median','count']).rename(columns={'median':'latency_median','count':'count'})
    agg = agg.sort_index()

    results = []
    for idx in agg.index:
        # compute prior window
        prior_start = idx - pd.Timedelta(minutes=prior_minutes)
        prior = agg[(agg.index >= prior_start) & (agg.index < idx)]
        if prior.empty or agg.at[idx,'count'] < min_count:
            continue
        baseline = prior['latency_median'].median()
        cur = agg.at[idx, 'latency_median']
        if baseline == 0 or pd.isna(baseline):
            continue
        if cur > baseline * factor:
            print(f"Latency anomaly at {idx} median={cur:.1f} baseline={baseline:.1f}")
            results.append({"timestamp": idx.isoformat(), "latency_median": float(cur), "baseline": float(baseline)})
    if not results:
        print("No latency anomalies found.")
    return results

if __name__ == "__main__":
    detect_latency_spike()
