"""
Human-coded latency spike detector for AOHI.

If the input CSV does not have a `latency_ms` column, this detector
will simply return an empty list (no anomalies) instead of raising an
error. This keeps the API logs clean and avoids stack traces.
"""

from __future__ import annotations

from typing import List, Dict, Any

import pandas as pd
import numpy as np


def detect_latency_spike(
    csv_path: str,
    ts_col: str = "timestamp",
    latency_col: str = "latency_ms",
    freq: str = "5T",
    window: int = 6,
    z_thresh: float = 2.5,
    min_count: int = 10,
) -> List[Dict[str, Any]]:
    """
    Detect latency spikes using a rolling z-score on median latency.

    Returns a list of:
    {
        "timestamp": ISO string,
        "latency_median": float,
        "zscore": float,
        "count": int
    }

    If `latency_ms` is missing, returns [] and prints a small message.
    """
    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No transactions found for latency detector.")
        return []

    if latency_col not in df.columns:
        # 🔧 IMPORTANT: avoid raising KeyError and spamming logs
        print(f"No latency column '{latency_col}' found; skipping latency detector.")
        return []

    # Bucket into time windows
    df["bucket"] = df[ts_col].dt.floor(freq)

    # Aggregate median latency + count
    agg = (
        df.groupby("bucket")[latency_col]
        .agg(["median", "count"])
        .rename(columns={"median": "latency_median", "count": "count"})
    )

    if agg.empty:
        print("No latency data after aggregation.")
        return []

    # Rolling stats
    agg["rolling_mean"] = agg["latency_median"].shift(1).rolling(window=window, min_periods=1).mean()
    agg["rolling_std"] = agg["latency_median"].shift(1).rolling(window=window, min_periods=1).std().fillna(1.0)

    # Z-score
    agg["zscore"] = (agg["latency_median"] - agg["rolling_mean"]) / agg["rolling_std"]

    # Filter anomalies
    anomalies = agg[(agg["zscore"] > z_thresh) & (agg["count"] >= min_count)]

    if anomalies.empty:
        print("No latency anomalies found.")
        return []

    results: List[Dict[str, Any]] = []
    print("\nLatency anomalies detected:")
    for ts, row in anomalies.iterrows():
        print(
            f" - {ts} median={row['latency_median']:.2f}ms "
            f"z={row['zscore']:.2f} count={int(row['count'])}"
        )
        results.append(
            {
                "timestamp": ts.isoformat(),
                "latency_median": float(row["latency_median"]),
                "zscore": float(row["zscore"]),
                "count": int(row["count"]),
            }
        )

    return results


if __name__ == "__main__":
    # Manual test (you can run from project root with: python -m detectors.latency)
    detect_latency_spike("data/transactions.csv")
