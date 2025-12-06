# detectors/ewma.py
"""
Simple EWMA (Exponentially Weighted Moving Average) drift detector for a count metric.
This detector computes EWMA on the failed_count series and flags points where the
absolute deviation from EWMA exceeds k * EWMA_std.

Functions:
 - detect_ewma_failed(csv_path, ts_col='timestamp', status_col='status', freq='5T',
                      span=6, k=3, min_failed=5)
"""

import pandas as pd
import numpy as np

def compute_failed_buckets(df, ts_col='timestamp', status_col='status', freq='5T'):
    df[ts_col] = pd.to_datetime(df[ts_col])
    df['bucket'] = df[ts_col].dt.floor(freq)
    failed = df[df[status_col] != 'success'].groupby('bucket').size().rename('failed_count')
    total = df.groupby('bucket').size().rename('total_count')
    agg = pd.concat([total, failed], axis=1).fillna(0)
    agg['failed_count'] = agg['failed_count'].astype(int)
    return agg

def detect_ewma_failed(csv_path, ts_col='timestamp', status_col='status', freq='5T',
                       span=6, k=3, min_failed=5):
    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No transactions found.")
        return []

    agg = compute_failed_buckets(df, ts_col, status_col, freq)

    # EWMA (pandas ewm). Use span (smoothing) and adjust=False for classic EWMA
    ewma = agg['failed_count'].ewm(span=span, adjust=False).mean()
    # approximate std of residuals using rolling std on residuals
    resid = agg['failed_count'] - ewma
    # use rolling window for std estimation (span*2 or 10)
    roll_std = resid.shift(1).rolling(window=max(3, span)).std().fillna(resid.std() if not resid.empty else 1.0)
    score = (agg['failed_count'] - ewma).abs() / (roll_std.replace(0, 1.0))

    agg['ewma'] = ewma
    agg['resid_std'] = roll_std
    agg['score'] = score

    anomalies = agg[(agg['score'] > k) & (agg['failed_count'] >= min_failed)]

    if anomalies.empty:
        print("No EWMA anomalies found.")
        return []

    results = []
    print("\nEWMA anomalies detected:")
    for idx, row in anomalies.iterrows():
        print(f" - {idx} failed={int(row['failed_count'])} score={row['score']:.2f}")
        results.append({"timestamp": idx.isoformat(), "failed": int(row['failed_count']), "score": float(row['score'])})
    return results

if __name__ == "__main__":
    detect_ewma_failed("data/transactions.csv")
