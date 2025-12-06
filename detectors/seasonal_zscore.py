# detectors/seasonal_zscore.py
"""
Human-coded seasonal z-score detector for failed transactions.
"""

import pandas as pd
import numpy as np

def detect_failed_tx_spike(csv_path, ts_col='timestamp', status_col='status',
                           freq='5T', window=6, z_thresh=1, min_failed=5):
    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No transactions found.")
        return []

    df['bucket'] = df[ts_col].dt.floor(freq)
    failed = df[df[status_col] != 'success'].groupby('bucket').size().rename('failed_count')
    total = df.groupby('bucket').size().rename('total_count')
    agg = pd.concat([total, failed], axis=1).fillna(0)
    agg['failed_count'] = agg['failed_count'].astype(int)

    agg['rolling_mean'] = agg['failed_count'].shift(1).rolling(window=window, min_periods=1).mean()
    agg['rolling_std'] = agg['failed_count'].shift(1).rolling(window=window, min_periods=1).std().fillna(1)

    agg['zscore'] = (agg['failed_count'] - agg['rolling_mean']) / agg['rolling_std']

    anomalies = agg[(agg['zscore'] > z_thresh) & (agg['failed_count'] >= min_failed)]

    if anomalies.empty:
        print("No anomalies found.")
        return []

    results = []
    print("\nAnomalies detected:")
    for idx, row in anomalies.iterrows():
        print(f" - {idx} failed={row['failed_count']} z={row['zscore']:.2f}")
        results.append({
            "timestamp": idx.isoformat(),
            "failed": int(row['failed_count']),
            "zscore": float(row['zscore'])
        })
    return results

if __name__ == "__main__":
    detect_failed_tx_spike("data/transactions.csv")
