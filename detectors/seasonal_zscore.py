"""
Human-coded seasonal z-score detector for failed transactions.
"""

import pandas as pd
import numpy as np
import math

# --- NEW HELPER ADDED ---
def safe_num(x):
    """Return x if finite, else convert inf/-inf/nan to string."""
    if x is None:
        return None
    if isinstance(x, float):
        return x if math.isfinite(x) else str(x)
    return x


def detect_failed_tx_spike(csv_path, ts_col='timestamp', status_col='status',
                           freq='5T', window=6, z_thresh=1, min_failed=5):

    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No transactions found.")
        return []

    # Bucket events into fixed time intervals
    df['bucket'] = df[ts_col].dt.floor(freq)

    # Count failed and total events in each bucket
    failed = df[df[status_col] != 'success'].groupby('bucket').size().rename('failed_count')
    total = df.groupby('bucket').size().rename('total_count')
    agg = pd.concat([total, failed], axis=1).fillna(0)
    agg['failed_count'] = agg['failed_count'].astype(int)

    # Rolling mean and std for seasonal baseline
    agg['rolling_mean'] = agg['failed_count'].shift(1).rolling(window=window, min_periods=1).mean()
    agg['rolling_std'] = agg['failed_count'].shift(1).rolling(window=window, min_periods=1).std().fillna(1)

    # Z-score calculation
    agg['zscore'] = (agg['failed_count'] - agg['rolling_mean']) / agg['rolling_std']

    # Filter anomalies
    anomalies = agg[(agg['zscore'] > z_thresh) & (agg['failed_count'] >= min_failed)]

    if anomalies.empty:
        print("No anomalies found.")
        return []

    results = []
    print("\nAnomalies detected:")
    for idx, row in anomalies.iterrows():

        # Apply safe_num so JSON never contains inf or -inf
        z_val = safe_num(float(row['zscore']))

        print(f" - {idx} failed={row['failed_count']} z={z_val}")

        results.append({
            "timestamp": idx.isoformat(),
            "failed": int(row['failed_count']),
            "zscore": z_val   # <-- SAFE VALUE (never raw inf)
        })

    return results


if __name__ == "__main__":
    detect_failed_tx_spike("data/transactions.csv")
