# detectors/revenue.py
"""
Revenue drop detector.
Assumes transactions.csv has an 'amount' column and 'status'. Buckets revenue by freq,
computes rolling baseline (median) and flags large drops: current_total < baseline * factor
"""

import pandas as pd

def detect_revenue_drop(csv_path="data/transactions.csv", ts_col="timestamp", freq="1H",
                        window=6, factor=0.7, min_revenue=1.0):
    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No transactions found.")
        return []

    df['bucket'] = df[ts_col].dt.floor(freq)
    # only successful revenue contributes
    revenue = df[df['status']=='success'].groupby('bucket')['amount'].sum().rename('revenue')
    revenue = revenue.sort_index()
    results = []
    # rolling baseline: median of previous `window` buckets
    rolling_med = revenue.shift(1).rolling(window=window, min_periods=1).median().fillna(0)
    for ts in revenue.index:
        baseline = rolling_med.loc[ts]
        cur = revenue.loc[ts]
        if baseline <= 0:
            continue
        if cur < baseline * factor and cur >= min_revenue:
            print(f"Revenue drop at {ts} current={cur:.2f} baseline={baseline:.2f}")
            results.append({"timestamp": ts.isoformat(), "current_revenue": float(cur), "baseline": float(baseline)})
    if not results:
        print("No revenue drops found.")
    return results

if __name__ == "__main__":
    detect_revenue_drop()
