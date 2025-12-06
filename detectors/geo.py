# detectors/geo.py
"""
Geo-specific failure detector.
Flags a country if failed_count in that country in a bucket exceeds threshold.
"""

import pandas as pd

def detect_geo_failures(csv_path="data/transactions.csv", ts_col="timestamp", country_col="country",
                        freq="5T", threshold=5):
    df = pd.read_csv(csv_path, parse_dates=[ts_col])
    if df.empty:
        print("No transactions found.")
        return []

    df['bucket'] = df[ts_col].dt.floor(freq)
    failed = df[df['status'] != 'success']
    if failed.empty:
        print("No failed transactions found.")
        return []

    grp = failed.groupby(['bucket', country_col]).size().rename('failed_count').reset_index()
    results = []
    for _, row in grp.iterrows():
        if int(row['failed_count']) >= threshold:
            ts = row['bucket']
            print(f"Geo failure: {row[country_col]} at {ts} failed={row['failed_count']}")
            results.append({"timestamp": pd.to_datetime(ts).isoformat(), "country": row[country_col], "failed_count": int(row['failed_count'])})
    if not results:
        print("No geo failures found.")
    return results

if __name__ == "__main__":
    detect_geo_failures()
