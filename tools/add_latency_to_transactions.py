"""
Utility script to add a latency_ms column to transactions.csv

Run from project root:
    python tools/add_latency_to_transactions.py
"""

import os
import numpy as np
import pandas as pd

# Adjust this if your file name is different
TRANSACTIONS_PATH = os.path.join("data", "transactions.csv")


def main() -> None:
    if not os.path.exists(TRANSACTIONS_PATH):
        raise FileNotFoundError(
            f"Could not find {TRANSACTIONS_PATH}. "
            "If your file has a different name, update TRANSACTIONS_PATH in this script."
        )

    print(f"Loading {TRANSACTIONS_PATH} ...")
    df = pd.read_csv(TRANSACTIONS_PATH)

    # Try to parse timestamp if present
    ts_col = None
    for c in ["timestamp", "event_time", "ts"]:
        if c in df.columns:
            ts_col = c
            break

    if ts_col is None:
        raise ValueError(
            "Could not find a timestamp column in transactions CSV. "
            "Expected one of: timestamp, event_time, ts"
        )

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

    # -------------------------
    # 1) Add base latency pattern
    # -------------------------
    # Normal latency ~ 80–250 ms
    base_latency = np.random.normal(loc=150, scale=40, size=len(df)).clip(40, 400)

    # Convert to int
    df["latency_ms"] = base_latency.astype(int)

    # -------------------------
    # 2) Inject specific latency anomaly to match your existing anomalies
    #    around 2025-12-05 03:00 in country IN
    # -------------------------
    if "country" in df.columns:
        mask_anomaly = (
            (df[ts_col] >= "2025-12-05 03:00:00")
            & (df[ts_col] <= "2025-12-05 03:10:00")
            & (df["country"] == "IN")
        )

        n_anomaly_rows = mask_anomaly.sum()
        if n_anomaly_rows > 0:
            print(f"Injecting high latency for {n_anomaly_rows} rows in IN between 03:00–03:10")
            # Very high latency for anomaly rows
            df.loc[mask_anomaly, "latency_ms"] = np.random.randint(1500, 3000, size=n_anomaly_rows)
        else:
            print("No rows matched anomaly mask (country IN, 03:00–03:10). Skipping explicit spike.")
    else:
        print("No 'country' column found; adding only generic latency without geo-specific spike.")

    # -------------------------
    # 3) Save back
    # -------------------------
    backup_path = TRANSACTIONS_PATH.replace(".csv", "_backup_before_latency.csv")
    if not os.path.exists(backup_path):
        print(f"Saving backup to {backup_path}")
        df.to_csv(backup_path, index=False)

    print(f"Writing updated CSV with latency_ms to {TRANSACTIONS_PATH}")
    df.to_csv(TRANSACTIONS_PATH, index=False)

    print("Done. Your transactions.csv now has a latency_ms column.")


if __name__ == "__main__":
    main()
