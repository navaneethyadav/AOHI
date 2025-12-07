"""
streaming/producer.py

Simulates a real-time event producer.

It reads data/transactions.csv in timestamp order and
slowly appends rows into runtime/stream_events.csv, one by one,
to imitate a live event stream.

Run from project root:
    python streaming/producer.py
"""

import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "transactions.csv"
RUNTIME_DIR = ROOT / "runtime"
STREAM_FILE = RUNTIME_DIR / "stream_events.csv"


def find_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    for col in ["timestamp", "event_time", "ts", "time"]:
        if col in df.columns:
            return col
    return None


def main() -> None:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Cannot find {DATA_PATH}")

    RUNTIME_DIR.mkdir(exist_ok=True, parents=True)

    print(f"[producer] Loading {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)

    ts_col = find_timestamp_column(df)
    if ts_col is None:
        raise ValueError(
            "No timestamp column found in transactions.csv. "
            "Expected one of: timestamp, event_time, ts, time."
        )

    # sort by time so stream is chronological
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.sort_values(ts_col)

    # optional: limit to first N rows so demo is not too long
    MAX_ROWS = 300
    if len(df) > MAX_ROWS:
        df = df.head(MAX_ROWS)

    # if previous stream file exists, archive it
    if STREAM_FILE.exists():
        backup = STREAM_FILE.with_name("stream_events_previous.csv")
        print(f"[producer] Backing up old stream file to {backup}")
        STREAM_FILE.rename(backup)

    print(f"[producer] Writing events gradually into {STREAM_FILE}")
    header_written = False
    total_rows = len(df)

    with open(STREAM_FILE, "w", encoding="utf-8") as f:
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            # write header only once
            if not header_written:
                f.write(",".join(df.columns) + "\n")
                header_written = True

            line = ",".join(str(row[col]) for col in df.columns)
            f.write(line + "\n")
            f.flush()

            print(f"[producer] Sent event {idx}/{total_rows}")
            time.sleep(0.5)  # simulate 1 event per 0.5 sec

    print("[producer] Finished sending all events.")


if __name__ == "__main__":
    main()
