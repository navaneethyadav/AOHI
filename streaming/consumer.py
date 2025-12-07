"""
streaming/consumer.py

Simulates a real-time event consumer.

It watches runtime/stream_events.csv and, whenever new rows appear,
it recomputes a few live metrics:

- total events
- failure rate (if a status column exists)
- average latency_ms (if latency_ms exists)

Run from project root:
    python streaming/consumer.py
"""

import time
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / "runtime"
STREAM_FILE = RUNTIME_DIR / "stream_events.csv"


def find_columns(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    """
    Return (status_col, latency_col) if present.
    """
    status_col = None
    latency_col = None

    for col in ["status", "result", "state"]:
        if col in df.columns:
            status_col = col
            break

    if "latency_ms" in df.columns:
        latency_col = "latency_ms"

    return status_col, latency_col


def main() -> None:
    print(f"[consumer] Watching for {STREAM_FILE}")

    last_seen_rows = 0

    while True:
        if not STREAM_FILE.exists():
            print("[consumer] Stream file not found yet. Waiting...")
            time.sleep(1.5)
            continue

        try:
            df = pd.read_csv(STREAM_FILE)
        except Exception as e:
            # file might be mid-write, just retry
            print(f"[consumer] Error reading stream file: {e}. Retrying...")
            time.sleep(1.0)
            continue

        current_rows = len(df)
        if current_rows == last_seen_rows:
            # no new data
            time.sleep(1.0)
            continue

        new_rows = current_rows - last_seen_rows
        last_seen_rows = current_rows

        status_col, latency_col = find_columns(df)

        print("\n[consumer] ================= LIVE METRICS =================")
        print(f"[consumer] Total events seen: {current_rows} (+{new_rows} new)")

        # failure rate
        if status_col is not None:
            # assuming failed rows have status like "FAILED" / "ERROR"
            failed_mask = df[status_col].astype(str).str.upper().isin(
                ["FAILED", "FAILURE", "ERROR"]
            )
            failed_count = failed_mask.sum()
            failure_rate = (failed_count / current_rows) * 100 if current_rows else 0.0
            print(
                f"[consumer] Failures: {failed_count}/{current_rows} "
                f"({failure_rate:.2f}% failure rate)"
            )
        else:
            print("[consumer] No status column found; skipping failure rate.")

        # latency
        if latency_col is not None:
            avg_latency = df[latency_col].mean()
            p95_latency = df[latency_col].quantile(0.95)
            print(
                f"[consumer] Avg latency: {avg_latency:.1f} ms | "
                f"P95 latency: {p95_latency:.1f} ms"
            )

            # simple live anomaly rule
            if p95_latency > 1500:
                print(
                    "[consumer] ⚠ HIGH LATENCY ALERT: "
                    f"P95 latency {p95_latency:.1f} ms (> 1500 ms)"
                )
        else:
            print("[consumer] No latency_ms column found; skipping latency analysis.")

        print("[consumer] ==================================================")

        time.sleep(1.5)


if __name__ == "__main__":
    main()
