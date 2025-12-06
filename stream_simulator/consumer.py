# stream_simulator/consumer.py
"""
Simple consumer:
 - watches stream/transactions folder
 - moves each JSON event to an 'ingested' CSV file (append)
 - after each batch of N events, runs the seasonal zscore detector on the ingested CSV
"""
import argparse
import json
import os
import time
import csv
from pathlib import Path
from detectors.seasonal_zscore import detect_failed_tx_spike

def ensure_dir(p): Path(p).mkdir(parents=True, exist_ok=True)

def consume(stream_dir, ingested_csv, poll=1.0, batch=10):
    ensure_dir(stream_dir)
    ensure_dir(os.path.dirname(ingested_csv) or ".")
    # create CSV file with header if not exists
    header_written = Path(ingested_csv).exists()
    while True:
        files = sorted([f for f in os.listdir(stream_dir) if f.endswith(".json")])
        if not files:
            time.sleep(poll)
            continue
        count = 0
        for fname in files[:batch]:
            path = os.path.join(stream_dir, fname)
            with open(path, 'r', encoding='utf-8') as rf:
                obj = json.load(rf)
            # append to CSV
            write_header = not Path(ingested_csv).exists()
            with open(ingested_csv, 'a', newline='', encoding='utf-8') as wf:
                writer = csv.DictWriter(wf, fieldnames=list(obj.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(obj)
            os.remove(path)
            count += 1
        if count > 0:
            print(f"Ingested {count} events. Running detector...")
            detect_failed_tx_spike(ingested_csv)
        time.sleep(poll)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--stream", "-s", default="stream/transactions", help="stream input folder")
    p.add_argument("--out", "-o", default="data/ingested_transactions.csv", help="ingested CSV path")
    p.add_argument("--poll", type=float, default=1.0, help="poll interval seconds")
    p.add_argument("--batch", type=int, default=20, help="events per batch")
    args = p.parse_args()
    consume(args.stream, args.out, args.poll, args.batch)
