# stream_simulator/producer.py
"""
Simple file-based stream producer.
Reads a CSV and writes each row as a JSON file into an output folder,
sleeping `delay` seconds between events so consumer can process them like a stream.
"""
import argparse
import csv
import json
import os
import time
from pathlib import Path

def ensure_dir(p): 
    Path(p).mkdir(parents=True, exist_ok=True)

def produce(csv_path, out_dir, delay=0.5, repeat=False):
    ensure_dir(out_dir)
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    idx = 0
    run = 0
    while True:
        for r in rows:
            ts = r.get('timestamp', '')
            filename = f"{int(time.time()*1000)}_{idx}.json"
            path = os.path.join(out_dir, filename)
            with open(path, 'w', encoding='utf-8') as wf:
                json.dump(r, wf)
            idx += 1
            time.sleep(delay)
        run += 1
        if not repeat:
            break

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", "-s", required=True, help="input CSV (transactions.csv)")
    p.add_argument("--out", "-o", default="stream/transactions", help="output folder for events")
    p.add_argument("--delay", "-d", type=float, default=0.5, help="seconds between events")
    p.add_argument("--repeat", action="store_true", help="loop forever")
    args = p.parse_args()
    produce(args.source, args.out, args.delay, args.repeat)
