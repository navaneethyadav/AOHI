# detectors/run_all_detectors.py
"""
Run both detectors and print a combined incident list.
This helps you show multiple detectors agreeing on the same time windows.
"""

from detectors.seasonal_zscore import detect_failed_tx_spike
from detectors.ewma import detect_ewma_failed

def run_all(path="data/transactions.csv"):
    print("Running seasonal z-score detector...")
    a = detect_failed_tx_spike(path, window=6, z_thresh=1, min_failed=10)
    print("\nRunning EWMA detector...")
    b = detect_ewma_failed(path, span=6, k=2, min_failed=5)

    # simple merge by timestamp (string ISO)
    t1 = {x['timestamp'].split('+')[0] if '+' in x['timestamp'] else x['timestamp']: x for x in a}
    t2 = {x['timestamp'].split('+')[0] if '+' in x['timestamp'] else x['timestamp']: x for x in b}

    combined = set(list(t1.keys()) + list(t2.keys()))
    print("\n== Combined incidents ==")
    for ts in sorted(combined):
        entry = {"timestamp": ts}
        if ts in t1:
            entry['z_detector'] = t1[ts]
        if ts in t2:
            entry['ewma_detector'] = t2[ts]
        print(entry)

if __name__ == "__main__":
    run_all("data/transactions.csv")
