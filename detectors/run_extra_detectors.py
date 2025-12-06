# detectors/run_extra_detectors.py
import json
from detectors.latency import detect_latency_spike
from detectors.revenue import detect_revenue_drop
from detectors.geo import detect_geo_failures

def run_all_extra():
    res = {
        "latency": detect_latency_spike(),
        "revenue": detect_revenue_drop(),
        "geo": detect_geo_failures()
    }
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    run_all_extra()
