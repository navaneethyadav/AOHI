# api/diag_detectors.py
"""
Diagnostic helper: discovers detectors in detectors/ and attempts to call them.
Run: python api/diag_detectors.py
"""

import sys, pkgutil, importlib, traceback
from pathlib import Path
import inspect
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DETECTORS_PATH = PROJECT_ROOT / "detectors"
DETECTORS_PKG = "detectors"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

print("PROJECT_ROOT:", PROJECT_ROOT)
print("DETECTORS_PATH:", DETECTORS_PATH)
print()

detectors = []
for finder, name, ispkg in pkgutil.iter_modules([str(DETECTORS_PATH)]):
    full_mod = f"{DETECTORS_PKG}.{name}"
    print("Importing", full_mod)
    try:
        mod = importlib.import_module(full_mod)
    except Exception:
        print("  IMPORT ERROR for", full_mod)
        print(traceback.format_exc())
        continue

    for attr in dir(mod):
        if attr.startswith("detect_") and callable(getattr(mod, attr)):
            detectors.append((full_mod + "." + attr, getattr(mod, attr)))

print("\nDiscovered detectors:", [d[0] for d in detectors])
print()

# Try to prepare a sample df if possible
sample_df = None
csv_candidate = PROJECT_ROOT / "data" / "transactions.csv"
if csv_candidate.exists():
    try:
        sample_df = pd.read_csv(csv_candidate, parse_dates=True)
        print("Loaded sample df from", csv_candidate)
    except Exception:
        sample_df = None
        print("Failed to load sample CSV", csv_candidate)
else:
    print("No transactions.csv found, sample_df will be None")

print("\nCalling detectors one by one (safe):\n")
for name, func in detectors:
    print("===", name, "===")
    try:
        # try no-arg
        try:
            out = func()
            print("-> called no-arg, output type:", type(out))
            print(out)
            continue
        except TypeError:
            pass

        # inspect signature
        sig = inspect.signature(func)
        params = sig.parameters
        kwargs = {}
        if any("csv" in p.lower() or "path" in p.lower() for p in params):
            # pass path string if available
            csv_path = str(csv_candidate) if csv_candidate.exists() else None
            if csv_path:
                for n in params:
                    if "csv" in n.lower() or "path" in n.lower():
                        kwargs[n] = csv_path
        elif any(p.lower() in ("df","data","dataframe") for p in params):
            for n in params:
                if n.lower() in ("df","data","dataframe"):
                    kwargs[n] = sample_df

        if kwargs:
            out = func(**kwargs)
            print("-> called with kwargs", kwargs.keys(), "output type:", type(out))
            print(out)
            continue

        # fallback: try single arg: df then csv str
        try:
            if sample_df is not None:
                out = func(sample_df)
                print("-> called with sample_df fallback:", type(out))
                print(out)
                continue
        except Exception:
            pass

        try:
            if csv_candidate.exists():
                out = func(str(csv_candidate))
                print("-> called with csv path fallback:", type(out))
                print(out)
                continue
        except Exception:
            pass

        print("-> Could not call detector:", name)
    except Exception:
        print("EXCEPTION calling", name)
        print(traceback.format_exc())
