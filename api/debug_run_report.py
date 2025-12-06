# api/debug_run_report.py
# Small wrapper to run the report generator with maximum verbosity and log everywhere.

import sys
import subprocess
from pathlib import Path
import traceback
import time

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "api" / "generate_report_pro.py"
OUT = ROOT / "data" / "debug_report_wrapper.pdf"
LOG = ROOT / "data" / "debug_wrapper.log"

def write(s):
    print(s)
    try:
        LOG.write_text(str(time.strftime("%Y-%m-%d %H:%M:%S")) + " " + str(s) + "\n", append=True)  # Python <3.11 fallback: will replace next line
    except TypeError:
        # older python versions do not support append param: fallback:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(str(time.strftime("%Y-%m-%d %H:%M:%S")) + " " + str(s) + "\n")

def main():
    write(f"Running: {sys.executable} {SCRIPT} --out {OUT} --name \"Navaneeth Kaku\" --api http://127.0.0.1:8000/rca")
    try:
        proc = subprocess.run([sys.executable, str(SCRIPT), "--out", str(OUT), "--name", "Navaneeth Kaku", "--api", "http://127.0.0.1:8000/rca"],
                              capture_output=True, text=True, cwd=str(ROOT), timeout=90)
        write("RETURN CODE: " + str(proc.returncode))
        write("STDOUT:\n" + (proc.stdout or "<no stdout>"))
        write("STDERR:\n" + (proc.stderr or "<no stderr>"))
    except subprocess.TimeoutExpired as e:
        write("TIMEOUT: " + str(e))
        write(traceback.format_exc())
    except Exception as e:
        write("EXCEPTION: " + str(e))
        write(traceback.format_exc())

    write("Files in data directory (top 30):")
    try:
        files = sorted((ROOT / "data").iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)[:30]
        for p in files:
            write(f" - {p.name} (size={p.stat().st_size})")
    except Exception:
        write("Unable to list data dir:\n" + traceback.format_exc())

if __name__ == "__main__":
    main()