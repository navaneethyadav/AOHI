# AOHI — Adaptive Operational Health Intelligence

AOHI is an end-to-end real-time anomaly detection and root-cause analysis demo platform built as a resume-grade project.

## Features
- Synthetic event & metrics data generator (with injected anomalies)
- Multiple detectors: seasonal z-score, EWMA, latency spike, revenue drop, geo-failure
- Rule-based RCA engine mapping detectors -> root causes -> playbooks
- FastAPI backend exposing `/incidents`, `/run_detectors`, `/run_extra_detectors`
- Interactive Streamlit dashboard with Plotly charts and incident drill-down
- Docker-ready architecture (optional)

## Quickstart (local)
1. Activate venv:
```powershell
cd C:\mainprojects\AOHI
& .\.venv\Scripts\Activate.ps1
