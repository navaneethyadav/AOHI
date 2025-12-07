# AOHI – Adaptive Operational Health Intelligence

AOHI is a **real-time operational health intelligence system** that monitors key business and system metrics, detects anomalies, performs root cause analysis (RCA), and generates **professional investigation reports (PDF)**.

It simulates how real companies (e-commerce, banking, SaaS) monitor their systems using **data, APIs, dashboards, and automation**.

---

## 🔍 Core Capabilities

- 📡 **Real-time Monitoring** of synthetic company metrics:
  - Transactions
  - Failures
  - Latency
  - Revenue
  - Geo (country-wise) behaviour

- 📈 **Anomaly Detection Engine**  
  Uses multiple statistical detectors:
  - EWMA-based failure spike detection
  - Seasonal Z-Score (failed_tx spike)
  - Revenue drop detection (hourly buckets)
  - Geo failure detection (country-wise failures)
  - Latency anomalies (if `latency_ms` column present)

- 🧠 **Root Cause Analysis (RCA) Engine**
  - Combines detector outputs
  - Uses rule-based logic and confidence scoring
  - Produces human-readable RCA summary

- 🧾 **Automatic PDF Reporting**
  - `/report_pro` endpoint generates a **professional PDF report**
  - Includes:
    - Overview of incidents
    - Detector summary
    - RCA explanation
    - Time window of anomalies

- 📊 **AOHI Dashboard (Streamlit UI)**
  - API health check
  - Live incident viewer
  - RCA viewer
  - PDF report trigger & download

- 🐳 **Dockerized Deployment**
  - `aohi-api` container → FastAPI backend
  - `aohi-ui` container → Streamlit dashboard
  - `docker compose up` runs the entire system

---

## 🏗️ Project Structure

```text
AOHI/
├─ api/
│  ├─ fastapi_app.py           # Main FastAPI application
│  ├─ generate_report_pro.py   # PDF report generator (pro version)
│  └─ ...                      # Other API utilities
├─ ui/
│  ├─ dashboard.py             # Streamlit AOHI dashboard
│  └─ ...
├─ detectors/
│  ├─ ewma.py                  # EWMA anomaly detector
│  ├─ geo.py                   # Geo (country-wise) failure detector
│  ├─ latency.py               # Latency anomaly detector
│  ├─ seasonal_zscore.py       # Seasonal Z-score detector
│  ├─ revenue.py               # Revenue anomaly detector
│  └─ run_all_detectors.py     # Helper to run multiple detectors
├─ data/
│  ├─ transactions.csv         # Synthetic transaction data
│  ├─ web_traffic.csv          # Synthetic web traffic (if used)
│  ├─ system_metrics.csv       # Synthetic system metrics (if used)
│  └─ ...
├─ runtime/
│  ├─ stream_events.csv        # Live event stream used by consumer
│  └─ ...
├─ streaming/
│  ├─ producer.py              # Sends events into stream_events.csv
│  ├─ consumer.py              # Reads stream_events.csv and prints live metrics
│  └─ ...
├─ Dockerfile.api              # Docker image for API service
├─ Dockerfile.ui               # Docker image for UI service
├─ docker-compose.yml          # Multi-container (API + UI) definition
├─ requirements.txt            # Python dependencies
├─ README.md                   # Project documentation (this file)
└─ .github/
   └─ workflows/
      └─ ci.yml                # GitHub Actions CI (lint/build)
