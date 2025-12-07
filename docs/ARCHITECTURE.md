\# AOHI Architecture



1\. \*\*Data Layer\*\*

&nbsp;  - CSV files: transactions, web\_traffic, system\_metrics, crm\_events

&nbsp;  - Synthetic anomalies for failures, latency, revenue drop, regional outages



2\. \*\*Streaming Layer\*\*

&nbsp;  - producer.py → writes live events into runtime/ files

&nbsp;  - consumer.py → reads and prints live metrics (latency, failures, p95)



3\. \*\*Detection Engine (Backend)\*\*

&nbsp;  - FastAPI app (`api/fastapi\_app.py`)

&nbsp;  - Detectors:

&nbsp;    - EWMA (detectors/ewma.py)

&nbsp;    - Geo failures (detectors/geo.py)

&nbsp;    - Latency spikes (detectors/latency.py)

&nbsp;    - Revenue drop (detectors/revenue.py)

&nbsp;    - Seasonal Z-Score (detectors/seasonal\_zscore.py)



4\. \*\*RCA + Recommendations\*\*

&nbsp;  - RCA logic combines detector outputs and returns root cause + confidence + recommendation.



5\. \*\*Reporting\*\*

&nbsp;  - `api/generate\_report\_pro.py` → builds professional PDF with incidents + RCA.



6\. \*\*Dashboard\*\*

&nbsp;  - Streamlit (`ui/dashboard.py`) → calls API, shows health, incidents, RCA, and triggers report.



7\. \*\*Deployment\*\*

&nbsp;  - Docker (Dockerfile.api, Dockerfile.ui, docker-compose.yml)

&nbsp;  - GitHub Actions CI (tests + lint).



