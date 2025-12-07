import json
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
import streamlit as st


# ----------------------------
# Basic config
# ----------------------------
st.set_page_config(
    page_title="AOHI Dashboard",
    layout="wide",
)

# AOHI API base – we keep it simple and local
API_BASE = "http://127.0.0.1:8000"


# ----------------------------
# Helpers
# ----------------------------

def call_api(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Call AOHI API and return JSON. On error, return an error dict."""
    url = f"{API_BASE}{path}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e), "url": url, "params": params}


def pretty_json_block(label: str, data: Any) -> None:
    """Subheader + pretty JSON code block."""
    st.subheader(label)
    st.code(json.dumps(data, indent=2, default=str), language="json")


def flatten_incidents(incidents_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Turn incidents JSON into a flat DataFrame.

    Columns:
      - detector
      - timestamp
      - failed
      - failed_count
      - zscore (forced to string so Arrow is happy)
      - current_revenue
      - baseline
      - country
    """
    rows: List[Dict[str, Any]] = []

    if not incidents_json:
        return pd.DataFrame()

    for det in incidents_json.get("incidents", []):
        det_name = det.get("detector", "unknown_detector")
        for item in det.get("result", []):
            rows.append(
                {
                    "detector": det_name,
                    "timestamp": item.get("timestamp"),
                    "failed": item.get("failed"),
                    "failed_count": item.get("failed_count"),
                    "zscore": item.get("zscore"),
                    "current_revenue": item.get("current_revenue"),
                    "baseline": item.get("baseline"),
                    "country": item.get("country"),
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Parse timestamp for charts
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # IMPORTANT: force zscore to string, so "inf" + floats don't break Arrow
    if "zscore" in df.columns:
        df["zscore"] = df["zscore"].astype(str)

    return df


def flatten_rca(rca_json: Dict[str, Any]) -> pd.DataFrame:
    """Flatten RCA JSON to a table."""
    if not rca_json:
        return pd.DataFrame()

    results = rca_json.get("results", {}).get("results", [])
    rows = []
    for r in results:
        rows.append(
            {
                "root_cause": r.get("root_cause"),
                "confidence": r.get("confidence"),
                "recommendation": r.get("recommendation"),
                "evidence": json.dumps(r.get("evidence", {}), indent=2),
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# ----------------------------
# Page layout
# ----------------------------

st.title("AOHI Dashboard (minimal)")
st.caption(f"This UI calls your local API at {API_BASE}")


# ============================
# 1️⃣ API HEALTH
# ============================

st.header("API Health")
col_health_btn, col_health_resp = st.columns([1, 3])

with col_health_btn:
    if st.button("Check API Health"):
        health_data = call_api("/health")
        st.session_state["health_data"] = health_data

with col_health_resp:
    health_data = st.session_state.get("health_data")
    if health_data:
        pretty_json_block("Health JSON", health_data)
    else:
        st.info("Click 'Check API Health' to load status.")


# ============================
# 2️⃣ INCIDENTS
# ============================

st.header("Incidents (live)")
col_inc_btns, col_inc_raw = st.columns([1, 3])

with col_inc_btns:
    if st.button("Load Incidents (force_run=True)"):
        incidents_data = call_api("/incidents", params={"force_run": "true"})
        st.session_state["incidents_data"] = incidents_data

with col_inc_raw:
    incidents_data = st.session_state.get("incidents_data")
    if incidents_data:
        pretty_json_block("Raw incidents JSON", incidents_data)
    else:
        st.info("Click 'Load Incidents (force_run=True)' to fetch live incidents.")


incidents_data = st.session_state.get("incidents_data")
if incidents_data:
    df_inc = flatten_incidents(incidents_data)

    if not df_inc.empty:
        st.subheader("Incidents table (flattened)")
        st.dataframe(df_inc, width="stretch")

        col_failures, col_revenue = st.columns(2)

        # Failures chart
        with col_failures:
            st.subheader("Failures over time (all detectors)")
            df_fail = df_inc.copy()
            df_fail["failed_metric"] = df_fail["failed_count"].fillna(df_fail["failed"])
            df_fail = df_fail.dropna(subset=["timestamp", "failed_metric"])
            if not df_fail.empty:
                df_fail_chart = (
                    df_fail.groupby("timestamp")["failed_metric"]
                    .sum()
                    .reset_index()
                    .sort_values("timestamp")
                )
                st.line_chart(
                    df_fail_chart,
                    x="timestamp",
                    y="failed_metric",
                    width="stretch",
                )
            else:
                st.info("No failed transactions to plot.")

        # Revenue chart
        with col_revenue:
            st.subheader("Revenue anomalies")
            df_rev = df_inc.dropna(subset=["current_revenue", "baseline"])
            if not df_rev.empty:
                df_rev_chart = (
                    df_rev[["timestamp", "current_revenue", "baseline"]]
                    .dropna()
                    .sort_values("timestamp")
                )
                st.line_chart(
                    df_rev_chart,
                    x="timestamp",
                    y=["baseline", "current_revenue"],
                    width="stretch",
                )
            else:
                st.info("No revenue anomalies to display.")
    else:
        st.warning("Incidents JSON came back empty after flattening.")


# ============================
# 3️⃣ RCA
# ============================

st.header("RCA")
col_rca_btns, col_rca_raw = st.columns([1, 3])

with col_rca_btns:
    if st.button("Load RCA"):
        rca_data = call_api("/rca")
        st.session_state["rca_data"] = rca_data

with col_rca_raw:
    rca_data = st.session_state.get("rca_data")
    if rca_data:
        pretty_json_block("Raw RCA JSON", rca_data)
    else:
        st.info("Click 'Load RCA' to fetch root cause analysis.")


rca_data = st.session_state.get("rca_data")
if rca_data:
    df_rca = flatten_rca(rca_data)
    if not df_rca.empty:
        st.subheader("RCA Summary (table)")
        st.dataframe(df_rca, width="stretch")

        st.subheader("RCA Summary (human-readable)")
        for idx, row in df_rca.iterrows():
            st.markdown(f"**Root Cause #{idx + 1}:**")
            st.write(f"- **Root cause:** {row['root_cause']}")
            st.write(f"- **Confidence:** {row['confidence']}")
            st.write(f"- **Recommendation:** {row['recommendation']}")
            st.write("- **Evidence:**")
            st.code(row["evidence"], language="json")
    else:
        st.warning("RCA data is empty after flattening.")


# ============================
# 4️⃣ REPORT GENERATION
# ============================

st.header("Generate Report (PDF)")

with st.form("report_form"):
    name = st.text_input("Report name", value="Navaneeth Kaku")
    submitted = st.form_submit_button("Generate Report (PDF)")

if submitted:
    st.info("Requesting report from API...")
    try:
        params = {"timeout": "60", "name": name}
        url = f"{API_BASE}/report_pro"
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()

        pdf_bytes = resp.content
        st.success("Report returned by API (download below).")
        st.download_button(
            label="Download AOHI Report PDF",
            data=pdf_bytes,
            file_name="AOHI_Report.pdf",
            mime="application/pdf",
        )
    except Exception as e:
        st.error(f"Failed to generate report: {e}")

st.caption(
    "If buttons fail: make sure uvicorn (API) is running on the same machine and port 8000."
)
