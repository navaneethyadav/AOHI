# ui/streamlit_app.py
"""
AOHI Dashboard — Interactive Plotly + Incident Drill-Down + PNG/CSV export
Usage:
  from project root (venv active):
    streamlit run ui/streamlit_app.py

Requires:
  pip install plotly kaleido
"""

# Ensure repo root is on sys.path so local imports work
import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

API_BASE = "http://127.0.0.1:8000"

st.set_page_config(page_title="AOHI Dashboard (Interactive)", layout="wide")
st.title("AOHI — Adaptive Operational Health Intelligence (Interactive)")

st.sidebar.header("Controls")
if st.sidebar.button("Refresh incidents"):
    st.experimental_rerun()
st.sidebar.markdown("Local demo — reads data/*.csv and calls API detectors")
st.sidebar.markdown("If API is down, the UI will fallback to empty detector outputs.")

# ---------------- Helpers / Loaders ----------------
@st.cache_data(ttl=15)
def fetch_incidents():
    try:
        r = requests.get(f"{API_BASE}/incidents", timeout=6)
        r.raise_for_status()
        return r.json().get("incidents", [])
    except Exception:
        return []

@st.cache_data(ttl=15)
def fetch_extra_detectors():
    try:
        r = requests.get(f"{API_BASE}/run_extra_detectors", timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        # fallback: empty outputs so UI doesn't crash
        return {"latency": [], "revenue": [], "geo": []}

@st.cache_data(ttl=30)
def load_transactions(path="data/transactions.csv"):
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    df["bucket_5min"] = df["timestamp"].dt.floor("5min")
    df["bucket_1h"] = df["timestamp"].dt.floor("1h")
    return df

@st.cache_data(ttl=30)
def load_system_metrics(path="data/system_metrics.csv"):
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    df["bucket_5min"] = df["timestamp"].dt.floor("5min")
    return df

# ---------------- Load data & detectors ----------------
incidents = fetch_incidents()
extra = fetch_extra_detectors()
tx = load_transactions()
sysm = load_system_metrics()

# Build anomaly set (minute precision) for chart highlighting
anomaly_buckets = set()
for inc in incidents:
    b = inc.get("incident_bucket")
    if b:
        try:
            anomaly_buckets.add(pd.to_datetime(b).floor("min").strftime("%Y-%m-%d %H:%M"))
        except Exception:
            pass

# ---------------- Top panels ----------------
col_main, col_stats, col_det = st.columns([3, 1, 1])
with col_main:
    st.subheader("Recent incidents (API)")
    if not incidents:
        st.info("No incidents returned by API.")
    else:
        for inc in incidents:
            detected = ", ".join(inc.get("detected_by", []))
            st.markdown(f"- **{inc.get('incident_bucket')}** — detected by: {detected}")

with col_stats:
    st.subheader("Quick stats")
    total_failed = int(((tx["status"] != "success").sum()) if not tx.empty else 0)
    st.metric("Total failed (dataset)", total_failed)
    st.metric("Detected incidents (API)", len(incidents))

with col_det:
    st.subheader("Extra detectors")
    st.write(f"Latency anomalies: {len(extra.get('latency', []))}")
    st.write(f"Revenue drops: {len(extra.get('revenue', []))}")
    st.write(f"Geo failures: {len(extra.get('geo', []))}")

st.write("---")

# ---------------- Latency chart (Plotly) ----------------
st.subheader("Latency (median per 5-min)")

if sysm.empty:
    st.info("No system metrics found (`data/system_metrics.csv`).")
    fig_lat = None
else:
    lat = sysm.groupby("bucket_5min")["latency_ms"].median().reset_index().sort_values("bucket_5min")
    # time range slider
    min_lat = lat["bucket_5min"].min().to_pydatetime()
    max_lat = lat["bucket_5min"].max().to_pydatetime()
    start_lat, end_lat = st.slider("Latency time range", value=(min_lat, max_lat), format="YYYY-MM-DD HH:mm", min_value=min_lat, max_value=max_lat, key="lat_range")
    mask_lat = (lat["bucket_5min"] >= pd.to_datetime(start_lat)) & (lat["bucket_5min"] <= pd.to_datetime(end_lat))
    lat_plot = lat.loc[mask_lat]

    if lat_plot.empty:
        st.info("No latency points in selected range.")
        fig_lat = None
    else:
        fig_lat = px.line(lat_plot, x="bucket_5min", y="latency_ms", title="Median latency (5-min)")
        # add shaded rectangles for incident buckets
        for ab in lat_plot["bucket_5min"].dt.strftime("%Y-%m-%d %H:%M"):
            if ab in anomaly_buckets:
                t = pd.to_datetime(ab)
                fig_lat.add_vrect(x0=t - pd.Timedelta(minutes=2.5), x1=t + pd.Timedelta(minutes=2.5), fillcolor="red", opacity=0.12, line_width=0)
        st.plotly_chart(fig_lat, use_container_width=True)

        # PNG download (requires kaleido)
        try:
            img_bytes = fig_lat.to_image(format="png", width=1200, height=400, scale=2)
            st.download_button("Download latency PNG", data=img_bytes, file_name="aohi_latency.png", mime="image/png")
        except Exception:
            st.warning("PNG export for latency requires 'kaleido' (pip install kaleido).")

# ---------------- Revenue chart (Plotly) ----------------
st.subheader("Revenue (hourly)")

if tx.empty:
    st.info("No transactions found (`data/transactions.csv`).")
    fig_rev = None
else:
    revenue = tx[tx["status"] == "success"].groupby("bucket_1h")["amount"].sum().reset_index().sort_values("bucket_1h")
    if revenue.empty:
        st.info("No revenue data to plot.")
        fig_rev = None
    else:
        min_rev = revenue["bucket_1h"].min().to_pydatetime()
        max_rev = revenue["bucket_1h"].max().to_pydatetime()
        start_rev, end_rev = st.slider("Revenue time range", value=(min_rev, max_rev), format="YYYY-MM-DD HH:mm", min_value=min_rev, max_value=max_rev, key="rev_range")
        mask_rev = (revenue["bucket_1h"] >= pd.to_datetime(start_rev)) & (revenue["bucket_1h"] <= pd.to_datetime(end_rev))
        rev_plot = revenue.loc[mask_rev]
        fig_rev = px.bar(rev_plot, x="bucket_1h", y="amount", title="Revenue per hour", labels={"amount": "Revenue"})
        # annotate revenue drops from detector
        for rd in extra.get("revenue", []):
            try:
                t = pd.to_datetime(rd.get("timestamp")).floor("h")
                if (t >= pd.to_datetime(start_rev)) and (t <= pd.to_datetime(end_rev)):
                    fig_rev.add_vline(x=t, line_dash="dash", annotation_text="Revenue drop", annotation_position="top right")
            except Exception:
                pass
        st.plotly_chart(fig_rev, use_container_width=True)

        # PNG download (requires kaleido)
        try:
            img_bytes = fig_rev.to_image(format="png", width=1200, height=400, scale=2)
            st.download_button("Download revenue PNG", data=img_bytes, file_name="aohi_revenue.png", mime="image/png")
        except Exception:
            st.warning("PNG export for revenue requires 'kaleido' (pip install kaleido).")

# ---------------- Geo failures chart ----------------
st.subheader("Geo failures (by country)")

geo_out = extra.get("geo", [])
fig_geo = None
if not geo_out:
    st.info("No geo failures detected by API.")
else:
    gdf = pd.DataFrame(geo_out)
    if not gdf.empty:
        gdf["timestamp"] = pd.to_datetime(gdf["timestamp"])
        agg_country = gdf.groupby("country")["failed_count"].sum().reset_index().sort_values("failed_count", ascending=False)
        fig_geo = px.bar(agg_country, x="country", y="failed_count", title="Failed transactions by country")
        st.plotly_chart(fig_geo, use_container_width=True)
        st.dataframe(gdf.sort_values(["timestamp", "country"]).reset_index(drop=True).head(200))

        # PNG download (requires kaleido)
        try:
            img_bytes = fig_geo.to_image(format="png", width=1000, height=400, scale=2)
            st.download_button("Download geo PNG", data=img_bytes, file_name="aohi_geo.png", mime="image/png")
        except Exception:
            st.warning("PNG export for geo requires 'kaleido' (pip install kaleido).")
    else:
        st.info("Geo detector returned no rows.")

st.write("---")

# ---------------- Incident Drill-down ----------------
st.subheader("Incident drill-down")
if not incidents:
    st.info("No incidents to inspect.")
else:
    # incident selector
    idx = st.selectbox("Select incident", options=list(range(len(incidents))), format_func=lambda i: incidents[i]["incident_bucket"])
    sel = incidents[idx]
    st.markdown(f"## Incident: {sel.get('incident_bucket')}")
    st.markdown("**Detected by:** " + ", ".join(sel.get("detected_by", [])))
    if sel.get("root_causes"):
        st.markdown("**Root causes & evidence**")
        for rc in sel.get("root_causes", []):
            st.markdown(f"- **{rc.get('root_cause')}** — {rc.get('description')}")
            st.write("Evidence:", rc.get("evidence"))
    else:
        st.markdown("_No root causes matched by rules._")

    if sel.get("playbooks"):
        st.markdown("**Suggested playbooks**")
        for pb in sel.get("playbooks", []):
            st.write(f"- **{pb.get('priority','')}** | {pb.get('owner','')} : {pb.get('steps','')}")

    # show transactions in 5-min bucket
    try:
        bucket_time = pd.to_datetime(sel.get("incident_bucket")).floor("5min")
        st.markdown(f"**Transactions in {bucket_time} (5-min window)**")
        if tx.empty:
            st.info("No transactions CSV loaded.")
        else:
            mask_tx = (tx["bucket_5min"] >= bucket_time) & (tx["bucket_5min"] < bucket_time + pd.Timedelta(minutes=5))
            tx_subset = tx.loc[mask_tx].sort_values("timestamp")
            if tx_subset.empty:
                st.write("No transactions in this bucket.")
            else:
                st.dataframe(tx_subset.reset_index(drop=True))
                st.write("Aggregate:", "total=", len(tx_subset), "failed=", int((tx_subset["status"]!="success").sum()), "sum_amount=", float(tx_subset["amount"].sum()))

                # CSV download for this incident's transactions
                csv_bytes = tx_subset.to_csv(index=False).encode("utf-8")
                filename = f"transactions_{bucket_time.strftime('%Y%m%d_%H%M')}.csv"
                st.download_button("Download transactions CSV for this incident", data=csv_bytes, file_name=filename, mime="text/csv")
    except Exception as e:
        st.error(f"Error showing transactions for bucket: {e}")

st.write("---")
st.info("Interactive charts and drill-down ready — use the selector to inspect incidents, evidence and playbooks.")
