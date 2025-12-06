# web/app.py
import streamlit as st
import requests
import json

API_BASE = "http://127.0.0.1:8000"

st.set_page_config(page_title="AOHI Dashboard", layout="wide")
st.title("AOHI Dashboard (Streamlit)")

col1, col2, col3 = st.columns(3)

with col1:
    st.header("API Health")
    try:
        r = requests.get(f"{API_BASE}/health", timeout=5)
        r.raise_for_status()
        st.json(r.json())
    except Exception as e:
        st.error(f"Health check failed: {e}")

with col2:
    st.header("Incidents")
    try:
        r = requests.get(f"{API_BASE}/incidents?force_run=true", timeout=20)
        r.raise_for_status()
        st.json(r.json())
    except Exception as e:
        st.error(f"Failed to fetch incidents: {e}")

with col3:
    st.header("RCA")
    try:
        r = requests.get(f"{API_BASE}/rca", timeout=20)
        r.raise_for_status()
        st.json(r.json())
    except Exception as e:
        st.error(f"Failed to fetch RCA: {e}")

st.markdown("---")
st.subheader("Generate report (PDF)")

name = st.text_input("Report name", "Navaneeth Kaku")
if st.button("Generate report now"):
    try:
        url = f"{API_BASE}/report_pro?force=true&timeout=60&name={requests.utils.quote(name)}"
        r = requests.get(url, stream=True, timeout=120)
        # The API returns file (200) or JSON on error
        if r.headers.get("content-type") == "application/pdf" or r.status_code == 200:
            # save to /data
            out_path = "data/AOHI_FromAPI_streamlit.pdf"
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            st.success(f"Report downloaded to {out_path}")
            st.markdown(f"[Open report file](./{out_path})")
        else:
            # show JSON error
            try:
                st.json(r.json())
            except Exception:
                st.write(r.text)
    except Exception as e:
        st.error(f"Report generation failed: {e}")

st.write("Notes: This UI calls your local API at http://127.0.0.1:8000")
