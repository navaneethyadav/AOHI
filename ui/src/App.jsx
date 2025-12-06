// ui/src/App.jsx
import { useState } from "react";

export default function App() {
  const [health, setHealth] = useState(null);
  const [incidents, setIncidents] = useState(null);
  const [rca, setRca] = useState(null);
  const [reportMsg, setReportMsg] = useState("");

  const apiBase = "http://127.0.0.1:8000";

  async function checkHealth() {
    setHealth("loading...");
    try {
      const res = await fetch(`${apiBase}/health`);
      const j = await res.json();
      setHealth(j);
    } catch (e) {
      setHealth({ error: e.message });
    }
  }

  async function loadIncidents() {
    setIncidents("loading...");
    try {
      const res = await fetch(`${apiBase}/incidents`);
      const j = await res.json();
      setIncidents(j);
    } catch (e) {
      setIncidents({ error: e.message });
    }
  }

  async function loadRca() {
    setRca("loading...");
    try {
      const res = await fetch(`${apiBase}/rca`);
      const j = await res.json();
      setRca(j);
    } catch (e) {
      setRca({ error: e.message });
    }
  }

  async function genReport() {
    setReportMsg("generating...");
    try {
      // Request and download the PDF using window.open so browser handles download
      const url = `${apiBase}/report_pro?force=true&timeout=60&name=Navaneeth%20Kaku`;
      // open in new tab - browser will download or show PDF
      window.open(url, "_blank");
      setReportMsg("report opened in new tab (or downloaded).");
    } catch (e) {
      setReportMsg("error: " + e.message);
    }
  }

  return (
    <div style={{ padding: 24, fontFamily: "sans-serif" }}>
      <h1>AOHI Dashboard</h1>

      <div style={{ marginBottom: 12 }}>
        <button onClick={checkHealth}>Check API Health</button>{" "}
        <button onClick={loadIncidents}>Load Incidents</button>{" "}
        <button onClick={loadRca}>Load RCA</button>{" "}
        <button onClick={genReport}>Generate Report</button>
      </div>

      <section style={{ marginTop: 16 }}>
        <h2>Health</h2>
        <pre>{health ? JSON.stringify(health, null, 2) : "—"}</pre>
      </section>

      <section style={{ marginTop: 16 }}>
        <h2>Incidents</h2>
        <pre>{incidents ? JSON.stringify(incidents, null, 2) : "—"}</pre>
      </section>

      <section style={{ marginTop: 16 }}>
        <h2>RCA</h2>
        <pre>{rca ? JSON.stringify(rca, null, 2) : "—"}</pre>
      </section>

      <section style={{ marginTop: 16 }}>
        <h2>Report</h2>
        <div>{reportMsg || "—"}</div>
      </section>
    </div>
  );
}