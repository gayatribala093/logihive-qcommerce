# ui/dashboard.py
import streamlit as st
import plotly.graph_objects as go
import sqlite3
import json
import httpx
import asyncio

st.set_page_config(layout="wide", page_title="LogiHive Live Operations Control Tower")
st.title("📊 LogiTwin Hyperlocal Fulfillment Resilience Map Dashboard")

# 💎 CORE CONFIGURATION MARKER: Kashish updates this string with Gayatri's local LAN Wi-Fi IP address
GAYATRI_HOST_IP = "127.0.0.1"  # e.g., "192.168.1.45"
INGESTION_GATEWAY_URL = f"http://{GAYATRI_HOST_IP}:8000/api/disruption"

def fetch_latest_serialized_states():
    try:
        # Read directly from the checkpointer file rows to check serialized session logs
        conn = sqlite3.connect("langgraph_state_registry.db", check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT thread_id, checkpoint FROM checkpoints ORDER BY created_at DESC LIMIT 5")
        rows = cursor.fetchall()
        conn.close()
        
        parsed_threads = []
        for row in rows:
            thread_id, raw_checkpoint = row[0], row[1]
            checkpoint_data = json.loads(raw_checkpoint)
            channel_values = checkpoint_data.get("v", {}).get("channel_values", {})
            
            parsed_threads.append({
                "thread_id": thread_id,
                "risk_score": channel_values.get("calculated_risk_tensor", 0.0),
                "approved": channel_values.get("human_approved", False),
                "logs": channel_values.get("execution_timeline_logs", [])
            })
        return parsed_threads
    except Exception:
        return []

active_system_threads = fetch_latest_serialized_states()

# Real-Time Geospatial Map Layers
mumbai_lats = [19.0760, 19.1254, 19.1162, 19.0544, 19.1170]
mumbai_lons = [72.8777, 72.9100, 72.8562, 72.8294, 72.9056]
node_labels = ["Mother-WH Amul", "Mother-WH HUL", "DS Andheri East", "DS Bandra West", "DS Powai"]
node_colors = ['green', 'green', 'green', 'green', 'green']

active_breaches = [t for t in active_system_threads if t["risk_score"] > 0.75 and not t["approved"]]
if active_breaches:
    node_colors[2] = 'red'  # Dynamic UI trigger color switch to flashing red alert markers
    node_colors[4] = 'orange'

geo_fig = go.Figure(go.Scattermapbox(
    lat=mumbai_lats, lon=mumbai_lons, mode='markers+text',
    marker=go.scattermapbox.Marker(size=15, color=node_colors),
    text=node_labels, textposition="top center"
))
geo_fig.update_layout(
    mapbox_style="carto-positron", mapbox_zoom=10.5,
    mapbox_center={"lat": 19.0900, "lon": 72.8700}, margin={"r":0,"t":0,"l":0,"b":0}, height=550
)

left_pane, right_pane = st.columns([2, 1])

with left_pane:
    st.subheader("📍 Mumbai Supply Chain Topology Asset Map")
    st.plotly_chart(geo_fig, use_container_width=True)

with right_pane:
    st.subheader("🛡️ Human-In-The-Loop (HITL) Operations Gateway")
    
    if not active_breaches:
        st.success("🟢 System Registry Stable: Core pipelines running smoothly with zero active exceptions.")
        
        st.write("---")
        st.markdown("### 🧪 Simulate Emergency Anomaly")
        if st.button("Trigger Live Mumbai Flooding Scenario Payload"):
            try:
                mock_alert = {
                    "alert_id": "mumbai_flood_thread_2026",
                    "location_zone": "Andheri East",
                    "alert_text": "Monsoon waters flooding warehouse bays. Operations suspended."
                }
                res = httpx.post(INGESTION_GATEWAY_URL, json=mock_alert, timeout=5.0)
                if res.status_code == 200:
                    st.toast("Alert payload pushed to Ingestion Redis Queue!", icon="🚀")
                    st.experimental_rerun()
            except Exception as e:
                st.error(f"Failed to connect to backend engine: {str(e)}")
    else:
        st.error("🚨 ALERT: Security Boundary Intercepted. Operations halted at serialization line.")
        for breach in active_breaches:
            with st.container(border=True):
                st.markdown(f"**Thread ID:** `{breach['thread_id']}`")
                st.markdown(f"**Risk Evaluation Variable:** :red[{breach['risk_score']}]")
                
                if st.button("Approve Autonomous Reroute Plan", key=f"release_{breach['thread_id']}", use_container_width=True):
                    try:
                        webhook_payload = {
                            "thread_id": breach["thread_id"],
                            "supervisor_token": "LOGIHIVE_SECURE_AUTH_2026"
                        }
                        res = httpx.post(f"http://{GAYATRI_HOST_IP}:8000/api/agent/resume", json=webhook_payload, timeout=10.0)
                        if res.status_code == 200:
                            st.success("🔒 Authorization Token Accepted. State Machine Unblocked!")
                            st.experimental_rerun()
                    except Exception as network_err:
                        st.error(f"Network Connection Dropped: {str(network_err)}")