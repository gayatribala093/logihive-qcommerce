# ui/dashboard.py
import streamlit as st
import plotly.graph_objects as go
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.graph import create_compiled_app
from interfaces import DisruptionAlert

# 1. Page Configuration & Title Clean-up
st.set_page_config(layout="wide", page_title="LogiHive Control Tower")
st.title("LogiHive Q-Commerce: An Event-Driven Agentic Digital Twin for Hyperlocal Supply Resilience and Macroeconomic Risk Mitigation")

# Persist the LangGraph compilation application context state inside Streamlit session memory
if "app_compiled" not in st.session_state:
    st.session_state.app_compiled = create_compiled_app()

THREAD_ID = "mumbai_flood_2026"
config = {"configurable": {"thread_id": THREAD_ID}}

# Spatial Coordinate Vectors for Mumbai Micro-Fulfillment Layout
nodes_lat = [19.076, 19.125, 19.116, 19.054, 19.117]
nodes_lon = [72.877, 72.910, 72.856, 72.829, 72.905]
nodes_names = ["Mother-WH Amul (Root)", "Mother-WH HUL (Root)", "DS Andheri East (Leaf)", "DS Bandra West (Leaf)", "DS Powai (Leaf)"]

# Dynamic state tracking
try:
    current_state = st.session_state.app_compiled.get_state(config)
except Exception:
    current_state = None

is_paused = len(current_state.next) > 0 if current_state else False
has_finished = (current_state.values.get("plan_proposed") == True) if current_state and current_state.values else False

# 2. Network Topology & Vector Routing Logic Mapping
fig = go.Figure()

if is_paused:
    # Highlight disrupted node and compromised baseline channels
    node_colors = ['#10b981', '#10b981', '#ef4444', '#10b981', '#f59e0b']  # Andheri East: Red, Powai: Orange
    node_sizes = [14, 14, 24, 14, 18]
    
    # Standard baseline open vectors
    fig.add_trace(go.Scattermapbox(lat=[19.125, 19.117], lon=[72.910, 72.905], mode='lines', line=dict(width=2, color='blue'), hoverinfo='none'))
    # FIX: Replaced 'dash' property with a thick transparent crimson alert line to fix Scattermapbox crash
    fig.add_trace(go.Scattermapbox(lat=[19.076, 19.116], lon=[72.877, 72.856], mode='lines', line=dict(width=5, color='rgba(239, 68, 68, 0.7)'), hoverinfo='none'))
    
elif has_finished:
    # Optimization committed: Draw alternative logistics route vector
    node_colors = ['#10b981', '#10b981', '#10b981', '#10b981', '#10b981']
    node_sizes = [14, 14, 14, 14, 14]
    
    # Active detour route trace
    fig.add_trace(go.Scattermapbox(lat=[19.125, 19.116], lon=[72.910, 72.856], mode='lines', line=dict(width=4, color='green'), name='Optimized Route'))
else:
    # Baseline standard operation lines
    node_colors = ['#10b981', '#10b981', '#10b981', '#10b981', '#10b981']
    node_sizes = [14, 14, 14, 14, 14]
    fig.add_trace(go.Scattermapbox(lat=[19.076, 19.116], lon=[72.877, 72.856], mode='lines', line=dict(width=2, color='blue'), hoverinfo='none'))

# Place coordinates pins array onto map canvas
fig.add_trace(go.Scattermapbox(
    lat=nodes_lat, lon=nodes_lon, mode='markers+text',
    marker=go.scattermapbox.Marker(size=node_sizes, color=node_colors),
    text=nodes_names, textposition="top center"
))

fig.update_layout(
    mapbox_style="carto-positron", mapbox_zoom=10.5,
    mapbox_center={"lat": 19.076, "lon": 72.877}, margin={"r":0,"t":0,"l":0,"b":0},
    showlegend=False
)

# 3. Layout Structure Workspace
left_col, right_col = st.columns([2, 1])
with left_col:
    st.subheader("Mumbai Supply Chain Network Visualizer Model")
    st.plotly_chart(fig, use_container_width=True)

with right_col:
    st.subheader("Human-In-The-Loop (HITL) Operations Console")
    
    if st.button("🔴 Trigger Live Mumbai Flooding Scenario Payload", use_container_width=True, disabled=is_paused):
        alert = DisruptionAlert(
            alert_id="ALT-99", 
            location_zone="Andheri East", 
            alert_text="Severe waterlogging and traffic gridlock reported near Andheri East entries."
        )
        initial_state = {"alert_context": alert, "human_approved": False, "plan_proposed": False}
        st.session_state.app_compiled.invoke(initial_state, config=config)
        st.rerun()

    st.markdown("---")
    
    if is_paused:
        st.warning("⚠️ SYSTEM BREAKPOINT ENGAGED: Multi-agent execution paused at Human Gatekeeper.")
        
        st.info(f"""
        **🤖 Local Model Analysis Metrics:**
        * **Computed Disruption Index:** `{current_state.values.get('risk_score')}`
        * **Incident Location:** `DS Andheri East Nodes`
        
        **🗺️ Computed Optimization Path:**
        * **Compromised Axis:** `Mother-WH Amul ➔ DS Andheri East` (Blocked via flood simulation)
        * **Detour Algorithm:** `Dijkstra's Shortest Path Engine`
        * **Proposed Reroute:** `Mother-WH HUL ➔ Western Express Corridor ➔ DS Andheri East`
        """)
        
        if st.button("✅ Authorize & Deploy Optimized Reroute Matrix", type="primary", use_container_width=True):
            st.session_state.app_compiled.update_state(config, {"human_approved": True}, as_node="analyst_agent")
            st.session_state.app_compiled.invoke(None, config=config)
            st.success("Authorization token committed. Advancing multi-agent parameters.")
            st.rerun()
            
    elif has_finished:
        st.success("🎉 CORE PIPELINE CLEAR: Alternate logistics routes committed.")
        st.subheader("📜 Architectural Audit Ledger")
        st.json(current_state.values.get("history", []))
        
        if st.button("🔄 Reset Environment for Evaluation", use_container_width=True):
            del st.session_state.app_compiled
            st.rerun()
    else:
        st.info("🟢 System status standard. Ingesting live quick-commerce telemetry tracking matrices.")