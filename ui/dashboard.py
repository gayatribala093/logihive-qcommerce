# ui/dashboard.py
import streamlit as st
import plotly.graph_objects as go
import sqlite3
import json
import httpx

# =====================================================================
# 🏢 STREAMLIT MULTI-COLUMN LAYOUT CONFIGURATION
# =====================================================================
st.set_page_config(layout="wide", page_title="LogiHive Live Operations Control Tower")
st.title("📊 LogiTwin Hyperlocal Fulfillment Resilience Map Dashboard")

# Define target endpoints for executing live state approvals
INGESTION_GATEWAY_URL = "http://localhost:8000/api/disruption"

# =====================================================================
# 💾 CHECKPOINTER DATABASE STORAGE INTERFACES
# =====================================================================
def fetch_latest_serialized_states():
    """
    Queries checkpoint transactions straight out of Gayatri's persistent 
    SqliteSaver tracking registry database file to expose blocked thread segments.
    """
    try:
        conn = sqlite3.connect("langgraph_state_registry.db", check_same_thread=False)
        cursor = conn.cursor()
        # Query active records generated natively within the LangGraph SqliteSaver engine schema
        cursor.execute("""
            SELECT thread_id, checkpoint 
            FROM checkpoints 
            ORDER BY created_at DESC 
            LIMIT 5
        """)
        rows = cursor.fetchall()
        conn.close()
        
        parsed_threads = []
        for row in rows:
            thread_id, raw_checkpoint = row[0], row[1]
            # De-serialize blobs to check internal risk tensors and approval statuses
            checkpoint_data = json.loads(raw_checkpoint)
            channel_values = checkpoint_data.get("v", {}).get("channel_values", {})
            
            parsed_threads.append({
                "thread_id": thread_id,
                "risk_score": channel_values.get("calculated_risk_tensor", 0.0),
                "approved": channel_values.get("human_approved", False),
                "logs": channel_values.get("execution_timeline_logs", [])
            })
        return parsed_threads
    except Exception as e:
        # Gracefully handle initial boots where the db file hasn't been written to disk yet
        return []

# =====================================================================
# 🗺️ PLOTLY MULTIMODAL GEOSPATIAL MAP GENERATOR
# =====================================================================
def generate_mumbai_asset_map(active_threads):
    """
    Generates a real-time scatter map projection of Mumbai logistics components,
    updating node color vectors instantly if a risk threshold breach occurs.
    """
    # Fixed coordinate map layout pins for hubs across Mumbai
    mumbai_latitudes = [19.0760, 19.1254, 19.1162, 19.0544, 19.1170]
    mumbai_longitudes = [72.8777, 72.9100, 72.8562, 72.8294, 72.9056]
    node_identifiers = ["Mother-WH Amul", "Mother-WH HUL", "DS Andheri East", "DS Bandra West", "DS Powai"]
    
    # Standard healthy color matrix configuration maps green
    node_colors = ['green', 'green', 'green', 'green', 'green']
    
    # If a live thread contains an unapproved critical risk, highlight the dark store node
    for thread in active_threads:
        if thread["risk_score"] > 0.75 and not thread["approved"]:
            # Scenario simulation: Andheri East node goes critical red
            node_colors[2] = 'red'
            node_colors[4] = 'orange' # Powai threshold warning limits triggered

    geo_fig = go.Figure(go.Scattermapbox(
        lat=mumbai_latitudes,
        lon=mumbai_longitudes,
        mode='markers+text',
        marker=go.scattermapbox.Marker(size=15, color=node_colors, opacity=0.9),
        text=node_identifiers,
        textposition="top center",
        hoverinfo="text"
    ))

    geo_fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=10.5,
        mapbox_center={"lat": 19.0900, "lon": 72.8700},
        margin={"r":0,"t":0,"l":0,"b":0},
        height=600
    )
    return geo_fig

# =====================================================================
# 🎛️ CORE INTERFACE RENDERING ENGINE BLOCK
# =====================================================================

# Continuously read active backend states from disk lines
active_system_threads = fetch_latest_serialized_states()

# Render a structural split pane interface matrix layout
left_pane, right_pane = st.columns([2, 1])

with left_pane:
    st.subheader("📍 Mumbai Supply Chain Topology Asset Map")
    mumbai_map = generate_mumbai_asset_map(active_system_threads)
    st.plotly_chart(mumbai_map, use_container_width=True)

with right_pane:
    st.subheader("🛡️ Human-In-The-Loop (HITL) Operations")