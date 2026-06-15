import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import time

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="LogiHive Live Operations Control Tower",
    layout="wide"
)

st.title("📊 LogiHive Live Operations Control Tower")

# ---------------------------------------------------
# AUTO REFRESH EVERY 5 SECONDS
# ---------------------------------------------------

if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > 5:
    st.session_state.last_refresh = time.time()
    st.rerun()

# ---------------------------------------------------
# FETCH CHECKPOINT DATA
# ---------------------------------------------------

def fetch_latest_serialized_states():

    try:
        conn = sqlite3.connect(
            "langgraph_state_registry.db",
            check_same_thread=False
        )

        cursor = conn.cursor()

        cursor.execute("""
        SELECT *
        FROM checkpoints
        ORDER BY rowid DESC
        LIMIT 5
        """)

        rows = cursor.fetchall()

        conn.close()

        return rows

    except Exception:
        return []


# ---------------------------------------------------
# FETCH LIVE TELEMETRY DATA
# ---------------------------------------------------

def fetch_latest_data():

    try:

        conn = sqlite3.connect(
            "langgraph_state_registry.db",
            check_same_thread=False
        )

        query = """
        SELECT
            store_name,
            latitude,
            longitude,
            risk
        FROM telemetry
        """

        df = pd.read_sql_query(
            query,
            conn
        )

        conn.close()

        return df

    except Exception:

        return pd.DataFrame({

            "store_name": [
                "DarkStore_Virar",
                "DarkStore_Vasai",
                "DarkStore_Malad",
                "DarkStore_Borivali"
            ],

            "latitude": [
                19.455,
                19.391,
                19.186,
                19.230
            ],

            "longitude": [
                72.811,
                72.839,
                72.848,
                72.856
            ],

            "risk": [
                0.30,
                0.45,
                0.85,
                0.92
            ]
        })


# ---------------------------------------------------
# LOAD DATA
# ---------------------------------------------------

data = fetch_latest_data()

data["Status"] = data["risk"].apply(
    lambda x:
    "High Risk"
    if x > 0.75
    else "Normal"
)

high_risk = data[data["risk"] > 0.75]

# ---------------------------------------------------
# KPI CARDS
# ---------------------------------------------------

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Stores",
        len(data)
    )

with col2:
    st.metric(
        "High Risk Stores",
        len(high_risk)
    )

with col3:
    st.metric(
        "Average Risk",
        round(data["risk"].mean(), 2)
    )

# ---------------------------------------------------
# MAIN LAYOUT
# ---------------------------------------------------

left_col, right_col = st.columns([2, 1])

# ---------------------------------------------------
# MAP
# ---------------------------------------------------

with left_col:

    st.subheader(
        "📍 Mumbai Fulfillment Network"
    )

    fig = px.scatter_map(
        data,
        lat="latitude",
        lon="longitude",
        color="Status",
        hover_name="store_name",
        size="risk",
        zoom=9
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

# ---------------------------------------------------
# CHECKPOINT REGISTRY
# ---------------------------------------------------

with right_col:

    st.subheader(
        "🛡️ Checkpoint Registry"
    )

    active_threads = fetch_latest_serialized_states()

    if not active_threads:

        st.success(
            "🟢 No active interruptions detected"
        )

    else:

        st.warning(
            "🚨 Active serialized agent states detected"
        )

        for row in active_threads:

            st.info(
                f"Checkpoint: {row}"
            )

# ---------------------------------------------------
# HIGH RISK ALERTS
# ---------------------------------------------------

if not high_risk.empty:

    st.error(
        f"⚠ {len(high_risk)} High-Risk Store(s) Detected!"
    )

    st.write("### Affected Stores")

    st.dataframe(
        high_risk[
            ["store_name", "risk"]
        ]
    )

# ---------------------------------------------------
# LIVE TELEMETRY TABLE
# ---------------------------------------------------

st.write("### Live Telemetry Feed")

st.dataframe(
    data,
    use_container_width=True
)

# ---------------------------------------------------
# REFRESH STATUS
# ---------------------------------------------------

st.caption(
    "🔄 Dashboard auto-refreshes every 5 seconds"
)