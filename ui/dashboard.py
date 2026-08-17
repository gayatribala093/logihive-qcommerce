"""
ui/dashboard.py
LogiHive Q-Commerce :: Hyperlocal Operations Control Tower
--------------------------------------------------------------
Streamlit front-end for the digital twin + multi-agent engine.

This is a thin client: it never imports core.twin or agents.graph
directly. It talks to the FastAPI gateway (core/ingestion.py) over
HTTP, exactly like any other operator tool would, so the dashboard can
be deployed/scaled independently of the agentic backend.

Run with:
    streamlit run ui/dashboard.py -- --api http://localhost:8000
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from typing import Any

import httpx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

DEFAULT_API_BASE = "http://localhost:8000"
REQUEST_TIMEOUT_S = 120.0
AUTO_REFRESH_SECONDS = 8

TIER_COLORS = {
    "mother_warehouse": "#7C3AED",   # violet
    "regional_hub": "#2563EB",       # blue
    "dark_store": "#059669",         # emerald
}
TIER_SIZES = {
    "mother_warehouse": 22,
    "regional_hub": 16,
    "dark_store": 13,
}
TIER_LABELS = {
    "mother_warehouse": "Mother Warehouse",
    "regional_hub": "Regional Hub",
    "dark_store": "Dark Store",
}

MUMBAI_CENTER = {"lat": 19.09, "lon": 72.90}


def _api_base() -> str:
    if "--api" in sys.argv:
        idx = sys.argv.index("--api")
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return st.session_state.get("api_base", DEFAULT_API_BASE)


# --------------------------------------------------------------------------
# Page setup
# --------------------------------------------------------------------------

st.set_page_config(
    page_title="LogiHive | Hyperlocal Operations Control Tower",
    page_icon="⚡",
    layout="wide",
)

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.6rem; }
        .lh-status-ok { background:#ECFDF5; border:1px solid #A7F3D0; color:#065F46;
            padding:0.65rem 0.9rem; border-radius:10px; font-size:0.92rem; }
        .lh-status-warn { background:#FFFBEB; border:1px solid #FDE68A; color:#92400E;
            padding:0.65rem 0.9rem; border-radius:10px; font-size:0.92rem; }
        .lh-status-bad { background:#FEF2F2; border:1px solid #FECACA; color:#991B1B;
            padding:0.65rem 0.9rem; border-radius:10px; font-size:0.92rem; }
        .lh-card { background:#F8FAFC; border:1px solid #E2E8F0; border-radius:12px;
            padding:0.9rem 1.05rem; margin-bottom:0.7rem; }
        .lh-metric-label { color:#64748B; font-size:0.78rem; text-transform:uppercase;
            letter-spacing:0.04em; }
        .lh-pill { display:inline-block; padding:0.15rem 0.55rem; border-radius:999px;
            font-size:0.75rem; font-weight:600; }
        .lh-pill-low { background:#DCFCE7; color:#166534; }
        .lh-pill-med { background:#FEF9C3; color:#854D0E; }
        .lh-pill-high { background:#FEE2E2; color:#991B1B; }
    </style>
    """,
    unsafe_allow_html=True,
)

if "last_plan" not in st.session_state:
    st.session_state.last_plan = None
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = 0.0


# --------------------------------------------------------------------------
# API client helpers — every call is defensive: the dashboard must stay
# usable (with a clear degraded-state banner) even if the backend, Redis,
# or Ollama is down.
# --------------------------------------------------------------------------

def _get(path: str) -> dict[str, Any] | None:
    try:
        resp = httpx.get(f"{_api_base()}{path}", timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        # Surface the FastAPI JSON `detail` field (which now carries the
        # real exception type/message) instead of httpx's generic
        # "Server error '502 Bad Gateway'" summary.
        try:
            detail = exc.response.json().get("detail", exc.response.text)
        except Exception:
            detail = exc.response.text
        st.session_state["api_error"] = f"HTTP {exc.response.status_code}: {detail}"
        return None
    except Exception as exc:  # noqa: BLE001
        st.session_state["api_error"] = str(exc)
        return None


def _post(path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    try:
        resp = httpx.post(f"{_api_base()}{path}", json=payload, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        try:
            detail = exc.response.json().get("detail", exc.response.text)
        except Exception:
            detail = exc.response.text
        st.session_state["api_error"] = f"HTTP {exc.response.status_code}: {detail}"
        return None
    except Exception as exc:  # noqa: BLE001
        st.session_state["api_error"] = str(exc)
        return None


@st.cache_data(ttl=AUTO_REFRESH_SECONDS, show_spinner=False)
def fetch_twin_snapshot(api_base: str, _bust: float) -> dict[str, Any] | None:
    return _get("/api/twin/snapshot")


def fetch_pending_hitl() -> list[dict[str, Any]]:
    data = _get("/api/hitl/pending")
    return data.get("pending", []) if data else []


def trigger_scenario(scenario_payload: dict[str, Any]) -> dict[str, Any] | None:
    return _post("/api/alert", scenario_payload)


def submit_hitl_decision(thread_id: str, decision: str) -> dict[str, Any] | None:
    return _post("/api/hitl/decision", {"thread_id": thread_id, "decision": decision})


def risk_pill(score: float) -> str:
    if score >= 0.75:
        return f'<span class="lh-pill lh-pill-high">risk {score:.2f}</span>'
    if score >= 0.4:
        return f'<span class="lh-pill lh-pill-med">risk {score:.2f}</span>'
    return f'<span class="lh-pill lh-pill-low">risk {score:.2f}</span>'


def edge_color(risk: float) -> str:
    if risk >= 0.75:
        return "#DC2626"
    if risk >= 0.4:
        return "#D97706"
    return "#3B82F6"


# --------------------------------------------------------------------------
# Header
# --------------------------------------------------------------------------

header_l, header_r = st.columns([0.72, 0.28])
with header_l:
    st.markdown("## ⚡ LogiHive Hyperlocal Operations Control Tower")
    st.caption("Event-driven agentic digital twin for quick-commerce supply resilience — Mumbai network")
with header_r:
    st.text_input("Backend API", value=_api_base(), key="api_base", label_visibility="collapsed")
    st.caption(f"Target: `{_api_base()}`")

st.session_state.pop("api_error", None)


# --------------------------------------------------------------------------
# Fetch live state
# --------------------------------------------------------------------------

refresh_bucket = int(time.time() // AUTO_REFRESH_SECONDS)
snapshot = fetch_twin_snapshot(_api_base(), refresh_bucket)
pending_hitl = fetch_pending_hitl()

nodes = snapshot.get("nodes", []) if snapshot else []
edges = snapshot.get("edges", []) if snapshot else []

if not snapshot:
    st.markdown(
        f'<div class="lh-status-bad">🔴 Cannot reach LogiHive backend at '
        f'<code>{_api_base()}</code>. Showing empty state — start '
        f'<code>uvicorn core.ingestion:app</code> and refresh.</div>',
        unsafe_allow_html=True,
    )
elif any(e.get("disruption_risk", 0) >= 0.75 for e in edges):
    st.markdown(
        '<div class="lh-status-bad">🔴 Critical corridor risk detected — one or more routes '
        'exceed the 0.75 disruption threshold and are pending human review.</div>',
        unsafe_allow_html=True,
    )
elif any(e.get("disruption_risk", 0) >= 0.4 for e in edges):
    st.markdown(
        '<div class="lh-status-warn">🟡 Elevated risk on parts of the network — '
        'monitoring active corridors.</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="lh-status-ok">🟢 System status standard. Ingesting live '
        'quick-commerce telemetry and disruption feeds.</div>',
        unsafe_allow_html=True,
    )

st.write("")


# --------------------------------------------------------------------------
# Top-line metrics
# --------------------------------------------------------------------------

m1, m2, m3, m4, m5 = st.columns(5)
dark_stores = [n for n in nodes if n.get("tier") == "dark_store"]
avg_risk = (sum(e.get("disruption_risk", 0) for e in edges) / len(edges)) if edges else 0.0
at_risk_edges = [e for e in edges if e.get("disruption_risk", 0) >= 0.4]
total_inventory = sum(n.get("inventory_units", 0) for n in dark_stores)

m1.metric("Nodes tracked", len(nodes))
m2.metric("Active corridors", len(edges))
m3.metric("Avg. corridor risk", f"{avg_risk:.2f}")
m4.metric("Corridors ⚠️", len(at_risk_edges))
m5.metric("Dark store inventory", f"{total_inventory:,} units")

st.write("")

map_col, panel_col = st.columns([0.62, 0.38], gap="large")


# --------------------------------------------------------------------------
# Left: Mumbai supply chain map
# --------------------------------------------------------------------------

with map_col:
    st.markdown("#### 🗺️ Mumbai Dark Store Logistics Network")

    fig = go.Figure()
    node_pos = {n["id"]: (n["lat"], n["lon"]) for n in nodes}

    # Edges — drawn first so node markers sit on top; colored/weighted by risk.
    for edge in edges:
        src, dst = edge.get("source"), edge.get("target")
        if src not in node_pos or dst not in node_pos:
            continue
        lat0, lon0 = node_pos[src]
        lat1, lon1 = node_pos[dst]
        risk = edge.get("disruption_risk", 0.0)
        fig.add_trace(
            go.Scattermapbox(
                lat=[lat0, lat1],
                lon=[lon0, lon1],
                mode="lines",
                line=dict(width=2 + risk * 4, color=edge_color(risk)),
                hoverinfo="text",
                text=f"{src} → {dst}<br>risk: {risk:.2f} | weight: {edge.get('weight', 0):.1f}",
                showlegend=False,
            )
        )

    # Nodes grouped by tier for a clean legend.
    for tier, color in TIER_COLORS.items():
        tier_nodes = [n for n in nodes if n.get("tier") == tier]
        if not tier_nodes:
            continue
        fig.add_trace(
            go.Scattermapbox(
                lat=[n["lat"] for n in tier_nodes],
                lon=[n["lon"] for n in tier_nodes],
                mode="markers+text",
                marker=dict(size=TIER_SIZES[tier], color=color),
                text=[n["id"] for n in tier_nodes],
                textposition="top center",
                hovertext=[
                    f"{n['id']}<br>tier: {tier}<br>inventory: {n.get('inventory_units', 0)} units"
                    for n in tier_nodes
                ],
                hoverinfo="text",
                name=TIER_LABELS[tier],
            )
        )

    fig.update_layout(
        mapbox=dict(style="open-street-map", center=MUMBAI_CENTER, zoom=9.4),
        margin=dict(l=0, r=0, t=0, b=0),
        height=560,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("📦 Dark store inventory"):
        if dark_stores:
            df = pd.DataFrame(
                [
                    {
                        "Dark Store": n["id"],
                        "Inventory (units)": n.get("inventory_units", 0),
                        "Node Risk": n.get("risk_score", 0.0),
                    }
                    for n in dark_stores
                ]
            ).sort_values("Inventory (units)")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("No dark store data available.")


# --------------------------------------------------------------------------
# Right: Threat Scenario Manager + HITL console + agent trace
# --------------------------------------------------------------------------

with panel_col:
    st.markdown("#### 🚨 Threat Scenario Manager")
    with st.container(border=True):
        scenario_area = st.selectbox(
            "Affected area",
            [n["id"] for n in dark_stores] or ["DS_POWAI"],
            key="scenario_area",
        )
        if st.button("🌧️ Trigger Live Mumbai Flooding Scenario Payload", use_container_width=True):
            payload = {
                "alert_id": f"SIM_{int(time.time())}",
                "category": "flood",
                "affected_area": scenario_area,
                "raw_text": (
                    f"Severe waterlogging reported near {scenario_area}; multiple approach "
                    f"roads impassable, riders stranded, ETA delays expected."
                ),
                "reported_at": time.time(),
                "severity_hint": "critical",
            }
            with st.spinner("Dispatching to Analyst Agent…"):
                result = trigger_scenario(payload)
            if result:
                st.session_state.last_plan = result
                st.toast("Scenario dispatched to the agent graph", icon="⚡")
            else:
                st.error(f"Scenario dispatch failed: {st.session_state.get('api_error')}")

        plan = st.session_state.last_plan
        if plan:
            score = plan.get("disruption_risk_score", plan.get("risk_tensor", {}).get("disruption_risk_score", 0))
            route = plan.get("route", {})
            st.markdown(
                f"""
                <div class="lh-card">
                  <div class="lh-metric-label">Last Applied Plan</div>
                  <div style="margin-top:0.25rem;">
                    {risk_pill(float(score))}
                    &nbsp; Rerouted via Dijkstra path:
                    <b>{route.get('source', '—')} → {route.get('target', scenario_area)}</b>
                    (est. {route.get('cost', '—')} min)
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("#### 🧑‍⚖️ Human-in-the-Loop Console")
    with st.container(border=True):
        if not pending_hitl:
            st.caption("No runs currently paused for review.")
        for item in pending_hitl:
            thread_id = item.get("thread_id", "unknown")
            score = item.get("disruption_risk_score", 0.0)
            alert_text = item.get("raw_alert_text", "")
            st.markdown(
                f"""
                <div class="lh-card">
                  <div class="lh-metric-label">Thread {thread_id}</div>
                  {risk_pill(float(score))}
                  <div style="margin-top:0.4rem; font-size:0.9rem; color:#334155;">{alert_text}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            c1, c2 = st.columns(2)
            if c1.button("✅ Approve", key=f"approve_{thread_id}", use_container_width=True):
                submit_hitl_decision(thread_id, "APPROVE")
                st.rerun()
            if c2.button("⛔ Reject", key=f"reject_{thread_id}", use_container_width=True):
                submit_hitl_decision(thread_id, "REJECT")
                st.rerun()

    st.markdown("#### 🧵 Agent Trace")
    with st.expander("Latest run trace", expanded=False):
        trace = (st.session_state.last_plan or {}).get("agent_trace", [])
        if trace:
            for line in trace:
                st.markdown(f"- `{line}`")
        else:
            st.caption("Trigger a scenario to see the agent's step-by-step trace.")


st.caption(
    f"Last refreshed {datetime.now().strftime('%H:%M:%S')} · "
    f"auto-refresh every {AUTO_REFRESH_SECONDS}s on page interaction"
)