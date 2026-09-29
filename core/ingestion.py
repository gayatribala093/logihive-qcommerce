"""
LogiHive Q-Commerce :: core/ingestion.py
High-throughput FastAPI gateway handling telemetry ingestion,
disruption routing, and supervisor HITL resumption.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from agents.graph import app_compiled
from core.twin import DIGITAL_TWIN
from interfaces import DarkStoreInventory, DisruptionAlert, RiderTelemetry

app = FastAPI(title="LogiHive Q-Commerce Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ResumePayload(BaseModel):
    thread_id: str
    supervisor_token: str
    decision: str = "APPROVE"


@app.get("/api/twin/snapshot")
async def get_twin_snapshot() -> Dict[str, Any]:
    return DIGITAL_TWIN.get_snapshot()


@app.post("/api/telemetry")
async def ingest_telemetry(payload: RiderTelemetry) -> Dict[str, Any]:
    return {"status": "BUFFERED", "rider_id": payload.rider_id}


@app.post("/api/inventory")
async def ingest_inventory(payload: DarkStoreInventory) -> Dict[str, Any]:
    return {"status": "RECORDED", "store_id": payload.store_id}


@app.post("/api/alert")
@app.post("/api/disruption")
async def trigger_disruption(alert: DisruptionAlert) -> Dict[str, Any]:
    config = {"configurable": {"thread_id": alert.alert_id}}
    initial_state = {
        "alert": alert,
        "disruption_risk_score": 0.0,
        "retrieved_sop": "",
        "mitigation_plan": {},
        "status": "PENDING",
        "history": [f"Incident received for target: {alert.get_zone()}"],
    }

    # Run LangGraph until the interrupt_before breakpoint
    res = await app_compiled.ainvoke(initial_state, config=config)
    return {
        "status": "INTERRUPTED_FOR_REVIEW",
        "thread_id": alert.alert_id,
        "state": res,
    }


@app.post("/api/agent/resume")
@app.post("/api/hitl/decision")
async def resume_workflow(payload: ResumePayload) -> Dict[str, Any]:
    if payload.supervisor_token != "ADMIN_AUTH_TOKEN":
        raise HTTPException(status_code=403, detail="Invalid Supervisor Authorization Token")

    config = {"configurable": {"thread_id": payload.thread_id}}

    if payload.decision.upper() == "REJECT":
        DIGITAL_TWIN.reset_topology()
        return {"status": "ABORTED", "thread_id": payload.thread_id}

    # Resume graph past the breakpoint
    res = await app_compiled.ainvoke(None, config=config)
    return {
        "status": "RESUMED_AND_RESOLVED",
        "thread_id": payload.thread_id,
        "state": res,
    }