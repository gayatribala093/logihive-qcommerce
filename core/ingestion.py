"""
core/ingestion.py
LogiHive Q-Commerce :: Async FastAPI Gateway & Redis Queue Worker
--------------------------------------------------------------------
The single HTTP surface for the whole system:

  POST /api/telemetry     -> validate RiderTelemetry, push to a Redis
                              stream, ack immediately (202). A background
                              consumer drains the stream and feeds live
                              rider density into the digital twin's
                              congestion model.
  POST /api/alert          -> validate DisruptionAlert, run it through
                              the LangGraph agent engine (RAG -> Analyst
                              -> optional HITL gate -> mitigation),
                              apply the resulting risk to the twin, and
                              return the plan to the caller.
  GET  /api/twin/snapshot  -> current NetworkX graph state (nodes+edges)
                              for the Streamlit dashboard's map.
  GET  /api/hitl/pending   -> threads currently paused at human_gate.
  POST /api/hitl/decision  -> inject an operator decision and resume a
                              paused thread to completion.
  GET  /health             -> liveness/readiness probe.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from interfaces import DarkStoreInventory, DisruptionAlert, RiderTelemetry
from core.twin import digital_twin, MUMBAI_NODES
from agents.graph import ainvoke_graph, aresume_graph, aget_state, HUMAN_GATE_RISK_THRESHOLD

logger = logging.getLogger("logihive.ingestion")

REDIS_URL = "redis://localhost:6379/0"
TELEMETRY_STREAM = "logihive:telemetry"
TELEMETRY_CONSUMER_GROUP = "logihive-twin-workers"
TELEMETRY_CONSUMER_NAME = "ingestion-worker-1"
CONGESTION_WINDOW_S = 30.0

MOTHER_WAREHOUSE_IDS = [n.node_id for n in MUMBAI_NODES if n.tier == "mother_warehouse"]


# --------------------------------------------------------------------------
# In-process state
# --------------------------------------------------------------------------
# Rider pings feeding the congestion model, and the registry of threads
# currently paused at the human_gate interrupt. Both are process-local;
# in a multi-replica deployment these would move to Redis/Postgres, but a
# single ingestion process is the right unit of scale for this milestone.

_redis_client: aioredis.Redis | None = None
_recent_pings: dict[str, list[float]] = {}  # dark_store_id -> [timestamps]
_pending_hitl: dict[str, dict[str, Any]] = {}  # thread_id -> summary
_state_lock = asyncio.Lock()


# --------------------------------------------------------------------------
# Lifespan: Redis connection + background consumer
# --------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis_client
    try:
        _redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await _redis_client.ping()
        try:
            await _redis_client.xgroup_create(
                TELEMETRY_STREAM, TELEMETRY_CONSUMER_GROUP, id="0", mkstream=True
            )
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        logger.info("Connected to Redis at %s", REDIS_URL)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Redis unavailable at startup (%s) — telemetry queueing degraded.", exc)
        _redis_client = None

    consumer_task = asyncio.create_task(_telemetry_consumer_loop())
    try:
        yield
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        if _redis_client is not None:
            await _redis_client.aclose()


app = FastAPI(
    title="LogiHive Q-Commerce Gateway",
    version="0.7.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to the dashboard's origin in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------
# Telemetry ingestion
# --------------------------------------------------------------------------

@app.post("/api/telemetry", status_code=202)
async def ingest_telemetry(telemetry: RiderTelemetry) -> dict[str, str]:
    """Accept a rider ping and push it onto the Redis stream. This
    endpoint deliberately does no synchronous processing — it must stay
    fast under the simulator's flood load — the background consumer
    does the real work of updating congestion.
    """
    if _redis_client is None:
        # Degrade gracefully: update in-memory congestion directly so the
        # system still functions (with reduced durability) if Redis is down.
        await _record_ping(telemetry.dark_store_id, telemetry.timestamp)
        return {"status": "accepted_degraded"}

    try:
        await _redis_client.xadd(
            TELEMETRY_STREAM,
            {"payload": telemetry.model_dump_json()},
            maxlen=100_000,
            approximate=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to enqueue telemetry: %s", exc)
        raise HTTPException(status_code=503, detail="Telemetry queue unavailable") from exc

    return {"status": "accepted"}


async def _telemetry_consumer_loop() -> None:
    """Background worker: drains the Redis stream and folds rider density
    into the twin's per-corridor congestion factor. Falls back to a
    lightweight polling no-op if Redis never came up.
    """
    if _redis_client is None:
        logger.warning("Telemetry consumer idle: no Redis connection.")
        return

    logger.info("Telemetry consumer loop started.")
    while True:
        try:
            entries = await _redis_client.xreadgroup(
                groupname=TELEMETRY_CONSUMER_GROUP,
                consumername=TELEMETRY_CONSUMER_NAME,
                streams={TELEMETRY_STREAM: ">"},
                count=200,
                block=2000,
            )
            if not entries:
                await _recompute_congestion()
                continue

            for _stream_name, messages in entries:
                for message_id, fields in messages:
                    try:
                        data = json.loads(fields["payload"])
                        telemetry = RiderTelemetry.model_validate(data)
                        await _record_ping(telemetry.dark_store_id, telemetry.timestamp)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Skipping malformed telemetry message %s: %s", message_id, exc)
                    finally:
                        await _redis_client.xack(TELEMETRY_STREAM, TELEMETRY_CONSUMER_GROUP, message_id)

            await _recompute_congestion()

        except asyncio.CancelledError:
            logger.info("Telemetry consumer loop stopping.")
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("Telemetry consumer loop error: %s", exc)
            await asyncio.sleep(1.0)


async def _record_ping(dark_store_id: str, timestamp: float) -> None:
    async with _state_lock:
        bucket = _recent_pings.setdefault(dark_store_id, [])
        bucket.append(timestamp)
        cutoff = time.time() - CONGESTION_WINDOW_S
        _recent_pings[dark_store_id] = [t for t in bucket if t >= cutoff][-500:]


async def _recompute_congestion() -> None:
    """Translate recent rider ping density per dark store into a
    congestion_factor on every edge that terminates at that store.
    Busier corridors cost more, which naturally biases Dijkstra away
    from saturated approach roads.
    """
    async with _state_lock:
        density_snapshot = {k: len(v) for k, v in _recent_pings.items()}

    if not density_snapshot:
        return

    snapshot = digital_twin.snapshot()
    for edge in snapshot["edges"]:
        target = edge["target"]
        if target not in density_snapshot:
            continue
        rider_count = density_snapshot[target]
        congestion = 1.0 + min(rider_count / 40.0, 1.5)
        try:
            digital_twin.update_edge_weight(
                edge["source"], edge["target"], congestion_factor=round(congestion, 3)
            )
        except ValueError:
            continue


# --------------------------------------------------------------------------
# Disruption alert -> agent engine -> twin
# --------------------------------------------------------------------------

def _affected_edges(area: str) -> list[tuple[str, str]]:
    """Every corridor whose terminus is the reported area — the set of
    edges a flood/protest/outage there realistically degrades."""
    snapshot = digital_twin.snapshot()
    return [(e["source"], e["target"]) for e in snapshot["edges"] if e["target"] == area]


async def _nearest_mother_warehouse(area: str) -> dict[str, Any] | None:
    routes = await asyncio.gather(
        *(digital_twin.shortest_path(mw, area) for mw in MOTHER_WAREHOUSE_IDS)
    )
    reachable = [r for r in routes if r["reachable"]]
    if not reachable:
        return None
    return min(reachable, key=lambda r: r["cost"])


@app.post("/api/alert")
async def ingest_alert(alert: DisruptionAlert) -> dict[str, Any]:
    """Run a disruption report through the full agent pipeline: SOP RAG
    lookup, Ollama-VLM risk scoring, an optional HITL pause above the
    0.75 threshold, then mitigation dispatch. Applies the resulting risk
    score to the affected corridors in the digital twin either way, so
    the map reflects the disruption even while a human review is pending.
    """
    thread_id = alert.alert_id
    config = {"configurable": {"thread_id": thread_id}}
    initial_state = {
        "raw_alert_text": alert.raw_text,
        "alert": alert,
        "retrieved_sops": [],
        "agent_trace": [],
        "disruption_risk_score": 0.0,
        "risk_tensor": {},
        "requires_human_review": False,
        "human_decision": None,
        "mitigation_actions": [],
    }

    try:
        result_state = await ainvoke_graph(initial_state, config)
    except Exception as exc:  # noqa: BLE001
        # logger.exception captures the full traceback in the uvicorn console.
        logger.exception("Agent graph invocation failed for %s", thread_id)
        raise HTTPException(
            status_code=502,
            detail=f"Agent engine failure: {type(exc).__name__}: {exc}",
        ) from exc

    risk_score = result_state.get("disruption_risk_score", 0.0)
    edges = _affected_edges(alert.affected_area)
    if edges:
        digital_twin.apply_disruption_alert(edges, risk_score)
    else:
        logger.info("No corridors terminate at %s; risk recorded but not applied to twin.", alert.affected_area)

    graph_state = await aget_state(config)
    paused = bool(graph_state.next) and "human_gate" in graph_state.next

    route = await _nearest_mother_warehouse(alert.affected_area)

    if paused:
        async with _state_lock:
            _pending_hitl[thread_id] = {
                "thread_id": thread_id,
                "disruption_risk_score": risk_score,
                "raw_alert_text": alert.raw_text,
                "category": alert.category,
                "affected_area": alert.affected_area,
                "queued_at": time.time(),
            }

    return {
        "thread_id": thread_id,
        "disruption_risk_score": risk_score,
        "risk_tensor": result_state.get("risk_tensor", {}),
        "requires_human_review": paused,
        "mitigation_actions": result_state.get("mitigation_actions", []),
        "agent_trace": result_state.get("agent_trace", []),
        "route": route or {},
    }


# --------------------------------------------------------------------------
# HITL console endpoints
# --------------------------------------------------------------------------

@app.get("/api/hitl/pending")
async def list_pending_hitl() -> dict[str, list[dict[str, Any]]]:
    async with _state_lock:
        pending = list(_pending_hitl.values())
    return {"pending": sorted(pending, key=lambda p: p["queued_at"], reverse=True)}


class HitlDecision(BaseModel):
    thread_id: str
    decision: str  # "APPROVE" | "REJECT"


@app.post("/api/hitl/decision")
async def submit_hitl_decision(payload: HitlDecision) -> dict[str, Any]:
    if payload.decision not in ("APPROVE", "REJECT"):
        raise HTTPException(status_code=400, detail="decision must be APPROVE or REJECT")

    async with _state_lock:
        pending_entry = _pending_hitl.pop(payload.thread_id, None)
    if pending_entry is None:
        raise HTTPException(status_code=404, detail=f"No pending thread {payload.thread_id}")

    config = {"configurable": {"thread_id": payload.thread_id}}
    try:
        result_state = await aresume_graph(config, {"human_decision": payload.decision})
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to resume thread %s", payload.thread_id)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to resume agent run: {type(exc).__name__}: {exc}",
        ) from exc

    if payload.decision == "REJECT":
        edges = _affected_edges(pending_entry["affected_area"])
        if edges:
            digital_twin.apply_disruption_alert(edges, disruption_risk=0.0)

    return {
        "thread_id": payload.thread_id,
        "decision": payload.decision,
        "mitigation_actions": result_state.get("mitigation_actions", []),
        "agent_trace": result_state.get("agent_trace", []),
    }


# --------------------------------------------------------------------------
# Digital twin + misc
# --------------------------------------------------------------------------

@app.get("/api/twin/snapshot")
async def twin_snapshot() -> dict[str, Any]:
    return digital_twin.snapshot()


@app.post("/api/inventory")
async def update_inventory(inventory: DarkStoreInventory) -> dict[str, str]:
    try:
        digital_twin.set_dark_store_inventory(inventory.dark_store_id, inventory.units_available)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"status": "updated"}


@app.get("/health")
async def health() -> dict[str, Any]:
    redis_ok = False
    if _redis_client is not None:
        try:
            redis_ok = await _redis_client.ping()
        except Exception:  # noqa: BLE001
            redis_ok = False
    return {
        "status": "ok",
        "redis_connected": redis_ok,
        "twin_nodes": digital_twin._graph.number_of_nodes(),
        "pending_hitl_count": len(_pending_hitl),
    }