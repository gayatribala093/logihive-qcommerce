"""
interfaces.py
LogiHive Q-Commerce :: System Data Contracts & Shared Thread Pool
--------------------------------------------------------------------
Single source of truth for every payload shape that crosses a process
boundary in this system (HTTP request bodies, Redis stream messages,
LangGraph state fields). Every other module imports from here rather
than redefining shapes locally, so a schema change only ever happens
in one place.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

from pydantic import BaseModel, ConfigDict, Field

# --------------------------------------------------------------------------
# Shared executor
# --------------------------------------------------------------------------
# One process-wide pool for CPU-bound work (NetworkX Dijkstra recalculation,
# JSON-heavy tensor post-processing, etc.) that must never block the FastAPI
# event loop. Sized via env var so deployment can tune it per-host without a
# code change. This executor is intentionally separate from the dedicated
# single-worker executor LangGraph uses internally (see agents/graph.py) to
# serialize access to the SQLite checkpointer.

EXECUTOR = ThreadPoolExecutor(
    max_workers=int(os.getenv("LOGIHIVE_EXECUTOR_WORKERS", "8")),
    thread_name_prefix="logihive-cpu",
)


# --------------------------------------------------------------------------
# Data contracts
# --------------------------------------------------------------------------

class RiderTelemetry(BaseModel):
    """A single GPS/status ping from a rider's device. High-volume,
    fire-and-forget: consumed off the Redis stream by the ingestion
    worker to keep corridor congestion estimates fresh.
    """

    model_config = ConfigDict(extra="forbid")

    rider_id: str = Field(..., min_length=1, max_length=64)
    dark_store_id: str = Field(..., min_length=1, max_length=64)
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed_kmph: float = Field(..., ge=0.0, le=120.0)
    battery_pct: float = Field(..., ge=0.0, le=100.0)
    active_order_id: str | None = None
    timestamp: float = Field(..., description="Unix epoch seconds")


class DarkStoreInventory(BaseModel):
    """Point-in-time stock level for a single SKU at a dark store."""

    model_config = ConfigDict(extra="forbid")

    dark_store_id: str = Field(..., min_length=1, max_length=64)
    sku_id: str = Field(..., min_length=1, max_length=64)
    units_available: int = Field(..., ge=0)
    reorder_threshold: int = Field(default=20, ge=0)
    updated_at: float = Field(..., description="Unix epoch seconds")


class DisruptionAlert(BaseModel):
    """An unstructured or semi-structured disruption report — the input
    to the Analyst Agent. `raw_text` is what gets parsed by the local
    Ollama VLM into a risk tensor.
    """

    model_config = ConfigDict(extra="forbid")

    alert_id: str = Field(..., min_length=1, max_length=64)
    category: str = Field(..., min_length=1, max_length=64)
    affected_area: str = Field(..., min_length=1, max_length=64)
    raw_text: str = Field(..., min_length=1, max_length=4000)
    reported_at: float = Field(..., description="Unix epoch seconds")
    severity_hint: str = Field(default="medium")


__all__ = ["EXECUTOR", "RiderTelemetry", "DarkStoreInventory", "DisruptionAlert"]