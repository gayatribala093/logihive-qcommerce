"""
core/twin.py
LogiHive Q-Commerce :: Digital Twin of Mumbai's Multi-Tier Supply Chain
-------------------------------------------------------------------------
Models Mother Warehouses -> Regional Hubs -> Dark Stores as a weighted
directed graph (NetworkX). Edge weights represent live traversal cost
(a function of distance, congestion, and disruption risk) and are
mutated in near-real-time as telemetry and alerts arrive.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from typing import Any

import networkx as nx

from interfaces import EXECUTOR  # shared global ThreadPoolExecutor

logger = logging.getLogger("logihive.core.twin")


# --------------------------------------------------------------------------
# Node metadata
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class NodeSpec:
    node_id: str
    tier: str  # "mother_warehouse" | "regional_hub" | "dark_store"
    lat: float
    lon: float


MUMBAI_NODES: list[NodeSpec] = [
    # Tier 1: Mother Warehouses
    NodeSpec("MW_BHIWANDI", "mother_warehouse", 19.2813, 73.0483),
    NodeSpec("MW_TALOJA", "mother_warehouse", 19.0821, 73.1002),
    # Tier 2: Regional Hubs
    NodeSpec("HUB_ANDHERI", "regional_hub", 19.1197, 72.8468),
    NodeSpec("HUB_BKC", "regional_hub", 19.0663, 72.8681),
    NodeSpec("HUB_THANE", "regional_hub", 19.2183, 72.9781),
    NodeSpec("HUB_NAVI_MUMBAI", "regional_hub", 19.0330, 73.0297),
    # Tier 3: Dark Stores
    NodeSpec("DS_BANDRA", "dark_store", 19.0596, 72.8295),
    NodeSpec("DS_POWAI", "dark_store", 19.1176, 72.9060),
    NodeSpec("DS_LOWER_PAREL", "dark_store", 19.0018, 72.8302),
    NodeSpec("DS_MULUND", "dark_store", 19.1726, 72.9425),
    NodeSpec("DS_VASHI", "dark_store", 19.0771, 72.9986),
]

# (source, target, base_distance_km, base_congestion_factor)
MUMBAI_EDGES: list[tuple[str, str, float, float]] = [
    ("MW_BHIWANDI", "HUB_THANE", 18.0, 1.1),
    ("MW_BHIWANDI", "HUB_ANDHERI", 32.0, 1.3),
    ("MW_TALOJA", "HUB_NAVI_MUMBAI", 14.0, 1.1),
    ("MW_TALOJA", "HUB_BKC", 28.0, 1.4),
    ("HUB_ANDHERI", "DS_BANDRA", 9.0, 1.2),
    ("HUB_ANDHERI", "DS_POWAI", 11.0, 1.15),
    ("HUB_BKC", "DS_BANDRA", 6.0, 1.25),
    ("HUB_BKC", "DS_LOWER_PAREL", 8.0, 1.3),
    ("HUB_THANE", "DS_MULUND", 7.0, 1.1),
    ("HUB_THANE", "DS_POWAI", 12.0, 1.15),
    ("HUB_NAVI_MUMBAI", "DS_VASHI", 5.0, 1.05),
    ("HUB_NAVI_MUMBAI", "HUB_BKC", 22.0, 1.35),
]


# --------------------------------------------------------------------------
# Digital Twin
# --------------------------------------------------------------------------

class SupplyChainTwin:
    """Thread-safe wrapper around a NetworkX DiGraph representing the
    live state of Mumbai's quick-commerce supply chain.
    """

    def __init__(self) -> None:
        self._graph = nx.DiGraph()
        self._lock = threading.RLock()
        self._build_base_topology()

    # ---- construction ---------------------------------------------------

    def _build_base_topology(self) -> None:
        for node in MUMBAI_NODES:
            self._graph.add_node(
                node.node_id,
                tier=node.tier,
                lat=node.lat,
                lon=node.lon,
                inventory_units=0,
                risk_score=0.0,
            )

        for src, dst, distance_km, congestion in MUMBAI_EDGES:
            weight = self._compute_weight(distance_km, congestion, disruption_risk=0.0)
            self._graph.add_edge(
                src,
                dst,
                base_distance_km=distance_km,
                congestion_factor=congestion,
                disruption_risk=0.0,
                weight=weight,
            )

        logger.info(
            "Digital twin initialized: %d nodes, %d edges",
            self._graph.number_of_nodes(),
            self._graph.number_of_edges(),
        )

    @staticmethod
    def _compute_weight(distance_km: float, congestion_factor: float, disruption_risk: float) -> float:
        """Composite edge cost: base distance amplified by congestion and
        disruption risk. Risk applies a steep, quadratic penalty as it
        approaches 1.0 so Dijkstra naturally routes traffic away from
        unstable corridors well before they become impassable.
        """
        risk_penalty = 1.0 + (disruption_risk ** 2) * 9.0  # up to 10x cost at risk=1.0
        return round(distance_km * congestion_factor * risk_penalty, 4)

    # ---- dynamic mutation -------------------------------------------------

    def update_edge_weight(
        self,
        source: str,
        target: str,
        *,
        congestion_factor: float | None = None,
        disruption_risk: float | None = None,
    ) -> float:
        """Dynamically recompute and persist a single edge's weight based
        on new congestion or disruption-risk telemetry. Returns the new
        weight. Thread-safe against concurrent writers (FastAPI handlers,
        agent nodes, background telemetry consumers).
        """
        with self._lock:
            if not self._graph.has_edge(source, target):
                raise ValueError(f"No such edge: {source} -> {target}")

            edge = self._graph[source][target]
            if congestion_factor is not None:
                edge["congestion_factor"] = congestion_factor
            if disruption_risk is not None:
                edge["disruption_risk"] = max(0.0, min(1.0, disruption_risk))

            new_weight = self._compute_weight(
                edge["base_distance_km"],
                edge["congestion_factor"],
                edge["disruption_risk"],
            )
            edge["weight"] = new_weight
            logger.debug(
                "Edge %s->%s updated: congestion=%.2f risk=%.2f weight=%.2f",
                source, target, edge["congestion_factor"], edge["disruption_risk"], new_weight,
            )
            return new_weight

    def apply_disruption_alert(self, affected_edges: list[tuple[str, str]], risk_score: float) -> None:
        """Bulk-apply a disruption risk score to a set of affected edges -
        typically called after the Analyst Agent emits a risk tensor for
        a geography, mapping the alert's affected_area to the graph
        corridors that feed it.
        """
        with self._lock:
            for src, dst in affected_edges:
                if self._graph.has_edge(src, dst):
                    self.update_edge_weight(src, dst, disruption_risk=risk_score)
                else:
                    logger.warning("apply_disruption_alert: no edge %s->%s in twin", src, dst)

    def set_dark_store_inventory(self, dark_store_id: str, units: int) -> None:
        with self._lock:
            if dark_store_id not in self._graph.nodes:
                raise ValueError(f"Unknown dark store node: {dark_store_id}")
            self._graph.nodes[dark_store_id]["inventory_units"] = units

    # ---- read-only queries -------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of the current graph state,
        suitable for the Streamlit dashboard or a /api/twin endpoint.
        """
        with self._lock:
            nodes = [{"id": n, **data} for n, data in self._graph.nodes(data=True)]
            edges = [
                {"source": u, "target": v, **data}
                for u, v, data in self._graph.edges(data=True)
            ]
        return {"nodes": nodes, "edges": edges}

    def _dijkstra_sync(self, source: str, target: str) -> tuple[list[str], float]:
        """Blocking Dijkstra call. Intended to run inside the shared
        ThreadPoolExecutor, never directly on the asyncio event loop -
        NetworkX's shortest-path routines are pure CPU-bound Python and
        will stall the event loop on a graph of any real size.
        """
        with self._lock:
            path = nx.dijkstra_path(self._graph, source, target, weight="weight")
            cost = nx.dijkstra_path_length(self._graph, source, target, weight="weight")
        return path, cost

    async def shortest_path(self, source: str, target: str) -> dict[str, Any]:
        """Async-safe Dijkstra shortest path lookup. CPU-bound graph
        traversal is offloaded to the shared ThreadPoolExecutor via
        loop.run_in_executor so the FastAPI event loop stays responsive
        under concurrent load (e.g. while the simulator is flooding
        /api/telemetry and /api/alert).
        """
        loop = asyncio.get_running_loop()
        try:
            path, cost = await loop.run_in_executor(
                EXECUTOR, self._dijkstra_sync, source, target
            )
        except nx.NetworkXNoPath:
            logger.warning("No path found: %s -> %s", source, target)
            return {"source": source, "target": target, "path": [], "cost": None, "reachable": False}
        except nx.NodeNotFound as exc:
            raise ValueError(f"Invalid node in shortest_path query: {exc}") from exc

        return {
            "source": source,
            "target": target,
            "path": path,
            "cost": cost,
            "reachable": True,
        }

    async def resilient_dark_store_routes(self, mother_warehouse: str) -> dict[str, dict[str, Any]]:
        """Compute the current shortest, disruption-aware route from a
        given mother warehouse to every dark store in the twin, in
        parallel. Powers the Streamlit dashboard's live resilience view.
        """
        dark_stores = [
            n for n, d in self._graph.nodes(data=True) if d.get("tier") == "dark_store"
        ]
        results = await asyncio.gather(
            *(self.shortest_path(mother_warehouse, ds) for ds in dark_stores)
        )
        return {r["target"]: r for r in results}


# Module-level singleton - one live twin per process, shared across
# FastAPI request handlers and LangGraph agent nodes.
digital_twin = SupplyChainTwin()


__all__ = ["SupplyChainTwin", "digital_twin", "MUMBAI_NODES", "MUMBAI_EDGES"]