"""
LogiHive Q-Commerce :: core/twin.py
In-memory 12-node 3-tier Mumbai logistics network using NetworkX.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Dict, Any, List, Tuple

import networkx as nx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from interfaces import EXECUTOR

# 12-Node Metropolitan Mumbai Topology
METRO_NODES: Dict[str, Dict[str, Any]] = {
    "MW_BHIWANDI": {"lat": 19.2969, "lon": 73.0620, "tier": "Mother Warehouse", "inventory_units": 85000, "status": "HEALTHY"},
    "MW_TALOJA": {"lat": 19.0700, "lon": 73.1100, "tier": "Mother Warehouse", "inventory_units": 72000, "status": "HEALTHY"},
    "HUB_THANE": {"lat": 19.2183, "lon": 72.9781, "tier": "Regional Hub", "inventory_units": 28400, "status": "HEALTHY"},
    "HUB_ANDHERI": {"lat": 19.1197, "lon": 72.8468, "tier": "Regional Hub", "inventory_units": 34100, "status": "HEALTHY"},
    "HUB_BKC": {"lat": 19.0657, "lon": 72.8687, "tier": "Regional Hub", "inventory_units": 31500, "status": "HEALTHY"},
    "HUB_NAVI_MUMBAI": {"lat": 19.0330, "lon": 73.0297, "tier": "Regional Hub", "inventory_units": 26800, "status": "HEALTHY"},
    "DS_POWAI": {"lat": 19.1176, "lon": 72.9050, "tier": "Dark Store", "inventory_units": 9800, "status": "HEALTHY"},
    "DS_MULUND": {"lat": 19.1726, "lon": 72.9565, "tier": "Dark Store", "inventory_units": 7400, "status": "HEALTHY"},
    "DS_BANDRA": {"lat": 19.0596, "lon": 72.8295, "tier": "Dark Store", "inventory_units": 8200, "status": "HEALTHY"},
    "DS_LOWER_PAREL": {"lat": 19.0033, "lon": 72.8286, "tier": "Dark Store", "inventory_units": 6900, "status": "HEALTHY"},
    "DS_ANDHERI_EAST": {"lat": 19.1136, "lon": 72.8697, "tier": "Dark Store", "inventory_units": 8600, "status": "HEALTHY"},
    "DS_VASHI": {"lat": 19.0771, "lon": 72.9986, "tier": "Dark Store", "inventory_units": 7100, "status": "HEALTHY"},
}

# 14 Baseline Inter-tier Transit Edges (transit delay in minutes)
METRO_EDGES: List[Tuple[str, str, float]] = [
    ("MW_BHIWANDI", "HUB_THANE", 18.0),
    ("HUB_THANE", "DS_MULUND", 12.0),
    ("HUB_THANE", "HUB_ANDHERI", 24.0),
    ("HUB_ANDHERI", "DS_ANDHERI_EAST", 10.0),
    ("HUB_ANDHERI", "DS_POWAI", 14.0),
    ("HUB_ANDHERI", "DS_BANDRA", 22.0),
    ("DS_POWAI", "DS_ANDHERI_EAST", 11.0),
    ("MW_TALOJA", "HUB_NAVI_MUMBAI", 15.0),
    ("HUB_NAVI_MUMBAI", "DS_VASHI", 9.0),
    ("HUB_NAVI_MUMBAI", "HUB_BKC", 25.0),
    ("HUB_BKC", "DS_BANDRA", 16.0),
    ("HUB_BKC", "DS_LOWER_PAREL", 18.0),
    ("DS_BANDRA", "DS_LOWER_PAREL", 14.0),
    ("DS_POWAI", "HUB_BKC", 20.0),
]


class MumbaiLogisticsTwin:
    def __init__(self) -> None:
        self.graph = nx.DiGraph()
        self.reset_topology()

    def reset_topology(self) -> None:
        self.graph.clear()
        for node_id, data in METRO_NODES.items():
            self.graph.add_node(node_id, **data)
        for u, v, weight in METRO_EDGES:
            self.graph.add_edge(u, v, weight=weight, base_weight=weight, disruption_risk=0.04)

    def apply_disruption(self, compromised_node: str, delay_penalty: float = 999.0) -> None:
        # Apply quadratic penalty to all edges connected to the disrupted node
        for u, v, data in self.graph.edges(data=True):
            if u == compromised_node or v == compromised_node:
                data["weight"] = data["base_weight"] + delay_penalty
                data["disruption_risk"] = 0.85
        if compromised_node in self.graph.nodes:
            self.graph.nodes[compromised_node]["status"] = "COMPROMISED"

    def calculate_shortest_path(self, source: str, target: str) -> Dict[str, Any]:
        try:
            path = nx.dijkstra_path(self.graph, source, target, weight="weight")
            length = nx.dijkstra_path_length(self.graph, source, target, weight="weight")
            return {"path": path, "travel_time_min": round(length, 1), "status": "OPTIMAL"}
        except nx.NetworkXNoPath:
            return {"path": [], "travel_time_min": 999.0, "status": "NO_PATH"}

    async def calculate_shortest_path_async(self, source: str, target: str) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(EXECUTOR, self.calculate_shortest_path, source, target)

    def get_snapshot(self) -> Dict[str, Any]:
        nodes_out = []
        for nid, data in self.graph.nodes(data=True):
            nodes_out.append({
                "id": nid,
                "lat": data.get("lat"),
                "lon": data.get("lon"),
                "tier": data.get("tier"),
                "inventory_units": data.get("inventory_units", 8200),
                "status": data.get("status", "HEALTHY"),
            })
        edges_out = []
        for u, v, data in self.graph.edges(data=True):
            edges_out.append({
                "source": u,
                "target": v,
                "weight": data.get("weight", 15.0),
                "disruption_risk": data.get("disruption_risk", 0.04),
            })
        return {"nodes": nodes_out, "edges": edges_out}


DIGITAL_TWIN = MumbaiLogisticsTwin()