"""v15 Infrastructure Graph Engine.

Represents the global infrastructure as a graph of regions, GPUs, edge nodes,
links, bandwidth, replication state, and thermal state. Supports graph-based
optimization for routing, placement, and failover decisions.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("fabric_core.infrastructure_graph")


class NodeType(str, Enum):
    region = "region"
    gpu_cluster = "gpu_cluster"
    edge_node = "edge_node"
    compute_pool = "compute_pool"


class LinkType(str, Enum):
    backbone = "backbone"
    edge_link = "edge_link"
    overlay = "overlay"
    peering = "peering"


class GraphNode(BaseModel):
    node_id: str
    node_type: NodeType = NodeType.region
    continent: str = ""
    region_id: str = ""
    gpu_count: int = 0
    gpu_types: List[str] = Field(default_factory=list)
    vram_gb: float = 0.0
    capacity: float = 1.0
    utilization: float = 0.0
    thermal_pressure: float = 0.0
    power_consumption_w: float = 0.0
    renewable_pct: float = 0.0
    carbon_intensity: float = 0.0
    status: str = "active"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    updated_at: float = 0.0


class GraphLink(BaseModel):
    source: str
    target: str
    link_type: LinkType = LinkType.backbone
    latency_ms: float = 0.0
    bandwidth_mbps: float = 0.0
    bandwidth_utilization: float = 0.0
    packet_loss_pct: float = 0.0
    encrypted: bool = True
    status: str = "active"
    updated_at: float = 0.0


class InfrastructureGraph:
    """Graph-based representation of global infrastructure for optimization."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._nodes: Dict[str, GraphNode] = {}
        self._links: Dict[str, GraphLink] = {}
        self._adjacency: Dict[str, Set[str]] = {}
        self._max_history = max_history
        self._events: List[Dict[str, Any]] = []

    def add_node(self, node: GraphNode) -> None:
        with self._lock:
            node.updated_at = now_ts()
            self._nodes[node.node_id] = node
            if node.node_id not in self._adjacency:
                self._adjacency[node.node_id] = set()

    def remove_node(self, node_id: str) -> bool:
        with self._lock:
            if node_id not in self._nodes:
                return False
            del self._nodes[node_id]
            neighbors = self._adjacency.pop(node_id, set())
            for n in neighbors:
                self._adjacency.get(n, set()).discard(node_id)
                link_key = self._link_key(node_id, n)
                self._links.pop(link_key, None)
                link_key_rev = self._link_key(n, node_id)
                self._links.pop(link_key_rev, None)
            return True

    def add_link(self, link: GraphLink) -> None:
        with self._lock:
            link.updated_at = now_ts()
            key = self._link_key(link.source, link.target)
            self._links[key] = link
            self._adjacency.setdefault(link.source, set()).add(link.target)
            self._adjacency.setdefault(link.target, set()).add(link.source)

    def _link_key(self, src: str, tgt: str) -> str:
        return f"{src}::{tgt}"

    def get_link(self, src: str, tgt: str) -> Optional[GraphLink]:
        with self._lock:
            return self._links.get(self._link_key(src, tgt)) or self._links.get(self._link_key(tgt, src))

    def neighbors(self, node_id: str) -> List[str]:
        with self._lock:
            return list(self._adjacency.get(node_id, set()))

    def shortest_path(self, src: str, tgt: str) -> Tuple[List[str], float]:
        with self._lock:
            if src not in self._nodes or tgt not in self._nodes:
                return [], float("inf")

            dist: Dict[str, float] = {n: float("inf") for n in self._nodes}
            prev: Dict[str, Optional[str]] = {n: None for n in self._nodes}
            dist[src] = 0.0
            unvisited = set(self._nodes.keys())

            while unvisited:
                current = min(unvisited, key=lambda n: dist[n])
                if dist[current] == float("inf"):
                    break
                if current == tgt:
                    break
                unvisited.remove(current)

                for neighbor in self._adjacency.get(current, set()):
                    if neighbor not in unvisited:
                        continue
                    link = self._links.get(self._link_key(current, neighbor)) or self._links.get(self._link_key(neighbor, current))
                    if not link or link.status != "active":
                        continue
                    new_dist = dist[current] + link.latency_ms
                    if new_dist < dist[neighbor]:
                        dist[neighbor] = new_dist
                        prev[neighbor] = current

            if dist[tgt] == float("inf"):
                return [], float("inf")

            path = []
            node = tgt
            while node is not None:
                path.append(node)
                node = prev[node]
            path.reverse()
            return path, dist[tgt]

    def regions_by_continent(self) -> Dict[str, List[str]]:
        with self._lock:
            result: Dict[str, List[str]] = {}
            for n in self._nodes.values():
                if n.node_type == NodeType.region and n.continent:
                    result.setdefault(n.continent, []).append(n.node_id)
            return result

    def hottest_nodes(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            nodes = sorted(self._nodes.values(), key=lambda n: n.thermal_pressure, reverse=True)
            return [n.model_dump(mode="json") for n in nodes[:limit]]

    def most_utilized(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            nodes = sorted(self._nodes.values(), key=lambda n: n.utilization, reverse=True)
            return [n.model_dump(mode="json") for n in nodes[:limit]]

    def least_utilized(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            active = [n for n in self._nodes.values() if n.status == "active"]
            nodes = sorted(active, key=lambda n: n.utilization)
            return [n.model_dump(mode="json") for n in nodes[:limit]]

    def congested_links(self, threshold: float = 0.8) -> List[Dict[str, Any]]:
        with self._lock:
            return [l.model_dump(mode="json") for l in self._links.values() if l.bandwidth_utilization >= threshold]

    def build_from_telemetry(self, telemetry: Dict[str, Any]) -> None:
        regions = telemetry.get("regions", [])
        for r in regions:
            rid = r.get("region_id", "")
            node = GraphNode(
                node_id=rid,
                node_type=NodeType.region,
                continent=r.get("continent", ""),
                region_id=rid,
                gpu_count=r.get("total_gpus", 0),
                gpu_types=r.get("gpu_types", []),
                vram_gb=r.get("total_vram_gb", 0.0),
                capacity=r.get("capacity", 1.0),
                utilization=r.get("workload_saturation", 0.0),
                thermal_pressure=r.get("thermal_pressure", 0.0),
                power_consumption_w=r.get("power_consumption_w", 0.0),
                renewable_pct=r.get("renewable_pct", 0.0),
                carbon_intensity=r.get("carbon_intensity", 0.0),
                status=r.get("status", "active"),
            )
            self.add_node(node)

        latency_map = telemetry.get("latency_map", {})
        for src, targets in latency_map.items():
            for tgt, lat in targets.items():
                link = GraphLink(
                    source=src,
                    target=tgt,
                    latency_ms=lat,
                    bandwidth_mbps=telemetry.get("bandwidth_map", {}).get(src, {}).get(tgt, 10000),
                )
                self.add_link(link)

    def graph_summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_nodes": len(self._nodes),
                "total_links": len(self._links),
                "by_type": {t.value: sum(1 for n in self._nodes.values() if n.node_type == t) for t in NodeType},
                "by_continent": {k: len(v) for k, v in self.regions_by_continent().items()},
                "active_nodes": sum(1 for n in self._nodes.values() if n.status == "active"),
                "active_links": sum(1 for l in self._links.values() if l.status == "active"),
            }

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            nodes = list(self._nodes.values())
            avg_util = sum(n.utilization for n in nodes) / len(nodes) if nodes else 0.0
            avg_thermal = sum(n.thermal_pressure for n in nodes) / len(nodes) if nodes else 0.0
            links = list(self._links.values())
            avg_latency = sum(l.latency_ms for l in links) / len(links) if links else 0.0
            return {
                "ts": now_ts(),
                "total_nodes": len(self._nodes),
                "total_links": len(self._links),
                "avg_utilization": round(avg_util, 3),
                "avg_thermal_pressure": round(avg_thermal, 3),
                "avg_link_latency_ms": round(avg_latency, 1),
            }
