"""v16 Satellite Registry.

Manages registration, health tracking, and capability reporting for
orbital compute nodes across LEO, MEO, GEO, HEO, lunar, and deep
space positions.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("orbital.satellite_registry")


class NodeOrbit(str, Enum):
    leo = "leo"
    meo = "meo"
    geo = "geo"
    heo = "heo"
    lunar = "lunar"
    deep_space = "deep_space"
    ground = "ground"


class NodeStatus(str, Enum):
    active = "active"
    degraded = "degraded"
    disconnected = "disconnected"
    maintenance = "maintenance"
    offline = "offline"
    blackout = "blackout"


class OrbitalNode(BaseModel):
    node_id: str
    name: str = ""
    orbit: NodeOrbit = NodeOrbit.ground
    position: Dict[str, float] = Field(default_factory=dict)
    status: NodeStatus = NodeStatus.active
    signal_latency_ms: float = 0.0
    bandwidth_kbps: float = 0.0
    compute_capacity: float = 1.0
    compute_utilization: float = 0.0
    gpu_available: bool = False
    gpu_count: int = 0
    memory_gb: float = 0.0
    storage_gb: float = 0.0
    power_watts: float = 0.0
    thermal_c: float = 0.0
    uptime_pct: float = 99.0
    isolation_risk: float = 0.0
    next_contact_window_ts: float = 0.0
    contact_window_duration_s: float = 0.0
    active_workloads: int = 0
    queue_depth: int = 0
    last_heartbeat: float = 0.0
    registered_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SatelliteRegistry:
    """Manages orbital and ground compute node registration, health
    tracking, and capability discovery."""

    def __init__(self, heartbeat_timeout_s: float = 120.0,
                 max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._heartbeat_timeout = heartbeat_timeout_s
        self._max_history = max_history
        self._nodes: Dict[str, OrbitalNode] = {}
        self._events: List[Dict[str, Any]] = []

    def register(self, node: OrbitalNode) -> OrbitalNode:
        with self._lock:
            ts = now_ts()
            node.registered_at = ts
            node.last_heartbeat = ts
            self._nodes[node.node_id] = node
            self._add_event("node_registered", node.node_id,
                            orbit=node.orbit.value, status=node.status.value)
            return node

    def deregister(self, node_id: str) -> bool:
        with self._lock:
            if node_id not in self._nodes:
                return False
            del self._nodes[node_id]
            self._add_event("node_deregistered", node_id)
            return True

    def heartbeat(self, node_id: str, updates: Optional[Dict[str, Any]] = None) -> bool:
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return False
            node.last_heartbeat = now_ts()
            if node.status == NodeStatus.disconnected:
                node.status = NodeStatus.active
                self._add_event("node_reconnected", node_id)
            if updates:
                for key in ("compute_utilization", "signal_latency_ms", "bandwidth_kbps",
                            "thermal_c", "power_watts", "active_workloads", "queue_depth",
                            "isolation_risk", "gpu_count"):
                    if key in updates:
                        setattr(node, key, updates[key])
            return True

    def check_health(self) -> List[str]:
        with self._lock:
            ts = now_ts()
            disconnected = []
            for node in self._nodes.values():
                timeout = self._heartbeat_timeout
                if node.orbit in (NodeOrbit.lunar, NodeOrbit.deep_space):
                    timeout *= 10
                elif node.orbit in (NodeOrbit.geo, NodeOrbit.heo):
                    timeout *= 3

                if ts - node.last_heartbeat > timeout:
                    if node.status != NodeStatus.disconnected:
                        node.status = NodeStatus.disconnected
                        disconnected.append(node.node_id)
                        self._add_event("node_disconnected", node.node_id,
                                        orbit=node.orbit.value)
            return disconnected

    def nodes_by_orbit(self, orbit: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [n.model_dump(mode="json") for n in self._nodes.values()
                    if n.orbit.value == orbit]

    def active_nodes(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [n.model_dump(mode="json") for n in self._nodes.values()
                    if n.status in (NodeStatus.active, NodeStatus.degraded)]

    def disconnected_nodes(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [n.model_dump(mode="json") for n in self._nodes.values()
                    if n.status == NodeStatus.disconnected]

    def ground_nodes(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [n.model_dump(mode="json") for n in self._nodes.values()
                    if n.orbit == NodeOrbit.ground and n.status != NodeStatus.offline]

    def orbital_nodes(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [n.model_dump(mode="json") for n in self._nodes.values()
                    if n.orbit != NodeOrbit.ground and n.status != NodeStatus.offline]

    def node_summary(self) -> Dict[str, Any]:
        with self._lock:
            by_orbit: Dict[str, int] = {}
            by_status: Dict[str, int] = {}
            total_gpu = 0
            total_compute = 0.0
            for node in self._nodes.values():
                by_orbit[node.orbit.value] = by_orbit.get(node.orbit.value, 0) + 1
                by_status[node.status.value] = by_status.get(node.status.value, 0) + 1
                total_gpu += node.gpu_count
                total_compute += node.compute_capacity
            return {
                "total_nodes": len(self._nodes),
                "by_orbit": by_orbit,
                "by_status": by_status,
                "total_gpus": total_gpu,
                "total_compute_capacity": round(total_compute, 2),
            }

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            summary = self.node_summary()
            active = sum(1 for n in self._nodes.values()
                         if n.status in (NodeStatus.active, NodeStatus.degraded))
            avg_latency = 0.0
            active_list = [n for n in self._nodes.values()
                           if n.status in (NodeStatus.active, NodeStatus.degraded)]
            if active_list:
                avg_latency = sum(n.signal_latency_ms for n in active_list) / len(active_list)
            return {
                "ts": now_ts(),
                "total_nodes": len(self._nodes),
                "active_nodes": active,
                "avg_signal_latency_ms": round(avg_latency, 1),
                **summary,
            }


from typing import Optional  # noqa: E402 — deferred to avoid circular import issues
