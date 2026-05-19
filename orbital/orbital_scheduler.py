"""v16 Orbital Scheduler.

Coordinates orbital compute nodes, optimizes edge execution across
high-latency links, reduces bandwidth waste, and prioritizes local
inference. Routing decisions consider signal latency, available compute,
orbital bandwidth, and regional isolation state.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("orbital.orbital_scheduler")


class OrbitType(str, Enum):
    leo = "leo"
    meo = "meo"
    geo = "geo"
    heo = "heo"
    lunar = "lunar"
    deep_space = "deep_space"
    ground = "ground"


class SchedulingMode(str, Enum):
    latency_optimized = "latency_optimized"
    bandwidth_optimized = "bandwidth_optimized"
    compute_local = "compute_local"
    store_forward = "store_forward"
    balanced = "balanced"


class OrbitalWorkload(BaseModel):
    workload_id: str
    source_node: str = ""
    orbit_type: OrbitType = OrbitType.ground
    mode: SchedulingMode = SchedulingMode.balanced
    gpu_required: bool = False
    max_latency_ms: float = 0.0
    bandwidth_required_kbps: float = 0.0
    priority: int = 5
    can_defer: bool = False
    estimated_duration_ms: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class OrbitalDecision(BaseModel):
    workload_id: str
    assigned_node: str = ""
    assigned_orbit: OrbitType = OrbitType.ground
    mode_used: SchedulingMode = SchedulingMode.balanced
    signal_latency_ms: float = 0.0
    score: float = 0.0
    factors: Dict[str, float] = Field(default_factory=dict)
    deferred: bool = False
    alternatives: List[str] = Field(default_factory=list)
    decision_time_ms: float = 0.0
    ts: float = 0.0


class OrbitalScheduler:
    """Coordinates workload scheduling across orbital and ground nodes
    with awareness of signal latency, bandwidth constraints, and
    disconnection windows."""

    def __init__(self, max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._decisions: List[OrbitalDecision] = []
        self._total_scheduled = 0
        self._total_deferred = 0
        self._by_orbit: Dict[str, int] = {}
        self._by_mode: Dict[str, int] = {}
        self._events: List[Dict[str, Any]] = []

    def schedule(self, workload: OrbitalWorkload,
                 nodes: List[Dict[str, Any]]) -> Optional[OrbitalDecision]:
        start = now_ts()
        if not nodes:
            return None

        candidates = self._filter_candidates(workload, nodes)
        if not candidates:
            if workload.can_defer:
                return self._defer_workload(workload, start)
            candidates = nodes

        scored = []
        for node in candidates:
            score, factors = self._score_node(workload, node)
            scored.append((score, node, factors))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_node, best_factors = scored[0]
        alternatives = [n.get("node_id", "") for _, n, _ in scored[1:4]]

        decision = OrbitalDecision(
            workload_id=workload.workload_id,
            assigned_node=best_node.get("node_id", ""),
            assigned_orbit=OrbitType(best_node.get("orbit_type", "ground")),
            mode_used=workload.mode,
            signal_latency_ms=round(best_node.get("signal_latency_ms", 0), 1),
            score=round(best_score, 4),
            factors=best_factors,
            alternatives=alternatives,
            decision_time_ms=round((now_ts() - start) * 1000, 2),
            ts=now_ts(),
        )

        with self._lock:
            self._decisions.append(decision)
            if len(self._decisions) > self._max_history:
                self._decisions = self._decisions[-self._max_history:]
            self._total_scheduled += 1
            orbit_key = decision.assigned_orbit.value
            self._by_orbit[orbit_key] = self._by_orbit.get(orbit_key, 0) + 1
            self._by_mode[workload.mode.value] = self._by_mode.get(workload.mode.value, 0) + 1
            self._add_event("workload_scheduled", workload.workload_id,
                            node=decision.assigned_node, orbit=orbit_key)

        return decision

    def schedule_batch(self, workloads: List[OrbitalWorkload],
                       nodes: List[Dict[str, Any]]) -> List[OrbitalDecision]:
        sorted_wl = sorted(workloads, key=lambda w: w.priority, reverse=True)
        results = []
        for wl in sorted_wl:
            decision = self.schedule(wl, nodes)
            if decision:
                results.append(decision)
        return results

    def _filter_candidates(self, workload: OrbitalWorkload,
                           nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        candidates = [n for n in nodes if n.get("status", "active") != "offline"]

        if workload.gpu_required:
            gpu_nodes = [n for n in candidates if n.get("gpu_available", False)]
            if gpu_nodes:
                candidates = gpu_nodes

        if workload.max_latency_ms > 0:
            latency_ok = [n for n in candidates
                          if n.get("signal_latency_ms", 0) <= workload.max_latency_ms]
            if latency_ok:
                candidates = latency_ok

        if workload.bandwidth_required_kbps > 0:
            bw_ok = [n for n in candidates
                     if n.get("bandwidth_kbps", 0) >= workload.bandwidth_required_kbps]
            if bw_ok:
                candidates = bw_ok

        return candidates

    def _score_node(self, workload: OrbitalWorkload,
                    node: Dict[str, Any]) -> tuple:
        signal_latency = node.get("signal_latency_ms", 100)
        latency_score = max(0.0, 1.0 - signal_latency / 5000.0)

        compute_util = node.get("compute_utilization", 0.5)
        compute_score = 1.0 - compute_util

        bandwidth = node.get("bandwidth_kbps", 1000)
        bw_score = min(1.0, bandwidth / 10000.0)

        isolation = node.get("isolation_risk", 0.0)
        isolation_score = 1.0 - isolation

        uptime = node.get("uptime_pct", 99.0)
        reliability_score = uptime / 100.0

        queue_depth = node.get("queue_depth", 0)
        queue_score = max(0.0, 1.0 - queue_depth / 1000.0)

        if workload.mode == SchedulingMode.latency_optimized:
            weights = {"latency": 0.40, "compute": 0.20, "bandwidth": 0.10,
                       "isolation": 0.10, "reliability": 0.10, "queue": 0.10}
        elif workload.mode == SchedulingMode.bandwidth_optimized:
            weights = {"latency": 0.10, "compute": 0.15, "bandwidth": 0.35,
                       "isolation": 0.10, "reliability": 0.15, "queue": 0.15}
        elif workload.mode == SchedulingMode.compute_local:
            weights = {"latency": 0.05, "compute": 0.40, "bandwidth": 0.05,
                       "isolation": 0.20, "reliability": 0.15, "queue": 0.15}
        elif workload.mode == SchedulingMode.store_forward:
            weights = {"latency": 0.05, "compute": 0.15, "bandwidth": 0.30,
                       "isolation": 0.05, "reliability": 0.25, "queue": 0.20}
        else:
            weights = {"latency": 0.20, "compute": 0.20, "bandwidth": 0.15,
                       "isolation": 0.15, "reliability": 0.15, "queue": 0.15}

        scores = {
            "latency": latency_score,
            "compute": compute_score,
            "bandwidth": bw_score,
            "isolation": isolation_score,
            "reliability": reliability_score,
            "queue": queue_score,
        }

        total = sum(weights[k] * scores[k] for k in weights)
        factors = {k: round(scores[k], 3) for k in scores}
        return total, factors

    def _defer_workload(self, workload: OrbitalWorkload, start: float) -> OrbitalDecision:
        with self._lock:
            self._total_deferred += 1
            self._add_event("workload_deferred", workload.workload_id)
        return OrbitalDecision(
            workload_id=workload.workload_id,
            deferred=True,
            decision_time_ms=round((now_ts() - start) * 1000, 2),
            ts=now_ts(),
        )

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_decisions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.model_dump(mode="json") for d in reversed(self._decisions)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._decisions[-100:] if self._decisions else []
            avg_time = sum(d.decision_time_ms for d in recent) / len(recent) if recent else 0.0
            avg_latency = sum(d.signal_latency_ms for d in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_scheduled": self._total_scheduled,
                "total_deferred": self._total_deferred,
                "avg_decision_time_ms": round(avg_time, 2),
                "avg_signal_latency_ms": round(avg_latency, 1),
                "by_orbit": dict(self._by_orbit),
                "by_mode": dict(self._by_mode),
            }
