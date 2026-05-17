from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("adaptation.topology")


class TopologyAction(BaseModel):
    action_id: str
    action_type: str = ""
    source_region: str = ""
    target_region: str = ""
    current_latency_ms: float = 0.0
    expected_latency_ms: float = 0.0
    improvement_pct: float = 0.0
    description: str = ""
    executed: bool = False
    created_at: float = 0.0


class TopologyOptimizer:
    """Optimizes the global network topology by analyzing inter-region
    latencies, identifying suboptimal paths, and recommending topology
    changes for improved routing efficiency."""

    def __init__(self, latency_improvement_threshold_pct: float = 10.0, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._threshold_pct = latency_improvement_threshold_pct
        self._max_history = max_history
        self._latency_matrix: Dict[str, Dict[str, float]] = {}
        self._actions: List[TopologyAction] = []
        self._total_optimizations = 0
        self._events: List[Dict[str, Any]] = []

    def update_latency_matrix(self, telemetry: Dict[str, Any]) -> None:
        latency_map = telemetry.get("latency_map", {})
        for src, targets in latency_map.items():
            if src not in self._latency_matrix:
                self._latency_matrix[src] = {}
            for tgt, latency in targets.items():
                self._latency_matrix[src][tgt] = latency

    def analyze(self, telemetry: Dict[str, Any]) -> List[TopologyAction]:
        self.update_latency_matrix(telemetry)
        actions: List[TopologyAction] = []
        ts = now_ts()

        for src, targets in self._latency_matrix.items():
            for tgt, direct_latency in targets.items():
                if direct_latency <= 0:
                    continue
                for mid, mid_latency in self._latency_matrix.get(src, {}).items():
                    if mid == tgt or mid == src:
                        continue
                    hop_latency = mid_latency + self._latency_matrix.get(mid, {}).get(tgt, 9999)
                    if hop_latency < direct_latency:
                        improvement = ((direct_latency - hop_latency) / direct_latency) * 100
                        if improvement >= self._threshold_pct:
                            actions.append(TopologyAction(
                                action_id=f"topo-{src}-{tgt}-via-{mid}-{int(ts)}",
                                action_type="add_relay",
                                source_region=src,
                                target_region=tgt,
                                current_latency_ms=round(direct_latency, 1),
                                expected_latency_ms=round(hop_latency, 1),
                                improvement_pct=round(improvement, 1),
                                description=f"Route {src}->{tgt} via {mid} ({direct_latency:.0f}ms -> {hop_latency:.0f}ms)",
                                created_at=ts,
                            ))

        regions = telemetry.get("regions", [])
        saturated = [r for r in regions if r.get("workload_saturation", 0) > 0.9]
        underutilized = [r for r in regions if r.get("workload_saturation", 0) < 0.3]

        for s_region in saturated:
            for u_region in underutilized:
                src_id = s_region.get("region_id", "")
                tgt_id = u_region.get("region_id", "")
                latency = self._latency_matrix.get(src_id, {}).get(tgt_id, 0)
                if latency > 0 and latency < 200:
                    actions.append(TopologyAction(
                        action_id=f"topo-link-{src_id}-{tgt_id}-{int(ts)}",
                        action_type="priority_link",
                        source_region=src_id,
                        target_region=tgt_id,
                        current_latency_ms=round(latency, 1),
                        description=f"Prioritize link {src_id}->{tgt_id} for load redistribution",
                        created_at=ts,
                    ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute(self, action: TopologyAction) -> TopologyAction:
        action.executed = True
        with self._lock:
            self._total_optimizations += 1
            self._events.append({
                "type": "topology_optimized",
                "action_id": action.action_id,
                "action_type": action.action_type,
                "source": action.source_region,
                "target": action.target_region,
                "improvement_pct": action.improvement_pct,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        logger.info("topology optimization: %s", action.description)
        return action

    def optimize(self, telemetry: Dict[str, Any]) -> List[TopologyAction]:
        actions = self.analyze(telemetry)
        for a in actions:
            self.execute(a)
        return actions

    def get_latency_matrix(self) -> Dict[str, Dict[str, float]]:
        return dict(self._latency_matrix)

    def recent_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in reversed(self._actions)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_optimizations": self._total_optimizations,
                "tracked_links": sum(len(t) for t in self._latency_matrix.values()),
                "tracked_regions": len(self._latency_matrix),
            }
