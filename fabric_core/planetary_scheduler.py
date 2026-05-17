"""v15 Planetary Scheduler.

Coordinates workloads globally across planetary-scale infrastructure,
optimizes latency worldwide, distributes inference intelligently,
and manages millions of concurrent requests.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("fabric_core.planetary_scheduler")


class SchedulingTier(str, Enum):
    realtime = "realtime"
    interactive = "interactive"
    batch = "batch"
    background = "background"


class SchedulingStrategy(str, Enum):
    latency_first = "latency_first"
    throughput_first = "throughput_first"
    cost_first = "cost_first"
    carbon_first = "carbon_first"
    balanced = "balanced"


class PlanetaryWorkload(BaseModel):
    workload_id: str
    tier: SchedulingTier = SchedulingTier.interactive
    strategy: SchedulingStrategy = SchedulingStrategy.balanced
    source_continent: str = ""
    preferred_regions: List[str] = Field(default_factory=list)
    gpu_required: bool = False
    gpu_type: str = ""
    min_vram_gb: float = 0.0
    max_latency_ms: float = 0.0
    estimated_duration_ms: float = 0.0
    priority: int = 5
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SchedulingDecision(BaseModel):
    workload_id: str
    assigned_region: str = ""
    assigned_continent: str = ""
    score: float = 0.0
    latency_estimate_ms: float = 0.0
    factors: Dict[str, float] = Field(default_factory=dict)
    alternatives: List[str] = Field(default_factory=list)
    decision_time_ms: float = 0.0
    ts: float = 0.0


class PlanetaryScheduler:
    """Coordinates workloads globally across planetary-scale infrastructure.

    Supports multiple scheduling strategies (latency, throughput, cost, carbon)
    and tiers (realtime, interactive, batch, background) for millions of
    concurrent requests.
    """

    def __init__(self, max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._decisions: List[SchedulingDecision] = []
        self._total_scheduled = 0
        self._by_tier: Dict[str, int] = {}
        self._by_strategy: Dict[str, int] = {}
        self._events: List[Dict[str, Any]] = []

    def schedule(self, workload: PlanetaryWorkload, telemetry: Dict[str, Any]) -> Optional[SchedulingDecision]:
        start = now_ts()
        regions = telemetry.get("regions", [])
        if not regions:
            return None

        candidates = self._filter_candidates(workload, regions)
        if not candidates:
            candidates = regions

        scored = []
        for r in candidates:
            score, factors = self._score_region(workload, r, telemetry)
            scored.append((score, r, factors))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_region, best_factors = scored[0]
        alternatives = [r.get("region_id", "") for _, r, _ in scored[1:4]]

        decision = SchedulingDecision(
            workload_id=workload.workload_id,
            assigned_region=best_region.get("region_id", ""),
            assigned_continent=best_region.get("continent", ""),
            score=round(best_score, 4),
            latency_estimate_ms=round(best_region.get("avg_latency_ms", 0), 1),
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
            self._by_tier[workload.tier.value] = self._by_tier.get(workload.tier.value, 0) + 1
            self._by_strategy[workload.strategy.value] = self._by_strategy.get(workload.strategy.value, 0) + 1
            self._events.append({
                "type": "workload_scheduled",
                "workload_id": workload.workload_id,
                "region": decision.assigned_region,
                "score": decision.score,
                "ts": decision.ts,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return decision

    def schedule_batch(self, workloads: List[PlanetaryWorkload], telemetry: Dict[str, Any]) -> List[SchedulingDecision]:
        sorted_wl = sorted(workloads, key=lambda w: w.priority, reverse=True)
        results = []
        for wl in sorted_wl:
            decision = self.schedule(wl, telemetry)
            if decision:
                results.append(decision)
        return results

    def _filter_candidates(self, workload: PlanetaryWorkload, regions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        candidates = [r for r in regions if r.get("status", "active") != "offline"]

        if workload.gpu_required:
            gpu_candidates = [r for r in candidates if r.get("available_gpus", 0) > 0 or r.get("total_gpus", 0) > 0]
            if gpu_candidates:
                candidates = gpu_candidates

        if workload.max_latency_ms > 0:
            latency_candidates = [r for r in candidates if r.get("avg_latency_ms", 0) <= workload.max_latency_ms or r.get("avg_latency_ms", 0) == 0]
            if latency_candidates:
                candidates = latency_candidates

        if workload.preferred_regions:
            preferred = [r for r in candidates if r.get("region_id", "") in workload.preferred_regions]
            if preferred:
                candidates = preferred

        return candidates

    def _score_region(self, workload: PlanetaryWorkload, region: Dict[str, Any], telemetry: Dict[str, Any]) -> tuple:
        latency = region.get("avg_latency_ms", 50)
        latency_score = max(0.0, 1.0 - latency / 500.0)

        saturation = region.get("workload_saturation", 0.5)
        load_score = 1.0 - saturation

        gpu_util = region.get("gpu_utilization", 0.5)
        gpu_score = 1.0 - gpu_util if workload.gpu_required else 0.5

        cost = region.get("cost_per_hour", 50)
        cost_score = max(0.0, 1.0 - cost / 200.0)

        carbon = region.get("carbon_intensity", 0.5)
        carbon_score = max(0.0, 1.0 - carbon)

        renewable = region.get("renewable_pct", 0.0)
        energy_score = renewable

        thermal = region.get("thermal_pressure", 0.3)
        thermal_score = max(0.0, 1.0 - thermal)

        bandwidth = region.get("bandwidth_utilization", 0.3)
        bw_score = max(0.0, 1.0 - bandwidth)

        if workload.strategy == SchedulingStrategy.latency_first:
            weights = {"latency": 0.40, "load": 0.20, "gpu": 0.15, "cost": 0.05, "carbon": 0.02, "energy": 0.03, "thermal": 0.05, "bandwidth": 0.10}
        elif workload.strategy == SchedulingStrategy.throughput_first:
            weights = {"latency": 0.10, "load": 0.35, "gpu": 0.20, "cost": 0.05, "carbon": 0.02, "energy": 0.03, "thermal": 0.10, "bandwidth": 0.15}
        elif workload.strategy == SchedulingStrategy.cost_first:
            weights = {"latency": 0.10, "load": 0.10, "gpu": 0.10, "cost": 0.40, "carbon": 0.05, "energy": 0.10, "thermal": 0.05, "bandwidth": 0.10}
        elif workload.strategy == SchedulingStrategy.carbon_first:
            weights = {"latency": 0.05, "load": 0.10, "gpu": 0.10, "cost": 0.10, "carbon": 0.30, "energy": 0.25, "thermal": 0.05, "bandwidth": 0.05}
        else:
            weights = {"latency": 0.20, "load": 0.20, "gpu": 0.15, "cost": 0.15, "carbon": 0.05, "energy": 0.05, "thermal": 0.10, "bandwidth": 0.10}

        scores = {
            "latency": latency_score,
            "load": load_score,
            "gpu": gpu_score,
            "cost": cost_score,
            "carbon": carbon_score,
            "energy": energy_score,
            "thermal": thermal_score,
            "bandwidth": bw_score,
        }

        total = sum(weights[k] * scores[k] for k in weights)

        if workload.tier == SchedulingTier.realtime:
            total += 0.05 * latency_score
        elif workload.tier == SchedulingTier.background:
            total += 0.05 * cost_score

        factors = {k: round(scores[k], 3) for k in scores}
        return total, factors

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
            avg_score = sum(d.score for d in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_scheduled": self._total_scheduled,
                "avg_decision_time_ms": round(avg_time, 2),
                "avg_score": round(avg_score, 4),
                "by_tier": dict(self._by_tier),
                "by_strategy": dict(self._by_strategy),
            }
