from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from regions.regional_capacity import RegionalCapacityTracker
from shared.utils import now_ts, setup_logging

logger = setup_logging("fabric.placement")


class PlacementRequest(BaseModel):
    workload_id: str
    require_gpu: bool = False
    gpu_type: str = ""
    min_vram_gb: float = 0.0
    preferred_region: str = ""
    max_latency_ms: float = 0.0
    cost_weight: float = 0.3
    performance_weight: float = 0.5
    efficiency_weight: float = 0.2
    power_efficiency_weight: float = 0.0
    thermal_weight: float = 0.0
    historical_success_weight: float = 0.0
    predicted_traffic_weight: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PlacementDecision(BaseModel):
    workload_id: str
    region_id: str
    score: float = 0.0
    factors: Dict[str, float] = Field(default_factory=dict)
    alternatives: List[str] = Field(default_factory=list)
    decision_time_ms: float = 0.0
    ts: float = 0.0


class PlacementEngine:
    """Decides workload placement globally based on multi-factor scoring.

    Factors: latency, GPU type/VRAM, cost efficiency, bandwidth,
    thermal state, energy utilization, regional congestion.
    """

    def __init__(
        self,
        registry: RegionRegistry,
        capacity_tracker: RegionalCapacityTracker,
        intelligence_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.registry = registry
        self.capacity = capacity_tracker
        self._intelligence = intelligence_context or {}
        self._lock = threading.Lock()
        self._decisions: List[PlacementDecision] = []
        self._total_placed = 0

    def set_intelligence_context(self, ctx: Dict[str, Any]) -> None:
        self._intelligence = ctx

    def place(self, request: PlacementRequest) -> Optional[PlacementDecision]:
        start = now_ts()
        candidates = self._filter_candidates(request)
        if not candidates:
            return None

        scored = []
        for region in candidates:
            score, factors = self._score_region(request, region)
            scored.append((score, region, factors))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_region, best_factors = scored[0]
        alternatives = [r.region_id for _, r, _ in scored[1:4]]

        decision = PlacementDecision(
            workload_id=request.workload_id,
            region_id=best_region.region_id,
            score=round(best_score, 4),
            factors=best_factors,
            alternatives=alternatives,
            decision_time_ms=round((now_ts() - start) * 1000, 2),
            ts=now_ts(),
        )

        with self._lock:
            self._decisions.append(decision)
            if len(self._decisions) > 500:
                self._decisions = self._decisions[-500:]
            self._total_placed += 1

        return decision

    def _filter_candidates(self, request: PlacementRequest) -> List[RegionRecord]:
        candidates = self.registry.active_regions()
        if request.require_gpu:
            candidates = [r for r in candidates if r.gpu_inventory.available_gpus > 0]
        if request.min_vram_gb > 0:
            candidates = [
                r for r in candidates
                if r.gpu_inventory.available_vram_gb >= request.min_vram_gb
            ]
        if request.gpu_type:
            candidates = [
                r for r in candidates
                if request.gpu_type in r.gpu_inventory.gpu_types
            ]
        if request.max_latency_ms > 0:
            candidates = [
                r for r in candidates
                if r.avg_latency_ms <= request.max_latency_ms or r.avg_latency_ms == 0
            ]
        return candidates

    def _score_region(self, request: PlacementRequest, region: RegionRecord) -> tuple[float, Dict[str, float]]:
        gpu_score = self._gpu_score(region, request)
        load_score = 1.0 - region.workload_saturation
        latency_score = max(0.0, 1.0 - region.avg_latency_ms / 500.0) if region.avg_latency_ms > 0 else 0.7
        capacity_score = min(region.capacity, 1.0)
        connectivity_score = 1.0 if region.edge_connectivity else 0.5

        cost_factor = load_score * 0.6 + capacity_score * 0.4
        perf_factor = latency_score * 0.5 + gpu_score * 0.5
        efficiency_factor = connectivity_score * 0.4 + (1.0 - region.workload_saturation) * 0.6

        score = (
            request.cost_weight * cost_factor
            + request.performance_weight * perf_factor
            + request.efficiency_weight * efficiency_factor
        )

        if request.preferred_region and region.region_id == request.preferred_region:
            score += 0.10

        if region.status == RegionStatus.degraded:
            score *= 0.7

        power_score = 0.5
        thermal_score = 0.5
        historical_score = 0.5
        predicted_score = 0.5

        intel = self._intelligence.get(region.region_id, {})
        if intel:
            renewable = intel.get("renewable_pct", 0.0)
            power_score = 0.3 + renewable * 0.7

            thermal_p = intel.get("thermal_pressure", 0.0)
            thermal_score = max(0.0, 1.0 - thermal_p)

            historical_score = intel.get("historical_success_rate", 0.5)

            predicted_sat = intel.get("predicted_saturation", 0.0)
            predicted_score = max(0.0, 1.0 - predicted_sat)

        if request.power_efficiency_weight > 0:
            score += request.power_efficiency_weight * power_score
        if request.thermal_weight > 0:
            score += request.thermal_weight * thermal_score
        if request.historical_success_weight > 0:
            score += request.historical_success_weight * historical_score
        if request.predicted_traffic_weight > 0:
            score += request.predicted_traffic_weight * predicted_score

        factors = {
            "gpu": round(gpu_score, 3),
            "load": round(load_score, 3),
            "latency": round(latency_score, 3),
            "capacity": round(capacity_score, 3),
            "connectivity": round(connectivity_score, 3),
            "cost": round(cost_factor, 3),
            "performance": round(perf_factor, 3),
            "efficiency": round(efficiency_factor, 3),
            "power": round(power_score, 3),
            "thermal": round(thermal_score, 3),
            "historical": round(historical_score, 3),
            "predicted": round(predicted_score, 3),
        }
        return score, factors

    def _gpu_score(self, region: RegionRecord, request: PlacementRequest) -> float:
        inv = region.gpu_inventory
        if not request.require_gpu:
            return 0.5
        if inv.total_gpus == 0:
            return 0.0
        base = inv.available_gpus / inv.total_gpus
        if request.gpu_type and request.gpu_type in inv.gpu_types:
            base += 0.1
        if request.min_vram_gb > 0 and inv.available_vram_gb >= request.min_vram_gb * 2:
            base += 0.1
        return min(base, 1.0)

    def recent_decisions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.model_dump() for d in reversed(self._decisions)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._decisions[-100:] if self._decisions else []
            avg_time = sum(d.decision_time_ms for d in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_placed": self._total_placed,
                "avg_decision_time_ms": round(avg_time, 2),
            }
