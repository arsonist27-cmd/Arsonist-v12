"""v15 Geo Optimizer.

Optimizes workload placement and routing across geographic regions,
considering latency, capacity, cost, energy, and regulatory constraints.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("planetary.geo_optimizer")


class GeoConstraint(BaseModel):
    constraint_id: str = ""
    constraint_type: str = "latency"
    max_latency_ms: float = 0.0
    preferred_continents: List[str] = Field(default_factory=list)
    excluded_regions: List[str] = Field(default_factory=list)
    data_residency: str = ""
    max_carbon_intensity: float = 1.0


class GeoPlacement(BaseModel):
    workload_id: str
    selected_region: str = ""
    selected_continent: str = ""
    score: float = 0.0
    latency_ms: float = 0.0
    carbon_intensity: float = 0.0
    cost_score: float = 0.0
    factors: Dict[str, float] = Field(default_factory=dict)
    ts: float = 0.0


class GeoOptimizer:
    """Optimizes workload placement across geographic regions using
    multi-factor scoring with latency, cost, carbon, and capacity."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._placements: List[GeoPlacement] = []
        self._total_optimized = 0

    def optimize(self, workload_id: str, regions: List[Dict[str, Any]],
                 constraint: Optional[GeoConstraint] = None) -> Optional[GeoPlacement]:
        if not regions:
            return None

        candidates = self._apply_constraints(regions, constraint)
        if not candidates:
            candidates = regions

        scored = []
        for r in candidates:
            score, factors = self._score(r)
            scored.append((score, r, factors))
        scored.sort(key=lambda x: x[0], reverse=True)

        best_score, best, factors = scored[0]
        placement = GeoPlacement(
            workload_id=workload_id,
            selected_region=best.get("region_id", ""),
            selected_continent=best.get("continent", ""),
            score=round(best_score, 4),
            latency_ms=round(best.get("avg_latency_ms", 0), 1),
            carbon_intensity=round(best.get("carbon_intensity", 0.5), 3),
            cost_score=round(factors.get("cost", 0), 3),
            factors=factors,
            ts=now_ts(),
        )

        with self._lock:
            self._placements.append(placement)
            if len(self._placements) > self._max_history:
                self._placements = self._placements[-self._max_history:]
            self._total_optimized += 1

        return placement

    def _apply_constraints(self, regions: List[Dict[str, Any]],
                           constraint: Optional[GeoConstraint]) -> List[Dict[str, Any]]:
        if not constraint:
            return regions

        result = list(regions)

        if constraint.excluded_regions:
            result = [r for r in result if r.get("region_id", "") not in constraint.excluded_regions]

        if constraint.preferred_continents:
            preferred = [r for r in result if r.get("continent", "") in constraint.preferred_continents]
            if preferred:
                result = preferred

        if constraint.max_latency_ms > 0:
            filtered = [r for r in result if r.get("avg_latency_ms", 0) <= constraint.max_latency_ms]
            if filtered:
                result = filtered

        if constraint.max_carbon_intensity < 1.0:
            filtered = [r for r in result if r.get("carbon_intensity", 0.5) <= constraint.max_carbon_intensity]
            if filtered:
                result = filtered

        return result

    def _score(self, region: Dict[str, Any]) -> tuple:
        latency = region.get("avg_latency_ms", 50)
        latency_score = max(0.0, 1.0 - latency / 500.0)

        saturation = region.get("workload_saturation", 0.5)
        capacity_score = 1.0 - saturation

        cost = region.get("cost_per_hour", 50)
        cost_score = max(0.0, 1.0 - cost / 200.0)

        carbon = region.get("carbon_intensity", 0.5)
        carbon_score = max(0.0, 1.0 - carbon)

        renewable = region.get("renewable_pct", 0.0)
        energy_score = renewable

        thermal = region.get("thermal_pressure", 0.3)
        thermal_score = max(0.0, 1.0 - thermal)

        total = (latency_score * 0.25 + capacity_score * 0.20 + cost_score * 0.15 +
                 carbon_score * 0.15 + energy_score * 0.10 + thermal_score * 0.15)

        factors = {
            "latency": round(latency_score, 3),
            "capacity": round(capacity_score, 3),
            "cost": round(cost_score, 3),
            "carbon": round(carbon_score, 3),
            "energy": round(energy_score, 3),
            "thermal": round(thermal_score, 3),
        }
        return total, factors

    def recent_placements(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in reversed(self._placements)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._placements[-100:] if self._placements else []
            avg_score = sum(p.score for p in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_optimized": self._total_optimized,
                "avg_score": round(avg_score, 4),
            }
