"""v15 Carbon + Energy Optimization.

Optimizes workloads based on renewable energy availability, power cost,
cooling efficiency, and carbon intensity. Supports energy-aware scheduling,
carbon-efficient routing, and green workload placement.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("efficiency.carbon_optimizer")


class CarbonTier(str, Enum):
    green = "green"
    mixed = "mixed"
    brown = "brown"
    critical = "critical"


class EnergySource(str, Enum):
    solar = "solar"
    wind = "wind"
    hydro = "hydro"
    nuclear = "nuclear"
    natural_gas = "natural_gas"
    coal = "coal"
    grid_mix = "grid_mix"


class RegionCarbonProfile(BaseModel):
    region_id: str
    continent: str = ""
    carbon_intensity_gco2_kwh: float = 0.0
    renewable_pct: float = 0.0
    power_cost_per_kwh: float = 0.0
    cooling_efficiency_pue: float = 1.5
    primary_source: EnergySource = EnergySource.grid_mix
    tier: CarbonTier = CarbonTier.mixed
    current_power_w: float = 0.0
    green_capacity_remaining: float = 1.0
    updated_at: float = 0.0


class CarbonPlacement(BaseModel):
    workload_id: str
    selected_region: str = ""
    carbon_score: float = 0.0
    estimated_co2_g: float = 0.0
    energy_cost: float = 0.0
    renewable_pct: float = 0.0
    factors: Dict[str, float] = Field(default_factory=dict)
    ts: float = 0.0


class CarbonOptimizer:
    """Optimizes workload placement and routing for minimum carbon footprint
    and maximum renewable energy usage across planetary infrastructure."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._profiles: Dict[str, RegionCarbonProfile] = {}
        self._placements: List[CarbonPlacement] = []
        self._total_optimized = 0
        self._total_co2_saved_g = 0.0
        self._events: List[Dict[str, Any]] = []

    def update_profile(self, profile: RegionCarbonProfile) -> None:
        profile.updated_at = now_ts()
        if profile.carbon_intensity_gco2_kwh < 100:
            profile.tier = CarbonTier.green
        elif profile.carbon_intensity_gco2_kwh < 400:
            profile.tier = CarbonTier.mixed
        elif profile.carbon_intensity_gco2_kwh < 700:
            profile.tier = CarbonTier.brown
        else:
            profile.tier = CarbonTier.critical
        with self._lock:
            self._profiles[profile.region_id] = profile

    def update_from_telemetry(self, telemetry: Dict[str, Any]) -> None:
        for r in telemetry.get("regions", []):
            carbon_gco2 = r.get("carbon_intensity", 0.5) * 1000
            profile = RegionCarbonProfile(
                region_id=r.get("region_id", ""),
                continent=r.get("continent", ""),
                carbon_intensity_gco2_kwh=carbon_gco2,
                renewable_pct=r.get("renewable_pct", 0.0),
                power_cost_per_kwh=r.get("power_cost_kwh", 0.10),
                cooling_efficiency_pue=r.get("pue", 1.5),
                current_power_w=r.get("power_consumption_w", 0.0),
                green_capacity_remaining=max(0, 1.0 - r.get("workload_saturation", 0.5)),
            )
            self.update_profile(profile)

    def optimize_placement(self, workload_id: str, estimated_kwh: float = 1.0,
                           regions: Optional[List[Dict[str, Any]]] = None) -> Optional[CarbonPlacement]:
        with self._lock:
            profiles = list(self._profiles.values())
        if not profiles:
            return None

        scored = []
        for p in profiles:
            score, factors = self._score_region(p)
            estimated_co2 = p.carbon_intensity_gco2_kwh * estimated_kwh * p.cooling_efficiency_pue
            energy_cost = p.power_cost_per_kwh * estimated_kwh * p.cooling_efficiency_pue
            scored.append((score, p, factors, estimated_co2, energy_cost))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_profile, factors, co2, cost = scored[0]

        worst_co2 = max(s[3] for s in scored) if scored else co2
        co2_saved = worst_co2 - co2

        placement = CarbonPlacement(
            workload_id=workload_id,
            selected_region=best_profile.region_id,
            carbon_score=round(best_score, 4),
            estimated_co2_g=round(co2, 2),
            energy_cost=round(cost, 4),
            renewable_pct=round(best_profile.renewable_pct, 3),
            factors=factors,
            ts=now_ts(),
        )

        with self._lock:
            self._placements.append(placement)
            if len(self._placements) > self._max_history:
                self._placements = self._placements[-self._max_history:]
            self._total_optimized += 1
            self._total_co2_saved_g += max(0, co2_saved)

        return placement

    def _score_region(self, profile: RegionCarbonProfile) -> tuple:
        carbon_score = max(0.0, 1.0 - profile.carbon_intensity_gco2_kwh / 1000.0)
        renewable_score = profile.renewable_pct
        cost_score = max(0.0, 1.0 - profile.power_cost_per_kwh / 0.30)
        pue_score = max(0.0, 1.0 - (profile.cooling_efficiency_pue - 1.0) / 1.0)
        capacity_score = profile.green_capacity_remaining

        total = (carbon_score * 0.35 + renewable_score * 0.25 + cost_score * 0.15 +
                 pue_score * 0.10 + capacity_score * 0.15)

        factors = {
            "carbon": round(carbon_score, 3),
            "renewable": round(renewable_score, 3),
            "cost": round(cost_score, 3),
            "pue": round(pue_score, 3),
            "capacity": round(capacity_score, 3),
        }
        return total, factors

    def greenest_regions(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            sorted_profiles = sorted(self._profiles.values(),
                                     key=lambda p: p.carbon_intensity_gco2_kwh)
            return [p.model_dump(mode="json") for p in sorted_profiles[:limit]]

    def brownest_regions(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            sorted_profiles = sorted(self._profiles.values(),
                                     key=lambda p: p.carbon_intensity_gco2_kwh, reverse=True)
            return [p.model_dump(mode="json") for p in sorted_profiles[:limit]]

    def recent_placements(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in reversed(self._placements)][:limit]

    def carbon_summary(self) -> Dict[str, Any]:
        with self._lock:
            profiles = list(self._profiles.values())
            if not profiles:
                return {"ts": now_ts(), "regions": 0}
            avg_carbon = sum(p.carbon_intensity_gco2_kwh for p in profiles) / len(profiles)
            avg_renewable = sum(p.renewable_pct for p in profiles) / len(profiles)
            avg_pue = sum(p.cooling_efficiency_pue for p in profiles) / len(profiles)
            by_tier = {}
            for p in profiles:
                by_tier[p.tier.value] = by_tier.get(p.tier.value, 0) + 1
            return {
                "ts": now_ts(),
                "regions": len(profiles),
                "avg_carbon_gco2_kwh": round(avg_carbon, 1),
                "avg_renewable_pct": round(avg_renewable, 3),
                "avg_pue": round(avg_pue, 2),
                "by_tier": by_tier,
            }

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_optimized": self._total_optimized,
                "total_co2_saved_g": round(self._total_co2_saved_g, 2),
                "profiles": len(self._profiles),
            }
