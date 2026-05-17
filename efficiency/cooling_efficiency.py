"""v15 Cooling Efficiency.

Monitors and optimizes cooling systems across planetary infrastructure,
tracking PUE (Power Usage Effectiveness), thermal zones, and cooling
capacity for workload-aware thermal management.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("efficiency.cooling")


class CoolingMode(str, Enum):
    free_cooling = "free_cooling"
    hybrid = "hybrid"
    mechanical = "mechanical"
    liquid = "liquid"
    immersion = "immersion"


class ThermalZoneStatus(str, Enum):
    optimal = "optimal"
    warm = "warm"
    hot = "hot"
    critical = "critical"


class CoolingProfile(BaseModel):
    region_id: str
    cooling_mode: CoolingMode = CoolingMode.hybrid
    pue: float = 1.5
    ambient_temp_c: float = 25.0
    inlet_temp_c: float = 22.0
    exhaust_temp_c: float = 35.0
    cooling_capacity_kw: float = 500.0
    cooling_load_kw: float = 250.0
    thermal_zone_status: ThermalZoneStatus = ThermalZoneStatus.optimal
    free_cooling_available: bool = False
    gpu_avg_temp_c: float = 65.0
    gpu_max_temp_c: float = 80.0
    updated_at: float = 0.0


class CoolingRecommendation(BaseModel):
    region_id: str
    action: str = ""
    reason: str = ""
    estimated_pue_improvement: float = 0.0
    priority: str = "normal"
    ts: float = 0.0


class CoolingEfficiencyManager:
    """Monitors and optimizes cooling efficiency across planetary
    infrastructure for thermal-aware workload management."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._profiles: Dict[str, CoolingProfile] = {}
        self._recommendations: List[CoolingRecommendation] = []
        self._events: List[Dict[str, Any]] = []

    def update_profile(self, profile: CoolingProfile) -> None:
        profile.updated_at = now_ts()
        load_ratio = profile.cooling_load_kw / profile.cooling_capacity_kw if profile.cooling_capacity_kw > 0 else 0
        if load_ratio > 0.9 or profile.gpu_max_temp_c > 90:
            profile.thermal_zone_status = ThermalZoneStatus.critical
        elif load_ratio > 0.7 or profile.gpu_max_temp_c > 80:
            profile.thermal_zone_status = ThermalZoneStatus.hot
        elif load_ratio > 0.5 or profile.gpu_avg_temp_c > 70:
            profile.thermal_zone_status = ThermalZoneStatus.warm
        else:
            profile.thermal_zone_status = ThermalZoneStatus.optimal

        if profile.ambient_temp_c < 18:
            profile.free_cooling_available = True

        with self._lock:
            self._profiles[profile.region_id] = profile

    def update_from_telemetry(self, telemetry: Dict[str, Any]) -> None:
        for r in telemetry.get("regions", []):
            profile = CoolingProfile(
                region_id=r.get("region_id", ""),
                pue=r.get("pue", 1.5),
                ambient_temp_c=r.get("ambient_temp_c", 25),
                inlet_temp_c=r.get("inlet_temp_c", 22),
                exhaust_temp_c=r.get("exhaust_temp_c", 35),
                cooling_capacity_kw=r.get("cooling_capacity_kw", 500),
                cooling_load_kw=r.get("cooling_load_kw", 250),
                gpu_avg_temp_c=r.get("gpu_temp_c", 65),
                gpu_max_temp_c=r.get("gpu_max_temp_c", 80),
            )
            thermal = r.get("thermal_pressure", 0.3)
            if thermal > 0.8:
                profile.cooling_mode = CoolingMode.liquid
            elif thermal > 0.5:
                profile.cooling_mode = CoolingMode.mechanical
            self.update_profile(profile)

    def analyze(self) -> List[CoolingRecommendation]:
        recommendations = []
        with self._lock:
            profiles = list(self._profiles.values())

        for p in profiles:
            if p.thermal_zone_status == ThermalZoneStatus.critical:
                recommendations.append(CoolingRecommendation(
                    region_id=p.region_id,
                    action="emergency_throttle",
                    reason=f"Critical thermal: GPU max {p.gpu_max_temp_c}C, cooling at capacity",
                    estimated_pue_improvement=0.0,
                    priority="critical",
                    ts=now_ts(),
                ))
            elif p.free_cooling_available and p.cooling_mode != CoolingMode.free_cooling:
                recommendations.append(CoolingRecommendation(
                    region_id=p.region_id,
                    action="switch_to_free_cooling",
                    reason=f"Ambient {p.ambient_temp_c}C allows free cooling",
                    estimated_pue_improvement=round(p.pue - 1.1, 2),
                    priority="normal",
                    ts=now_ts(),
                ))
            elif p.pue > 1.8:
                recommendations.append(CoolingRecommendation(
                    region_id=p.region_id,
                    action="optimize_cooling",
                    reason=f"PUE {p.pue:.2f} above target 1.5",
                    estimated_pue_improvement=round(p.pue - 1.5, 2),
                    priority="normal",
                    ts=now_ts(),
                ))

        with self._lock:
            self._recommendations.extend(recommendations)
            if len(self._recommendations) > self._max_history:
                self._recommendations = self._recommendations[-self._max_history:]

        return recommendations

    def coolest_regions(self, limit: int = 5) -> List[Dict[str, Any]]:
        with self._lock:
            sorted_profiles = sorted(self._profiles.values(), key=lambda p: p.gpu_avg_temp_c)
            return [p.model_dump(mode="json") for p in sorted_profiles[:limit]]

    def hottest_regions(self, limit: int = 5) -> List[Dict[str, Any]]:
        with self._lock:
            sorted_profiles = sorted(self._profiles.values(), key=lambda p: p.gpu_avg_temp_c, reverse=True)
            return [p.model_dump(mode="json") for p in sorted_profiles[:limit]]

    def cooling_summary(self) -> Dict[str, Any]:
        with self._lock:
            profiles = list(self._profiles.values())
            if not profiles:
                return {"ts": now_ts(), "regions": 0}
            avg_pue = sum(p.pue for p in profiles) / len(profiles)
            avg_gpu_temp = sum(p.gpu_avg_temp_c for p in profiles) / len(profiles)
            by_status = {}
            for p in profiles:
                by_status[p.thermal_zone_status.value] = by_status.get(p.thermal_zone_status.value, 0) + 1
            return {
                "ts": now_ts(),
                "regions": len(profiles),
                "avg_pue": round(avg_pue, 2),
                "avg_gpu_temp_c": round(avg_gpu_temp, 1),
                "by_thermal_status": by_status,
            }

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_profiles": len(self._profiles),
                "total_recommendations": len(self._recommendations),
            }
