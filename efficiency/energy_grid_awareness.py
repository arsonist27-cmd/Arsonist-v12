"""v15 Energy Grid Awareness.

Monitors and models regional energy grid conditions including renewable
availability, grid load, peak/off-peak pricing, and power reliability
for energy-aware infrastructure decisions.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("efficiency.energy_grid")


class GridStatus(str, Enum):
    normal = "normal"
    peak = "peak"
    off_peak = "off_peak"
    stressed = "stressed"
    emergency = "emergency"


class EnergyGrid(BaseModel):
    region_id: str
    continent: str = ""
    grid_status: GridStatus = GridStatus.normal
    renewable_available_mw: float = 0.0
    total_capacity_mw: float = 100.0
    current_load_mw: float = 50.0
    renewable_pct: float = 0.0
    price_per_kwh: float = 0.10
    carbon_intensity_gco2: float = 400.0
    solar_available: bool = False
    wind_available: bool = False
    hydro_available: bool = False
    reliability_score: float = 0.99
    updated_at: float = 0.0


class GridRecommendation(BaseModel):
    region_id: str
    action: str = ""
    reason: str = ""
    priority: str = "normal"
    estimated_savings_pct: float = 0.0
    ts: float = 0.0


class EnergyGridManager:
    """Monitors energy grid conditions across regions and provides
    recommendations for energy-aware workload scheduling."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._grids: Dict[str, EnergyGrid] = {}
        self._recommendations: List[GridRecommendation] = []
        self._events: List[Dict[str, Any]] = []

    def update_grid(self, grid: EnergyGrid) -> None:
        grid.updated_at = now_ts()
        load_ratio = grid.current_load_mw / grid.total_capacity_mw if grid.total_capacity_mw > 0 else 0
        if load_ratio > 0.9:
            grid.grid_status = GridStatus.emergency
        elif load_ratio > 0.8:
            grid.grid_status = GridStatus.stressed
        elif load_ratio > 0.6:
            grid.grid_status = GridStatus.peak
        elif load_ratio < 0.3:
            grid.grid_status = GridStatus.off_peak
        else:
            grid.grid_status = GridStatus.normal

        if grid.total_capacity_mw > 0:
            grid.renewable_pct = grid.renewable_available_mw / grid.total_capacity_mw

        with self._lock:
            self._grids[grid.region_id] = grid

    def update_from_telemetry(self, telemetry: Dict[str, Any]) -> None:
        for r in telemetry.get("regions", []):
            grid = EnergyGrid(
                region_id=r.get("region_id", ""),
                continent=r.get("continent", ""),
                renewable_available_mw=r.get("renewable_mw", 50),
                total_capacity_mw=r.get("total_power_mw", 100),
                current_load_mw=r.get("current_load_mw", 50),
                renewable_pct=r.get("renewable_pct", 0.0),
                price_per_kwh=r.get("power_cost_kwh", 0.10),
                carbon_intensity_gco2=r.get("carbon_intensity", 0.5) * 1000,
                solar_available=r.get("solar", False),
                wind_available=r.get("wind", False),
                hydro_available=r.get("hydro", False),
                reliability_score=r.get("grid_reliability", 0.99),
            )
            self.update_grid(grid)

    def analyze(self) -> List[GridRecommendation]:
        recommendations = []
        with self._lock:
            grids = list(self._grids.values())

        for grid in grids:
            if grid.grid_status == GridStatus.emergency:
                recommendations.append(GridRecommendation(
                    region_id=grid.region_id,
                    action="reduce_load",
                    reason=f"Grid emergency: load at {grid.current_load_mw}/{grid.total_capacity_mw}MW",
                    priority="critical",
                    estimated_savings_pct=20.0,
                    ts=now_ts(),
                ))
            elif grid.grid_status == GridStatus.off_peak and grid.renewable_pct > 0.5:
                recommendations.append(GridRecommendation(
                    region_id=grid.region_id,
                    action="increase_load",
                    reason=f"Off-peak with {grid.renewable_pct:.0%} renewable, favorable for batch work",
                    priority="low",
                    estimated_savings_pct=15.0,
                    ts=now_ts(),
                ))
            elif grid.price_per_kwh > 0.20:
                recommendations.append(GridRecommendation(
                    region_id=grid.region_id,
                    action="shift_workloads",
                    reason=f"High energy price ${grid.price_per_kwh:.2f}/kWh",
                    priority="normal",
                    estimated_savings_pct=10.0,
                    ts=now_ts(),
                ))

        with self._lock:
            self._recommendations.extend(recommendations)
            if len(self._recommendations) > self._max_history:
                self._recommendations = self._recommendations[-self._max_history:]

        return recommendations

    def best_regions_for_batch(self, limit: int = 5) -> List[Dict[str, Any]]:
        with self._lock:
            eligible = [g for g in self._grids.values()
                        if g.grid_status in (GridStatus.normal, GridStatus.off_peak)]
            sorted_grids = sorted(eligible, key=lambda g: (g.renewable_pct, -g.price_per_kwh), reverse=True)
            return [g.model_dump(mode="json") for g in sorted_grids[:limit]]

    def grid_summary(self) -> Dict[str, Any]:
        with self._lock:
            grids = list(self._grids.values())
            if not grids:
                return {"ts": now_ts(), "regions": 0}
            avg_renewable = sum(g.renewable_pct for g in grids) / len(grids)
            avg_price = sum(g.price_per_kwh for g in grids) / len(grids)
            by_status = {}
            for g in grids:
                by_status[g.grid_status.value] = by_status.get(g.grid_status.value, 0) + 1
            return {
                "ts": now_ts(),
                "regions": len(grids),
                "avg_renewable_pct": round(avg_renewable, 3),
                "avg_price_per_kwh": round(avg_price, 4),
                "by_status": by_status,
            }

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_grids": len(self._grids),
                "total_recommendations": len(self._recommendations),
            }
