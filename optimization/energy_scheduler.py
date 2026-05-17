from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("optimization.energy")


class EnergyTier(str, Enum):
    green = "green"
    standard = "standard"
    peak = "peak"


class EnergySchedulingAction(BaseModel):
    action_id: str
    region_id: str = ""
    action_type: str = ""
    current_energy_kwh: float = 0.0
    projected_savings_kwh: float = 0.0
    energy_tier: EnergyTier = EnergyTier.standard
    workloads_affected: int = 0
    description: str = ""
    executed: bool = False
    created_at: float = 0.0


class RegionEnergyProfile(BaseModel):
    region_id: str
    energy_cost_per_kwh: float = 0.0
    current_power_w: float = 0.0
    energy_tier: EnergyTier = EnergyTier.standard
    renewable_pct: float = 0.0
    carbon_intensity: float = 0.0
    efficiency_pue: float = 1.5
    ts: float = 0.0


class EnergyScheduler:
    """Schedules workloads based on energy costs and availability, preferring
    regions with green energy, lower carbon intensity, and off-peak pricing."""

    def __init__(
        self,
        peak_cost_threshold: float = 0.15,
        green_renewable_threshold: float = 0.5,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._peak_threshold = peak_cost_threshold
        self._green_threshold = green_renewable_threshold
        self._max_history = max_history
        self._profiles: Dict[str, RegionEnergyProfile] = {}
        self._actions: List[EnergySchedulingAction] = []
        self._total_scheduled = 0
        self._events: List[Dict[str, Any]] = []

    def _classify_tier(self, cost: float, renewable_pct: float) -> EnergyTier:
        if renewable_pct >= self._green_threshold:
            return EnergyTier.green
        if cost > self._peak_threshold:
            return EnergyTier.peak
        return EnergyTier.standard

    def analyze(self, telemetry: Dict[str, Any]) -> List[EnergySchedulingAction]:
        actions: List[EnergySchedulingAction] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            energy_cost = r.get("energy_cost_per_kwh", 0.10)
            power_w = r.get("power_consumption_w", 0.0)
            renewable = r.get("renewable_pct", 0.0)
            carbon = r.get("carbon_intensity", 0.5)
            pue = r.get("pue", 1.5)

            tier = self._classify_tier(energy_cost, renewable)

            profile = RegionEnergyProfile(
                region_id=region_id,
                energy_cost_per_kwh=round(energy_cost, 4),
                current_power_w=round(power_w, 1),
                energy_tier=tier,
                renewable_pct=round(renewable, 3),
                carbon_intensity=round(carbon, 3),
                efficiency_pue=round(pue, 2),
                ts=ts,
            )
            with self._lock:
                self._profiles[region_id] = profile

            if tier == EnergyTier.peak:
                actions.append(EnergySchedulingAction(
                    action_id=f"energy-defer-{region_id}-{int(ts)}",
                    region_id=region_id,
                    action_type="defer_to_offpeak",
                    current_energy_kwh=round(power_w / 1000, 3),
                    projected_savings_kwh=round(power_w / 1000 * 0.3, 3),
                    energy_tier=tier,
                    description=f"Defer non-critical workloads from {region_id} (peak pricing)",
                    created_at=ts,
                ))

            if tier == EnergyTier.green and r.get("workload_saturation", 0) < 0.5:
                actions.append(EnergySchedulingAction(
                    action_id=f"energy-attract-{region_id}-{int(ts)}",
                    region_id=region_id,
                    action_type="attract_workloads",
                    energy_tier=tier,
                    description=f"Route workloads to {region_id} (green energy, low load)",
                    created_at=ts,
                ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute(self, action: EnergySchedulingAction) -> EnergySchedulingAction:
        action.executed = True
        with self._lock:
            self._total_scheduled += 1
            self._events.append({
                "type": "energy_scheduling_executed",
                "action_id": action.action_id,
                "action_type": action.action_type,
                "region": action.region_id,
                "tier": action.energy_tier.value,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        return action

    def schedule(self, telemetry: Dict[str, Any]) -> List[EnergySchedulingAction]:
        actions = self.analyze(telemetry)
        for a in actions:
            self.execute(a)
        return actions

    def green_regions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._profiles.values()
                    if p.energy_tier == EnergyTier.green]

    def energy_map(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {rid: p.model_dump(mode="json") for rid, p in self._profiles.items()}

    def recent_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in reversed(self._actions)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            profiles = list(self._profiles.values())
            total_power = sum(p.current_power_w for p in profiles)
            avg_renewable = sum(p.renewable_pct for p in profiles) / len(profiles) if profiles else 0.0
            return {
                "ts": now_ts(),
                "total_scheduled": self._total_scheduled,
                "total_power_w": round(total_power, 1),
                "avg_renewable_pct": round(avg_renewable, 3),
                "green_regions": sum(1 for p in profiles if p.energy_tier == EnergyTier.green),
                "peak_regions": sum(1 for p in profiles if p.energy_tier == EnergyTier.peak),
            }
