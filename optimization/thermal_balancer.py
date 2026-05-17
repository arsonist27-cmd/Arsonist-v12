from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("optimization.thermal")


class ThermalZone(str, Enum):
    safe = "safe"
    warm = "warm"
    hot = "hot"
    critical = "critical"


class ThermalAction(BaseModel):
    action_id: str
    region_id: str = ""
    gpu_id: str = ""
    zone: ThermalZone = ThermalZone.safe
    current_temp_c: float = 0.0
    target_temp_c: float = 70.0
    power_watts: float = 0.0
    action_type: str = ""
    workloads_to_migrate: int = 0
    executed: bool = False
    created_at: float = 0.0


class ThermalSnapshot(BaseModel):
    region_id: str
    avg_temp_c: float = 0.0
    max_temp_c: float = 0.0
    thermal_pressure: float = 0.0
    zone: ThermalZone = ThermalZone.safe
    power_consumption_w: float = 0.0
    cooling_efficiency: float = 1.0
    gpu_count: int = 0
    ts: float = 0.0


class ThermalBalancer:
    """Monitors GPU temperatures and power consumption across regions,
    optimizes workload placement to avoid thermal hotspots, and extends
    GPU lifespan through thermal-aware scheduling."""

    def __init__(
        self,
        safe_temp_c: float = 65.0,
        warm_temp_c: float = 75.0,
        hot_temp_c: float = 85.0,
        critical_temp_c: float = 92.0,
        target_temp_c: float = 70.0,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._safe = safe_temp_c
        self._warm = warm_temp_c
        self._hot = hot_temp_c
        self._critical = critical_temp_c
        self._target = target_temp_c
        self._max_history = max_history
        self._snapshots: Dict[str, ThermalSnapshot] = {}
        self._actions: List[ThermalAction] = []
        self._total_rebalances = 0
        self._events: List[Dict[str, Any]] = []

    def _classify_zone(self, temp_c: float) -> ThermalZone:
        if temp_c >= self._critical:
            return ThermalZone.critical
        if temp_c >= self._hot:
            return ThermalZone.hot
        if temp_c >= self._warm:
            return ThermalZone.warm
        return ThermalZone.safe

    def monitor(self, telemetry: Dict[str, Any]) -> List[ThermalSnapshot]:
        snapshots: List[ThermalSnapshot] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            avg_temp = r.get("gpu_temp_c", r.get("avg_gpu_temp_c", 0.0))
            max_temp = r.get("max_gpu_temp_c", avg_temp)
            power = r.get("power_consumption_w", 0.0)
            gpu_count = r.get("total_gpus", 0)
            cooling = r.get("cooling_efficiency", 1.0)

            thermal_pressure = 0.0
            if avg_temp > 0:
                thermal_pressure = min(1.0, max(0.0, (avg_temp - self._safe) / (self._critical - self._safe)))

            snap = ThermalSnapshot(
                region_id=region_id,
                avg_temp_c=round(avg_temp, 1),
                max_temp_c=round(max_temp, 1),
                thermal_pressure=round(thermal_pressure, 3),
                zone=self._classify_zone(avg_temp),
                power_consumption_w=round(power, 1),
                cooling_efficiency=round(cooling, 3),
                gpu_count=gpu_count,
                ts=ts,
            )
            snapshots.append(snap)

            with self._lock:
                self._snapshots[region_id] = snap

        return snapshots

    def analyze(self, telemetry: Dict[str, Any]) -> List[ThermalAction]:
        snapshots = self.monitor(telemetry)
        actions: List[ThermalAction] = []
        ts = now_ts()

        for snap in snapshots:
            if snap.zone == ThermalZone.critical:
                actions.append(ThermalAction(
                    action_id=f"thermal-emergency-{snap.region_id}-{int(ts)}",
                    region_id=snap.region_id,
                    zone=snap.zone,
                    current_temp_c=snap.avg_temp_c,
                    target_temp_c=self._target,
                    power_watts=snap.power_consumption_w,
                    action_type="emergency_throttle",
                    workloads_to_migrate=max(1, snap.gpu_count // 2),
                    created_at=ts,
                ))
            elif snap.zone == ThermalZone.hot:
                actions.append(ThermalAction(
                    action_id=f"thermal-rebalance-{snap.region_id}-{int(ts)}",
                    region_id=snap.region_id,
                    zone=snap.zone,
                    current_temp_c=snap.avg_temp_c,
                    target_temp_c=self._target,
                    power_watts=snap.power_consumption_w,
                    action_type="workload_redistribution",
                    workloads_to_migrate=max(1, snap.gpu_count // 4),
                    created_at=ts,
                ))
            elif snap.zone == ThermalZone.warm:
                actions.append(ThermalAction(
                    action_id=f"thermal-monitor-{snap.region_id}-{int(ts)}",
                    region_id=snap.region_id,
                    zone=snap.zone,
                    current_temp_c=snap.avg_temp_c,
                    target_temp_c=self._target,
                    power_watts=snap.power_consumption_w,
                    action_type="increase_monitoring",
                    created_at=ts,
                ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute(self, action: ThermalAction) -> ThermalAction:
        action.executed = True
        with self._lock:
            self._total_rebalances += 1
            self._events.append({
                "type": "thermal_action_executed",
                "action_id": action.action_id,
                "action_type": action.action_type,
                "region": action.region_id,
                "zone": action.zone.value,
                "temp_c": action.current_temp_c,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        logger.info("thermal %s on %s (%.0fC)", action.action_type, action.region_id, action.current_temp_c)
        return action

    def balance(self, telemetry: Dict[str, Any]) -> List[ThermalAction]:
        actions = self.analyze(telemetry)
        for a in actions:
            if a.zone in (ThermalZone.hot, ThermalZone.critical):
                self.execute(a)
        return actions

    def thermal_map(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {rid: s.model_dump(mode="json") for rid, s in self._snapshots.items()}

    def hotspots(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [s.model_dump(mode="json") for s in self._snapshots.values()
                    if s.zone in (ThermalZone.hot, ThermalZone.critical)]

    def recent_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in reversed(self._actions)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            snaps = list(self._snapshots.values())
            avg_temp = sum(s.avg_temp_c for s in snaps) / len(snaps) if snaps else 0.0
            hotspot_count = sum(1 for s in snaps if s.zone in (ThermalZone.hot, ThermalZone.critical))
            total_power = sum(s.power_consumption_w for s in snaps)
            return {
                "ts": now_ts(),
                "total_rebalances": self._total_rebalances,
                "avg_temp_c": round(avg_temp, 1),
                "hotspot_regions": hotspot_count,
                "total_power_w": round(total_power, 1),
                "monitored_regions": len(snaps),
            }
