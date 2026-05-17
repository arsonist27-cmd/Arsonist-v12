"""v15 Global Fabric Controller.

Top-level orchestration controller for the planet-scale AI operating fabric.
Coordinates the planetary scheduler, infrastructure graph, decision engine,
and all v15 subsystems into a unified control loop.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("fabric_core.global_controller")


class FabricStatus(BaseModel):
    controller_id: str = "primary"
    mode: str = "planet_scale"
    active_continents: List[str] = Field(default_factory=list)
    total_regions: int = 0
    total_gpus: int = 0
    total_workloads: int = 0
    global_utilization: float = 0.0
    global_latency_avg_ms: float = 0.0
    carbon_efficiency: float = 0.0
    uptime_s: float = 0.0
    ts: float = 0.0


class GlobalFabricController:
    """Top-level controller for the planet-scale AI operating fabric.

    Runs the global control loop, collecting telemetry from all subsystems
    and coordinating planetary-scale decisions.
    """

    def __init__(self, controller_id: str = "primary") -> None:
        self._lock = threading.RLock()
        self._controller_id = controller_id
        self._started_at = now_ts()
        self._loop_count = 0
        self._events: List[Dict[str, Any]] = []
        self._last_status: Dict[str, Any] = {}
        self._subsystem_metrics: Dict[str, Dict[str, Any]] = {}

    def run_control_loop(self, telemetry: Dict[str, Any]) -> Dict[str, Any]:
        start = now_ts()
        regions = telemetry.get("regions", [])

        continents = set()
        total_gpus = 0
        total_workloads = 0
        total_util = 0.0
        total_latency = 0.0

        for r in regions:
            continent = r.get("continent", "")
            if continent:
                continents.add(continent)
            total_gpus += r.get("total_gpus", 0)
            total_workloads += r.get("active_workloads", 0)
            total_util += r.get("workload_saturation", 0.0)
            total_latency += r.get("avg_latency_ms", 0.0)

        n = len(regions) or 1
        avg_util = total_util / n
        avg_latency = total_latency / n

        carbon_vals = [r.get("carbon_intensity", 0.5) for r in regions]
        carbon_eff = 1.0 - (sum(carbon_vals) / len(carbon_vals)) if carbon_vals else 0.5

        status = FabricStatus(
            controller_id=self._controller_id,
            active_continents=sorted(continents),
            total_regions=len(regions),
            total_gpus=total_gpus,
            total_workloads=total_workloads,
            global_utilization=round(avg_util, 3),
            global_latency_avg_ms=round(avg_latency, 1),
            carbon_efficiency=round(carbon_eff, 3),
            uptime_s=round(now_ts() - self._started_at, 1),
            ts=now_ts(),
        )

        result = status.model_dump(mode="json")
        loop_time = round((now_ts() - start) * 1000, 2)
        result["loop_time_ms"] = loop_time

        with self._lock:
            self._loop_count += 1
            self._last_status = result
            self._events.append({
                "type": "control_loop",
                "loop": self._loop_count,
                "regions": len(regions),
                "continents": len(continents),
                "loop_time_ms": loop_time,
                "ts": now_ts(),
            })
            if len(self._events) > 500:
                self._events = self._events[-500:]

        return result

    def register_subsystem_metrics(self, name: str, metrics: Dict[str, Any]) -> None:
        with self._lock:
            self._subsystem_metrics[name] = metrics

    def fabric_status(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._last_status)

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def subsystem_health(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._subsystem_metrics)

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "controller_id": self._controller_id,
                "loop_count": self._loop_count,
                "uptime_s": round(now_ts() - self._started_at, 1),
                "subsystems": len(self._subsystem_metrics),
            }
