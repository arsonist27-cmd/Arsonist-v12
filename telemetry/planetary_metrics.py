"""v15 Planetary Observability.

Tracks worldwide traffic, continental load, failover events, energy usage,
carbon metrics, and infrastructure efficiency across the entire planetary
AI operating fabric.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.planetary_metrics")


class ContinentalMetrics(BaseModel):
    continent: str
    total_regions: int = 0
    active_regions: int = 0
    total_gpus: int = 0
    active_workloads: int = 0
    avg_latency_ms: float = 0.0
    avg_utilization: float = 0.0
    avg_carbon_intensity: float = 0.0
    renewable_pct: float = 0.0
    failover_events: int = 0
    ts: float = 0.0


class PlanetarySnapshot(BaseModel):
    total_continents: int = 0
    total_regions: int = 0
    total_gpus: int = 0
    total_workloads: int = 0
    global_avg_latency_ms: float = 0.0
    global_avg_utilization: float = 0.0
    global_carbon_intensity: float = 0.0
    global_renewable_pct: float = 0.0
    total_failovers: int = 0
    total_energy_kwh: float = 0.0
    total_co2_kg: float = 0.0
    continents: Dict[str, ContinentalMetrics] = Field(default_factory=dict)
    ts: float = 0.0


class PlanetaryMetricsCollector:
    """Collects and aggregates metrics across the entire planetary
    infrastructure for global observability and reporting."""

    def __init__(self, max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._snapshots: List[PlanetarySnapshot] = []
        self._failover_log: List[Dict[str, Any]] = []
        self._energy_log: List[Dict[str, Any]] = []
        self._events: List[Dict[str, Any]] = []
        self._total_snapshots = 0

    def collect(self, telemetry: Dict[str, Any]) -> PlanetarySnapshot:
        regions = telemetry.get("regions", [])

        by_continent: Dict[str, List[Dict[str, Any]]] = {}
        for r in regions:
            c = r.get("continent", "unknown")
            by_continent.setdefault(c, []).append(r)

        continent_metrics: Dict[str, ContinentalMetrics] = {}
        total_gpus = 0
        total_workloads = 0
        total_latency = 0.0
        total_util = 0.0
        total_carbon = 0.0
        total_renewable = 0.0
        total_energy = 0.0

        for continent, cont_regions in by_continent.items():
            c_gpus = sum(r.get("total_gpus", 0) for r in cont_regions)
            c_workloads = sum(r.get("active_workloads", 0) for r in cont_regions)
            c_latency = sum(r.get("avg_latency_ms", 0) for r in cont_regions)
            c_util = sum(r.get("workload_saturation", 0) for r in cont_regions)
            c_carbon = sum(r.get("carbon_intensity", 0.5) for r in cont_regions)
            c_renewable = sum(r.get("renewable_pct", 0) for r in cont_regions)
            c_energy = sum(r.get("power_consumption_w", 0) for r in cont_regions) / 1000.0
            n = len(cont_regions)
            active = sum(1 for r in cont_regions if r.get("status", "active") != "offline")

            cm = ContinentalMetrics(
                continent=continent,
                total_regions=n,
                active_regions=active,
                total_gpus=c_gpus,
                active_workloads=c_workloads,
                avg_latency_ms=round(c_latency / n, 1) if n else 0,
                avg_utilization=round(c_util / n, 3) if n else 0,
                avg_carbon_intensity=round(c_carbon / n, 3) if n else 0,
                renewable_pct=round(c_renewable / n, 3) if n else 0,
                ts=now_ts(),
            )
            continent_metrics[continent] = cm

            total_gpus += c_gpus
            total_workloads += c_workloads
            total_latency += c_latency
            total_util += c_util
            total_carbon += c_carbon
            total_renewable += c_renewable
            total_energy += c_energy

        n_regions = len(regions) or 1
        snapshot = PlanetarySnapshot(
            total_continents=len(by_continent),
            total_regions=len(regions),
            total_gpus=total_gpus,
            total_workloads=total_workloads,
            global_avg_latency_ms=round(total_latency / n_regions, 1),
            global_avg_utilization=round(total_util / n_regions, 3),
            global_carbon_intensity=round(total_carbon / n_regions, 3),
            global_renewable_pct=round(total_renewable / n_regions, 3),
            total_energy_kwh=round(total_energy, 2),
            continents=continent_metrics,
            ts=now_ts(),
        )

        with self._lock:
            self._snapshots.append(snapshot)
            if len(self._snapshots) > self._max_history:
                self._snapshots = self._snapshots[-self._max_history:]
            self._total_snapshots += 1

        return snapshot

    def record_failover(self, event: Dict[str, Any]) -> None:
        with self._lock:
            event["ts"] = now_ts()
            self._failover_log.append(event)
            if len(self._failover_log) > self._max_history:
                self._failover_log = self._failover_log[-self._max_history:]

    def record_energy(self, event: Dict[str, Any]) -> None:
        with self._lock:
            event["ts"] = now_ts()
            self._energy_log.append(event)
            if len(self._energy_log) > self._max_history:
                self._energy_log = self._energy_log[-self._max_history:]

    def latest_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            if self._snapshots:
                return self._snapshots[-1].model_dump(mode="json")
            return {}

    def snapshot_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [s.model_dump(mode="json") for s in reversed(self._snapshots)][:limit]

    def failover_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._failover_log))[:limit]

    def energy_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._energy_log))[:limit]

    def continental_breakdown(self) -> Dict[str, Any]:
        with self._lock:
            if not self._snapshots:
                return {}
            latest = self._snapshots[-1]
            return {k: v.model_dump(mode="json") for k, v in latest.continents.items()}

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_snapshots": self._total_snapshots,
                "failover_events_logged": len(self._failover_log),
                "energy_events_logged": len(self._energy_log),
            }
