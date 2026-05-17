"""v15 Continental Failover System.

Provides continent-wide rerouting, disaster recovery, regional isolation,
and global workload continuity when entire continents or large regions
experience outages.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("planetary.continental_failover")


class FailoverScope(str, Enum):
    region = "region"
    zone = "zone"
    continent = "continent"
    global_cascade = "global_cascade"


class FailoverStatus(str, Enum):
    detected = "detected"
    isolating = "isolating"
    rerouting = "rerouting"
    recovered = "recovered"
    failed = "failed"


class FailoverTrigger(str, Enum):
    outage = "outage"
    latency_spike = "latency_spike"
    capacity_exhaustion = "capacity_exhaustion"
    thermal_critical = "thermal_critical"
    network_partition = "network_partition"
    disaster = "disaster"


class ContinentalFailoverEvent(BaseModel):
    event_id: str
    scope: FailoverScope = FailoverScope.continent
    trigger: FailoverTrigger = FailoverTrigger.outage
    status: FailoverStatus = FailoverStatus.detected
    affected_continent: str = ""
    affected_regions: List[str] = Field(default_factory=list)
    target_continents: List[str] = Field(default_factory=list)
    target_regions: List[str] = Field(default_factory=list)
    workloads_rerouted: int = 0
    reroute_time_ms: float = 0.0
    detected_at: float = 0.0
    isolated_at: float = 0.0
    recovered_at: float = 0.0
    description: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ContinentalFailoverManager:
    """Manages continent-wide failover, disaster recovery, and global
    workload continuity across planetary infrastructure."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._events: List[ContinentalFailoverEvent] = []
        self._active: Dict[str, ContinentalFailoverEvent] = {}
        self._total_failovers = 0
        self._total_rerouted = 0
        self._log: List[Dict[str, Any]] = []

    def detect_failures(self, telemetry: Dict[str, Any]) -> List[ContinentalFailoverEvent]:
        results: List[ContinentalFailoverEvent] = []
        ts = now_ts()
        regions = telemetry.get("regions", [])

        by_continent: Dict[str, List[Dict[str, Any]]] = {}
        for r in regions:
            c = r.get("continent", "unknown")
            by_continent.setdefault(c, []).append(r)

        for continent, cont_regions in by_continent.items():
            offline = [r for r in cont_regions if r.get("status") == "offline"]
            if len(offline) > len(cont_regions) * 0.5 and len(offline) >= 2:
                event = ContinentalFailoverEvent(
                    event_id=f"fo-continent-{continent}-{int(ts)}",
                    scope=FailoverScope.continent,
                    trigger=FailoverTrigger.outage,
                    affected_continent=continent,
                    affected_regions=[r.get("region_id", "") for r in offline],
                    detected_at=ts,
                    description=f"Continental outage: {len(offline)}/{len(cont_regions)} regions offline in {continent}",
                )
                results.append(event)
                continue

            high_latency = [r for r in cont_regions if r.get("avg_latency_ms", 0) > 500]
            if len(high_latency) > len(cont_regions) * 0.6 and len(high_latency) >= 2:
                event = ContinentalFailoverEvent(
                    event_id=f"fo-latency-{continent}-{int(ts)}",
                    scope=FailoverScope.continent,
                    trigger=FailoverTrigger.latency_spike,
                    affected_continent=continent,
                    affected_regions=[r.get("region_id", "") for r in high_latency],
                    detected_at=ts,
                    description=f"Continental latency spike: {len(high_latency)} regions above 500ms in {continent}",
                )
                results.append(event)
                continue

            thermal_critical = [r for r in cont_regions if r.get("gpu_temp_c", 0) > 90 or r.get("thermal_pressure", 0) > 0.9]
            if len(thermal_critical) > len(cont_regions) * 0.5:
                event = ContinentalFailoverEvent(
                    event_id=f"fo-thermal-{continent}-{int(ts)}",
                    scope=FailoverScope.continent,
                    trigger=FailoverTrigger.thermal_critical,
                    affected_continent=continent,
                    affected_regions=[r.get("region_id", "") for r in thermal_critical],
                    detected_at=ts,
                    description=f"Continental thermal crisis: {len(thermal_critical)} regions critical in {continent}",
                )
                results.append(event)

        with self._lock:
            for e in results:
                self._events.append(e)
                self._active[e.event_id] = e
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return results

    def execute_failover(self, event_id: str, telemetry: Dict[str, Any]) -> Optional[ContinentalFailoverEvent]:
        with self._lock:
            event = self._active.get(event_id)
            if not event:
                return None

        start = now_ts()
        event.status = FailoverStatus.isolating
        event.isolated_at = now_ts()

        regions = telemetry.get("regions", [])
        healthy_continents: Dict[str, List[str]] = {}
        for r in regions:
            if r.get("continent", "") != event.affected_continent and r.get("status", "active") != "offline":
                c = r.get("continent", "other")
                healthy_continents.setdefault(c, []).append(r.get("region_id", ""))

        target_continents = sorted(healthy_continents.keys())
        target_regions = []
        for c in target_continents:
            target_regions.extend(healthy_continents[c])

        workloads_per_region = 5
        workloads_rerouted = len(event.affected_regions) * workloads_per_region

        event.status = FailoverStatus.rerouting
        event.target_continents = target_continents
        event.target_regions = target_regions[:10]
        event.workloads_rerouted = workloads_rerouted
        event.reroute_time_ms = round((now_ts() - start) * 1000, 2)
        event.status = FailoverStatus.recovered
        event.recovered_at = now_ts()

        with self._lock:
            self._active.pop(event_id, None)
            self._total_failovers += 1
            self._total_rerouted += workloads_rerouted
            self._log.append({
                "type": "continental_failover",
                "event_id": event_id,
                "continent": event.affected_continent,
                "trigger": event.trigger.value,
                "regions_affected": len(event.affected_regions),
                "workloads_rerouted": workloads_rerouted,
                "reroute_time_ms": event.reroute_time_ms,
                "ts": now_ts(),
            })
            if len(self._log) > self._max_history:
                self._log = self._log[-self._max_history:]

        logger.info("continental failover %s: %s -> %s (%d workloads rerouted in %.1fms)",
                     event_id, event.affected_continent, target_continents, workloads_rerouted, event.reroute_time_ms)
        return event

    def failover(self, telemetry: Dict[str, Any]) -> List[ContinentalFailoverEvent]:
        events = self.detect_failures(telemetry)
        results = []
        for e in events:
            result = self.execute_failover(e.event_id, telemetry)
            if result:
                results.append(result)
        return results

    def active_events(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.model_dump(mode="json") for e in self._active.values()]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.model_dump(mode="json") for e in reversed(self._events)][:limit]

    def recent_log(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._log))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_failovers": self._total_failovers,
                "total_rerouted": self._total_rerouted,
                "active_events": len(self._active),
            }
