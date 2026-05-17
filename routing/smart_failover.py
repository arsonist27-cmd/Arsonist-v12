from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from regions.regional_capacity import RegionalCapacityTracker
from shared.utils import now_ts, setup_logging

logger = setup_logging("routing.failover")


class FailoverTrigger(str, Enum):
    latency_spike = "latency_spike"
    region_outage = "region_outage"
    gpu_exhaustion = "gpu_exhaustion"
    network_partition = "network_partition"
    deployment_failure = "deployment_failure"
    manual = "manual"


class FailoverEvent:
    def __init__(
        self,
        source_region: str,
        target_region: str,
        trigger: FailoverTrigger,
        workloads_migrated: int = 0,
    ) -> None:
        self.source_region = source_region
        self.target_region = target_region
        self.trigger = trigger
        self.workloads_migrated = workloads_migrated
        self.ts = now_ts()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_region": self.source_region,
            "target_region": self.target_region,
            "trigger": self.trigger.value,
            "workloads_migrated": self.workloads_migrated,
            "ts": self.ts,
        }


class SmartFailover:
    """Automatic regional failover with transparent rerouting."""

    LATENCY_SPIKE_THRESHOLD_MS = 1000.0
    GPU_EXHAUSTION_THRESHOLD = 0.95
    CHECK_INTERVAL_SEC = 10.0

    def __init__(
        self,
        registry: RegionRegistry,
        capacity_tracker: RegionalCapacityTracker,
        on_failover: Optional[Callable[[FailoverEvent], None]] = None,
    ) -> None:
        self.registry = registry
        self.capacity = capacity_tracker
        self._on_failover = on_failover
        self._lock = threading.Lock()
        self._events: List[FailoverEvent] = []
        self._draining_regions: Dict[str, float] = {}
        self._total_failovers = 0
        self._total_workloads_migrated = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="smart-failover")
        self._thread.start()
        logger.info("Smart failover monitor started")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.CHECK_INTERVAL_SEC + 2)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.check_all()
            except Exception:
                logger.exception("Failover check error")
            self._stop.wait(self.CHECK_INTERVAL_SEC)

    def check_all(self) -> List[FailoverEvent]:
        events: List[FailoverEvent] = []
        for region in self.registry.list_regions():
            trigger = self._evaluate_region(region)
            if trigger:
                target = self._select_failover_target(region)
                if target:
                    event = self._execute_failover(region, target, trigger)
                    events.append(event)
        return events

    def _evaluate_region(self, region: RegionRecord) -> Optional[FailoverTrigger]:
        if region.status == RegionStatus.offline:
            return FailoverTrigger.region_outage
        if region.avg_latency_ms > self.LATENCY_SPIKE_THRESHOLD_MS:
            return FailoverTrigger.latency_spike
        inv = region.gpu_inventory
        if inv.total_gpus > 0 and inv.available_gpus == 0:
            return FailoverTrigger.gpu_exhaustion
        if region.workload_saturation > self.GPU_EXHAUSTION_THRESHOLD:
            return FailoverTrigger.gpu_exhaustion
        return None

    def _select_failover_target(self, failing_region: RegionRecord) -> Optional[RegionRecord]:
        if failing_region.failover_target:
            target = self.registry.get(failing_region.failover_target)
            if target and target.status == RegionStatus.active:
                return target

        candidates = self.registry.active_regions()
        candidates = [r for r in candidates if r.region_id != failing_region.region_id]
        candidates = [r for r in candidates if r.workload_saturation < 0.85]
        if not candidates:
            return None
        candidates.sort(key=lambda r: (r.failover_priority, r.workload_saturation))
        return candidates[0]

    def _execute_failover(
        self,
        source: RegionRecord,
        target: RegionRecord,
        trigger: FailoverTrigger,
    ) -> FailoverEvent:
        self.registry.update_status(source.region_id, RegionStatus.draining)
        event = FailoverEvent(
            source_region=source.region_id,
            target_region=target.region_id,
            trigger=trigger,
        )
        with self._lock:
            self._events.append(event)
            if len(self._events) > 200:
                self._events = self._events[-200:]
            self._total_failovers += 1

        logger.warning(
            "Failover: %s -> %s (trigger=%s)",
            source.region_id, target.region_id, trigger.value,
        )
        if self._on_failover:
            try:
                self._on_failover(event)
            except Exception:
                logger.exception("Failover callback error")
        return event

    def trigger_manual_failover(self, source_region_id: str, target_region_id: str) -> Optional[FailoverEvent]:
        source = self.registry.get(source_region_id)
        target = self.registry.get(target_region_id)
        if not source or not target:
            return None
        return self._execute_failover(source, target, FailoverTrigger.manual)

    def recover_region(self, region_id: str) -> Optional[RegionRecord]:
        with self._lock:
            self._draining_regions.pop(region_id, None)
        return self.registry.update_status(region_id, RegionStatus.active)

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.to_dict() for e in reversed(self._events)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_failovers": self._total_failovers,
                "total_workloads_migrated": self._total_workloads_migrated,
                "currently_draining": list(self._draining_regions.keys()),
                "recent_events": len(self._events),
            }
