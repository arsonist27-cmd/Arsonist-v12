from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from shared.utils import now_ts, setup_logging

logger = setup_logging("regions.health")

DEFAULT_HEARTBEAT_TIMEOUT_SEC = 60
DEFAULT_DEGRADED_LATENCY_MS = 500.0
DEFAULT_DEGRADED_SATURATION = 0.90
DEFAULT_CHECK_INTERVAL_SEC = 15


class RegionHealthMonitor:
    """Monitors region heartbeats and marks unhealthy regions."""

    def __init__(
        self,
        registry: RegionRegistry,
        heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT_SEC,
        degraded_latency_ms: float = DEFAULT_DEGRADED_LATENCY_MS,
        degraded_saturation: float = DEFAULT_DEGRADED_SATURATION,
        check_interval: float = DEFAULT_CHECK_INTERVAL_SEC,
    ) -> None:
        self.registry = registry
        self.heartbeat_timeout = heartbeat_timeout
        self.degraded_latency_ms = degraded_latency_ms
        self.degraded_saturation = degraded_saturation
        self.check_interval = check_interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._health_history: Dict[str, List[Dict[str, Any]]] = {}

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="region-health")
        self._thread.start()
        logger.info("Region health monitor started (interval=%ss)", self.check_interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.check_interval + 2)
        logger.info("Region health monitor stopped")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.check_all()
            except Exception:
                logger.exception("Region health check error")
            self._stop.wait(self.check_interval)

    def check_all(self) -> Dict[str, RegionStatus]:
        results: Dict[str, RegionStatus] = {}
        ts = now_ts()
        for region in self.registry.list_regions():
            new_status = self._evaluate(region, ts)
            results[region.region_id] = new_status
            if new_status != region.status:
                logger.warning(
                    "Region %s status %s -> %s",
                    region.region_id, region.status.value, new_status.value,
                )
                self.registry.update_status(region.region_id, new_status)
            self._record_health(region.region_id, new_status, ts)
        return results

    def _evaluate(self, region: RegionRecord, ts: float) -> RegionStatus:
        if region.status == RegionStatus.draining:
            return RegionStatus.draining
        heartbeat_age = ts - region.last_heartbeat
        if heartbeat_age > self.heartbeat_timeout:
            return RegionStatus.offline
        if (
            region.avg_latency_ms > self.degraded_latency_ms
            or region.workload_saturation > self.degraded_saturation
        ):
            return RegionStatus.degraded
        return RegionStatus.active

    def _record_health(self, region_id: str, status: RegionStatus, ts: float) -> None:
        if region_id not in self._health_history:
            self._health_history[region_id] = []
        history = self._health_history[region_id]
        history.append({"ts": ts, "status": status.value})
        if len(history) > 100:
            self._health_history[region_id] = history[-100:]

    def get_health_history(self, region_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        return list(reversed(self._health_history.get(region_id, [])))[:limit]

    def region_health_summary(self) -> Dict[str, Any]:
        regions = self.registry.list_regions()
        by_status: Dict[str, int] = {}
        for r in regions:
            by_status[r.status.value] = by_status.get(r.status.value, 0) + 1
        return {
            "ts": now_ts(),
            "total_regions": len(regions),
            "by_status": by_status,
        }
