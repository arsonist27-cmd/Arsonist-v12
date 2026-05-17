from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from fabric.placement_engine import PlacementEngine, PlacementRequest
from fabric.topology_manager import TopologyManager
from regions.region_registry import RegionRegistry
from regions.regional_capacity import RegionalCapacityTracker
from shared.utils import now_ts, setup_logging

logger = setup_logging("fabric.compute")


class ComputeFabric:
    """Top-level abstraction over the global compute mesh.

    Coordinates placement, topology, and workload lifecycle.
    """

    def __init__(
        self,
        registry: RegionRegistry,
        capacity_tracker: RegionalCapacityTracker,
    ) -> None:
        self.registry = registry
        self.capacity = capacity_tracker
        self.topology = TopologyManager(registry)
        self.placement = PlacementEngine(registry, capacity_tracker)
        self._lock = threading.Lock()
        self._active_workloads: Dict[str, Dict[str, Any]] = {}
        self._completed_workloads = 0
        self._failed_workloads = 0

    def submit_workload(self, request: PlacementRequest) -> Optional[Dict[str, Any]]:
        decision = self.placement.place(request)
        if not decision:
            logger.warning("No placement found for workload %s", request.workload_id)
            return None
        workload = {
            "workload_id": request.workload_id,
            "region_id": decision.region_id,
            "score": decision.score,
            "factors": decision.factors,
            "submitted_at": now_ts(),
            "status": "running",
        }
        with self._lock:
            self._active_workloads[request.workload_id] = workload
        logger.info("Workload %s placed in region %s", request.workload_id, decision.region_id)
        return workload

    def complete_workload(self, workload_id: str, success: bool = True) -> None:
        with self._lock:
            wl = self._active_workloads.pop(workload_id, None)
        if not wl:
            return
        if success:
            self._completed_workloads += 1
        else:
            self._failed_workloads += 1

    def migrate_workload(self, workload_id: str, target_region: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            wl = self._active_workloads.get(workload_id)
            if not wl:
                return None
            old_region = wl["region_id"]
            wl["region_id"] = target_region
            wl["migrated_from"] = old_region
            wl["migrated_at"] = now_ts()
        logger.info("Workload %s migrated %s -> %s", workload_id, old_region, target_region)
        return wl

    def active_workloads(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._active_workloads.values())

    def workloads_in_region(self, region_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [w for w in self._active_workloads.values() if w["region_id"] == region_id]

    def drain_region(self, region_id: str, target_region: str) -> int:
        migrated = 0
        with self._lock:
            to_migrate = [
                wid for wid, w in self._active_workloads.items()
                if w["region_id"] == region_id
            ]
        for wid in to_migrate:
            result = self.migrate_workload(wid, target_region)
            if result:
                migrated += 1
        return migrated

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            by_region: Dict[str, int] = {}
            for w in self._active_workloads.values():
                rid = w["region_id"]
                by_region[rid] = by_region.get(rid, 0) + 1
            return {
                "ts": now_ts(),
                "active_workloads": len(self._active_workloads),
                "completed_workloads": self._completed_workloads,
                "failed_workloads": self._failed_workloads,
                "by_region": by_region,
            }
