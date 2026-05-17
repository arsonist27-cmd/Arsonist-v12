from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from regions.region_registry import GPUInventory, RegionRecord, RegionRegistry
from shared.utils import now_ts, setup_logging

logger = setup_logging("regions.capacity")


class RegionalCapacityTracker:
    """Tracks and aggregates capacity across all regions."""

    def __init__(self, registry: RegionRegistry) -> None:
        self.registry = registry
        self._lock = threading.Lock()
        self._snapshots: List[Dict[str, Any]] = []

    def update_capacity(
        self,
        region_id: str,
        gpu_inventory: Optional[GPUInventory] = None,
        workload_saturation: Optional[float] = None,
        capacity: Optional[float] = None,
    ) -> Optional[RegionRecord]:
        updates: Dict[str, Any] = {}
        if gpu_inventory is not None:
            updates["gpu_inventory"] = gpu_inventory
        if workload_saturation is not None:
            updates["workload_saturation"] = workload_saturation
        if capacity is not None:
            updates["capacity"] = capacity
        return self.registry.heartbeat(region_id, updates)

    def global_capacity_summary(self) -> Dict[str, Any]:
        regions = self.registry.list_regions()
        total_gpus = 0
        available_gpus = 0
        total_vram = 0.0
        available_vram = 0.0
        avg_saturation = 0.0
        active_count = 0
        for r in regions:
            total_gpus += r.gpu_inventory.total_gpus
            available_gpus += r.gpu_inventory.available_gpus
            total_vram += r.gpu_inventory.total_vram_gb
            available_vram += r.gpu_inventory.available_vram_gb
            if r.status.value in ("active", "degraded"):
                avg_saturation += r.workload_saturation
                active_count += 1
        summary = {
            "ts": now_ts(),
            "total_regions": len(regions),
            "active_regions": active_count,
            "total_gpus": total_gpus,
            "available_gpus": available_gpus,
            "total_vram_gb": round(total_vram, 2),
            "available_vram_gb": round(available_vram, 2),
            "avg_saturation": round(avg_saturation / active_count, 4) if active_count else 0.0,
        }
        with self._lock:
            self._snapshots.append(summary)
            if len(self._snapshots) > 200:
                self._snapshots = self._snapshots[-200:]
        return summary

    def region_capacity(self, region_id: str) -> Optional[Dict[str, Any]]:
        region = self.registry.get(region_id)
        if not region:
            return None
        return {
            "region_id": region.region_id,
            "capacity": region.capacity,
            "workload_saturation": region.workload_saturation,
            "gpu_inventory": region.gpu_inventory.model_dump(),
            "status": region.status.value,
        }

    def find_available_regions(
        self,
        min_gpus: int = 0,
        max_saturation: float = 0.85,
    ) -> List[RegionRecord]:
        results = []
        for r in self.registry.active_regions():
            if r.gpu_inventory.available_gpus >= min_gpus and r.workload_saturation <= max_saturation:
                results.append(r)
        results.sort(key=lambda r: r.workload_saturation)
        return results

    def capacity_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._snapshots))[:limit]
