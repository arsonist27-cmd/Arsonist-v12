from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from regions.latency_map import LatencyMap
from regions.region_registry import RegionRecord, RegionRegistry
from shared.utils import now_ts, setup_logging

logger = setup_logging("routing.latency_router")


class LatencyRouter:
    """Pure latency-based routing: picks the lowest-latency region for a client."""

    def __init__(self, registry: RegionRegistry, latency_map: LatencyMap) -> None:
        self.registry = registry
        self.latency_map = latency_map
        self._route_count = 0

    def route_by_client(
        self,
        client_id: str,
        require_gpu: bool = False,
        exclude_regions: Optional[List[str]] = None,
    ) -> Optional[str]:
        candidates = self.registry.active_regions()
        if require_gpu:
            candidates = [r for r in candidates if r.gpu_inventory.available_gpus > 0]
        exclude = set(exclude_regions or [])
        candidates = [r for r in candidates if r.region_id not in exclude]
        if not candidates:
            return None

        region_ids = [r.region_id for r in candidates]
        best = self.latency_map.best_region_for_client(client_id, region_ids)
        if best:
            self._route_count += 1
            return best

        candidates.sort(key=lambda r: r.avg_latency_ms)
        self._route_count += 1
        return candidates[0].region_id

    def route_by_region(
        self,
        source_region: str,
        require_gpu: bool = False,
        exclude_regions: Optional[List[str]] = None,
    ) -> Optional[str]:
        candidates = self.registry.active_regions()
        if require_gpu:
            candidates = [r for r in candidates if r.gpu_inventory.available_gpus > 0]
        exclude = set(exclude_regions or [])
        exclude.add(source_region)
        candidates = [r for r in candidates if r.region_id not in exclude]
        if not candidates:
            return None

        measured: List[Tuple[float, str]] = []
        for c in candidates:
            lat = self.latency_map.get_inter_region(source_region, c.region_id)
            if lat is not None:
                measured.append((lat, c.region_id))

        if measured:
            measured.sort()
            self._route_count += 1
            return measured[0][1]

        candidates.sort(key=lambda r: r.avg_latency_ms)
        self._route_count += 1
        return candidates[0].region_id

    def ranked_by_latency(
        self,
        source_region: str,
        limit: int = 5,
    ) -> List[Tuple[float, str]]:
        candidates = self.registry.active_regions()
        result: List[Tuple[float, str]] = []
        for c in candidates:
            if c.region_id == source_region:
                continue
            lat = self.latency_map.get_inter_region(source_region, c.region_id)
            if lat is not None:
                result.append((lat, c.region_id))
        result.sort()
        return result[:limit]

    def metrics(self) -> Dict[str, Any]:
        return {
            "ts": now_ts(),
            "total_latency_routes": self._route_count,
        }
