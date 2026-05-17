from __future__ import annotations

import math
from typing import List, Optional, Tuple

from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from shared.utils import setup_logging

logger = setup_logging("regions.geo_routing")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class GeoRouter:
    """Routes requests to the nearest healthy region by geographic distance."""

    def __init__(self, registry: RegionRegistry) -> None:
        self.registry = registry

    def nearest_region(
        self,
        client_lat: float,
        client_lon: float,
        exclude: Optional[List[str]] = None,
        require_gpu: bool = False,
    ) -> Optional[RegionRecord]:
        candidates = self._ranked(client_lat, client_lon, exclude, require_gpu)
        return candidates[0][1] if candidates else None

    def ranked_regions(
        self,
        client_lat: float,
        client_lon: float,
        exclude: Optional[List[str]] = None,
        require_gpu: bool = False,
        limit: int = 5,
    ) -> List[Tuple[float, RegionRecord]]:
        return self._ranked(client_lat, client_lon, exclude, require_gpu)[:limit]

    def _ranked(
        self,
        client_lat: float,
        client_lon: float,
        exclude: Optional[List[str]],
        require_gpu: bool,
    ) -> List[Tuple[float, RegionRecord]]:
        exclude_set = set(exclude or [])
        candidates: List[Tuple[float, RegionRecord]] = []
        for region in self.registry.active_regions():
            if region.region_id in exclude_set:
                continue
            if region.status == RegionStatus.draining:
                continue
            if require_gpu and region.gpu_inventory.available_gpus <= 0:
                continue
            dist = haversine_km(client_lat, client_lon, region.latitude, region.longitude)
            candidates.append((dist, region))
        candidates.sort(key=lambda x: x[0])
        return candidates
