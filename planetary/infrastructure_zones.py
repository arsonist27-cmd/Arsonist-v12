"""v15 Infrastructure Zones.

Manages infrastructure zones — logical groupings of regions, edge nodes,
and compute pools within continents. Supports zone isolation, capacity
management, and hierarchical failover.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("planetary.infrastructure_zones")


class ZoneType(str, Enum):
    primary = "primary"
    secondary = "secondary"
    edge = "edge"
    disaster_recovery = "disaster_recovery"


class ZoneStatus(str, Enum):
    active = "active"
    degraded = "degraded"
    draining = "draining"
    isolated = "isolated"
    offline = "offline"


class InfrastructureZone(BaseModel):
    zone_id: str
    zone_type: ZoneType = ZoneType.primary
    continent: str = ""
    region_ids: List[str] = Field(default_factory=list)
    status: ZoneStatus = ZoneStatus.active
    capacity: float = 1.0
    utilization: float = 0.0
    gpu_count: int = 0
    active_workloads: int = 0
    failover_target: str = ""
    max_workloads: int = 10000
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: float = 0.0
    updated_at: float = 0.0


class ZoneManager:
    """Manages infrastructure zones for hierarchical organization,
    capacity management, and zone-level failover."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._zones: Dict[str, InfrastructureZone] = {}
        self._events: List[Dict[str, Any]] = []
        self._max_history = max_history

    def register_zone(self, zone: InfrastructureZone) -> None:
        with self._lock:
            zone.created_at = now_ts()
            zone.updated_at = now_ts()
            self._zones[zone.zone_id] = zone
            self._events.append({
                "type": "zone_registered",
                "zone_id": zone.zone_id,
                "zone_type": zone.zone_type.value,
                "continent": zone.continent,
                "ts": now_ts(),
            })

    def remove_zone(self, zone_id: str) -> bool:
        with self._lock:
            if zone_id not in self._zones:
                return False
            del self._zones[zone_id]
            return True

    def update_zone_status(self, zone_id: str, status: ZoneStatus) -> bool:
        with self._lock:
            zone = self._zones.get(zone_id)
            if not zone:
                return False
            old_status = zone.status
            zone.status = status
            zone.updated_at = now_ts()
            self._events.append({
                "type": "zone_status_change",
                "zone_id": zone_id,
                "old_status": old_status.value,
                "new_status": status.value,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
            return True

    def update_from_telemetry(self, telemetry: Dict[str, Any]) -> None:
        regions = telemetry.get("regions", [])
        region_map: Dict[str, Dict[str, Any]] = {r.get("region_id", ""): r for r in regions}

        with self._lock:
            for zone in self._zones.values():
                total_gpus = 0
                total_workloads = 0
                total_util = 0.0
                active_count = 0
                offline_count = 0

                for rid in zone.region_ids:
                    r = region_map.get(rid)
                    if not r:
                        continue
                    total_gpus += r.get("total_gpus", 0)
                    total_workloads += r.get("active_workloads", 0)
                    total_util += r.get("workload_saturation", 0.0)
                    if r.get("status") == "offline":
                        offline_count += 1
                    else:
                        active_count += 1

                n = active_count + offline_count
                zone.gpu_count = total_gpus
                zone.active_workloads = total_workloads
                zone.utilization = round(total_util / n, 3) if n > 0 else 0.0

                if offline_count > 0 and active_count == 0:
                    zone.status = ZoneStatus.offline
                elif offline_count > active_count:
                    zone.status = ZoneStatus.degraded
                zone.updated_at = now_ts()

    def get_zone(self, zone_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            zone = self._zones.get(zone_id)
            return zone.model_dump(mode="json") if zone else None

    def zones_by_continent(self, continent: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [z.model_dump(mode="json") for z in self._zones.values() if z.continent == continent]

    def active_zones(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [z.model_dump(mode="json") for z in self._zones.values() if z.status == ZoneStatus.active]

    def failover_targets(self, zone_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            zone = self._zones.get(zone_id)
            if not zone:
                return []
            if zone.failover_target and zone.failover_target in self._zones:
                target = self._zones[zone.failover_target]
                if target.status in (ZoneStatus.active, ZoneStatus.degraded):
                    return [target.model_dump(mode="json")]
            same_continent = [z for z in self._zones.values()
                              if z.zone_id != zone_id and z.continent == zone.continent
                              and z.status == ZoneStatus.active]
            return [z.model_dump(mode="json") for z in same_continent]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            active = sum(1 for z in self._zones.values() if z.status == ZoneStatus.active)
            total_gpus = sum(z.gpu_count for z in self._zones.values())
            total_workloads = sum(z.active_workloads for z in self._zones.values())
            return {
                "ts": now_ts(),
                "total_zones": len(self._zones),
                "active_zones": active,
                "total_gpus": total_gpus,
                "total_workloads": total_workloads,
            }
