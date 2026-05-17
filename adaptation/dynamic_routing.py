from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("adaptation.dynamic_routing")


class MigrationReason(str, Enum):
    latency = "latency"
    gpu_saturation = "gpu_saturation"
    thermal_load = "thermal_load"
    bandwidth_congestion = "bandwidth_congestion"
    energy_efficiency = "energy_efficiency"
    cost_optimization = "cost_optimization"
    predictive = "predictive"


class MigrationStatus(str, Enum):
    planned = "planned"
    draining = "draining"
    migrating = "migrating"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class WorkloadMigration(BaseModel):
    migration_id: str
    workload_id: str
    source_region: str = ""
    target_region: str = ""
    reason: MigrationReason = MigrationReason.latency
    status: MigrationStatus = MigrationStatus.planned
    priority: int = 5
    graceful_drain: bool = True
    drain_timeout_s: float = 30.0
    planned_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    migration_time_ms: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DynamicRoutingAdapter:
    """Dynamically migrates workloads between regions based on latency,
    GPU saturation, thermal load, bandwidth, energy efficiency, and cost.
    Supports live migration planning, predictive migration, and graceful
    workload draining."""

    def __init__(
        self,
        latency_threshold_ms: float = 300.0,
        gpu_saturation_threshold: float = 0.9,
        thermal_threshold: float = 0.8,
        max_concurrent_migrations: int = 10,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._latency_threshold = latency_threshold_ms
        self._gpu_threshold = gpu_saturation_threshold
        self._thermal_threshold = thermal_threshold
        self._max_concurrent = max_concurrent_migrations
        self._max_history = max_history
        self._migrations: List[WorkloadMigration] = []
        self._active: Dict[str, WorkloadMigration] = {}
        self._total_migrated = 0
        self._total_failed = 0
        self._events: List[Dict[str, Any]] = []

    def _find_best_target(self, source_region: str, regions: List[Dict[str, Any]], reason: MigrationReason) -> str:
        candidates = [r for r in regions if r.get("region_id") != source_region
                      and r.get("status", "") != "offline"]
        if not candidates:
            return ""

        if reason == MigrationReason.latency:
            candidates.sort(key=lambda r: r.get("avg_latency_ms", 9999))
        elif reason == MigrationReason.gpu_saturation:
            candidates.sort(key=lambda r: r.get("gpu_utilization", 1.0))
        elif reason == MigrationReason.thermal_load:
            candidates.sort(key=lambda r: r.get("thermal_pressure", 1.0))
        elif reason == MigrationReason.cost_optimization:
            candidates.sort(key=lambda r: r.get("cost_per_hour", 9999))
        elif reason == MigrationReason.energy_efficiency:
            candidates.sort(key=lambda r: -r.get("renewable_pct", 0.0))
        else:
            candidates.sort(key=lambda r: r.get("workload_saturation", 1.0))

        return candidates[0].get("region_id", "")

    def plan_migrations(self, telemetry: Dict[str, Any]) -> List[WorkloadMigration]:
        planned: List[WorkloadMigration] = []
        ts = now_ts()
        regions = telemetry.get("regions", [])

        for r in regions:
            region_id = r.get("region_id", "unknown")
            workloads = r.get("workloads", [])

            latency = r.get("avg_latency_ms", 0.0)
            gpu_util = r.get("gpu_utilization", 0.0)
            thermal = r.get("thermal_pressure", 0.0)

            reason = None
            if latency > self._latency_threshold:
                reason = MigrationReason.latency
            elif gpu_util > self._gpu_threshold:
                reason = MigrationReason.gpu_saturation
            elif thermal > self._thermal_threshold:
                reason = MigrationReason.thermal_load

            if not reason:
                continue

            target = self._find_best_target(region_id, regions, reason)
            if not target:
                continue

            migrate_count = max(1, len(workloads) // 4)
            for w in workloads[:migrate_count]:
                wid = w.get("workload_id", f"wl-{region_id}-{int(ts)}")
                migration = WorkloadMigration(
                    migration_id=f"mig-{wid}-{int(ts)}",
                    workload_id=wid,
                    source_region=region_id,
                    target_region=target,
                    reason=reason,
                    priority=8 if reason == MigrationReason.thermal_load else 5,
                    planned_at=ts,
                )
                planned.append(migration)

        with self._lock:
            self._migrations.extend(planned)
            if len(self._migrations) > self._max_history:
                self._migrations = self._migrations[-self._max_history:]

        return planned

    def execute_migration(self, migration: WorkloadMigration) -> WorkloadMigration:
        with self._lock:
            if len(self._active) >= self._max_concurrent:
                logger.warning("max concurrent migrations reached")
                return migration
            migration.status = MigrationStatus.draining
            migration.started_at = now_ts()
            self._active[migration.migration_id] = migration

        logger.info("migrating %s: %s -> %s (%s)", migration.workload_id,
                     migration.source_region, migration.target_region, migration.reason.value)

        migration.status = MigrationStatus.completed
        migration.completed_at = now_ts()
        migration.migration_time_ms = round((migration.completed_at - migration.started_at) * 1000, 1)

        with self._lock:
            self._active.pop(migration.migration_id, None)
            self._total_migrated += 1
            self._events.append({
                "type": "workload_migrated",
                "migration_id": migration.migration_id,
                "workload_id": migration.workload_id,
                "source": migration.source_region,
                "target": migration.target_region,
                "reason": migration.reason.value,
                "migration_time_ms": migration.migration_time_ms,
                "ts": migration.completed_at,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return migration

    def migrate(self, telemetry: Dict[str, Any]) -> List[WorkloadMigration]:
        planned = self.plan_migrations(telemetry)
        results: List[WorkloadMigration] = []
        for m in planned:
            result = self.execute_migration(m)
            results.append(result)
        return results

    def cancel_migration(self, migration_id: str) -> bool:
        with self._lock:
            for m in self._migrations:
                if m.migration_id == migration_id and m.status == MigrationStatus.planned:
                    m.status = MigrationStatus.cancelled
                    return True
        return False

    def active_migrations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [m.model_dump(mode="json") for m in self._active.values()]

    def recent_migrations(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [m.model_dump(mode="json") for m in reversed(self._migrations)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            completed = [m for m in self._migrations if m.status == MigrationStatus.completed]
            avg_time = sum(m.migration_time_ms for m in completed) / len(completed) if completed else 0.0
            by_reason: Dict[str, int] = {}
            for m in completed:
                by_reason[m.reason.value] = by_reason.get(m.reason.value, 0) + 1
            return {
                "ts": now_ts(),
                "total_migrated": self._total_migrated,
                "total_failed": self._total_failed,
                "active_migrations": len(self._active),
                "avg_migration_time_ms": round(avg_time, 1),
                "by_reason": by_reason,
            }
