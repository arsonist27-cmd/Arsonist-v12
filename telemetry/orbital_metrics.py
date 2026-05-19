"""v16 Orbital Metrics.

Tracks orbital node health, signal latency, synchronization lag,
disconnected regions, replication backlog, and link quality across
the interplanetary infrastructure fabric.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.orbital_metrics")


class OrbitalSnapshot(BaseModel):
    ts: float = 0.0
    total_nodes: int = 0
    active_nodes: int = 0
    disconnected_nodes: int = 0
    orbital_nodes: int = 0
    ground_nodes: int = 0
    avg_signal_latency_ms: float = 0.0
    max_signal_latency_ms: float = 0.0
    total_links: int = 0
    active_links: int = 0
    degraded_links: int = 0
    offline_links: int = 0
    sync_lag_avg_s: float = 0.0
    replication_backlog: int = 0
    active_partitions: int = 0
    active_failovers: int = 0
    autonomous_regions: int = 0
    messages_in_queue: int = 0
    burst_syncs: int = 0
    by_orbit: Dict[str, int] = Field(default_factory=dict)
    by_status: Dict[str, int] = Field(default_factory=dict)


class OrbitalMetricsCollector:
    """Collects and aggregates metrics across the interplanetary
    infrastructure for observability and decision support."""

    def __init__(self, max_snapshots: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_snapshots = max_snapshots
        self._snapshots: List[OrbitalSnapshot] = []
        self._failover_log: List[Dict[str, Any]] = []
        self._partition_log: List[Dict[str, Any]] = []
        self._sync_log: List[Dict[str, Any]] = []
        self._total_collections = 0

    def collect(self, node_data: List[Dict[str, Any]] = None,
                link_data: List[Dict[str, Any]] = None,
                queue_metrics: Dict[str, Any] = None,
                replication_metrics: Dict[str, Any] = None,
                partition_metrics: Dict[str, Any] = None,
                failover_metrics: Dict[str, Any] = None,
                operations_metrics: Dict[str, Any] = None) -> OrbitalSnapshot:
        if node_data is None:
            node_data = []
        if link_data is None:
            link_data = []

        total_nodes = len(node_data)
        active = sum(1 for n in node_data if n.get("status") in ("active", "degraded"))
        disconnected = sum(1 for n in node_data if n.get("status") == "disconnected")
        orbital = sum(1 for n in node_data if n.get("orbit", "ground") != "ground")
        ground = sum(1 for n in node_data if n.get("orbit", "ground") == "ground")

        latencies = [n.get("signal_latency_ms", 0) for n in node_data if n.get("signal_latency_ms", 0) > 0]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        max_latency = max(latencies) if latencies else 0.0

        by_orbit: Dict[str, int] = {}
        by_status: Dict[str, int] = {}
        for n in node_data:
            orbit = n.get("orbit", "ground")
            status = n.get("status", "unknown")
            by_orbit[orbit] = by_orbit.get(orbit, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1

        total_links = len(link_data)
        active_links = sum(1 for l in link_data if l.get("state") in ("optimal", "active"))
        degraded_links = sum(1 for l in link_data
                             if l.get("state") in ("degraded", "congested", "intermittent"))
        offline_links = sum(1 for l in link_data if l.get("state") in ("offline", "blackout"))

        messages_in_queue = 0
        if queue_metrics:
            messages_in_queue = queue_metrics.get("pending", 0) + queue_metrics.get("in_transit", 0)

        replication_backlog = 0
        sync_lag_avg = 0.0
        if replication_metrics:
            replication_backlog = replication_metrics.get("total_pending_outbound", 0)

        active_partitions = 0
        if partition_metrics:
            active_partitions = partition_metrics.get("active_partitions", 0)

        active_failovers = 0
        if failover_metrics:
            active_failovers = failover_metrics.get("active_failovers", 0)

        autonomous_regions = 0
        if operations_metrics:
            mode = operations_metrics.get("mode", "connected")
            if mode in ("autonomous", "isolated"):
                autonomous_regions = 1

        snapshot = OrbitalSnapshot(
            ts=now_ts(),
            total_nodes=total_nodes,
            active_nodes=active,
            disconnected_nodes=disconnected,
            orbital_nodes=orbital,
            ground_nodes=ground,
            avg_signal_latency_ms=round(avg_latency, 1),
            max_signal_latency_ms=round(max_latency, 1),
            total_links=total_links,
            active_links=active_links,
            degraded_links=degraded_links,
            offline_links=offline_links,
            sync_lag_avg_s=round(sync_lag_avg, 3),
            replication_backlog=replication_backlog,
            active_partitions=active_partitions,
            active_failovers=active_failovers,
            autonomous_regions=autonomous_regions,
            messages_in_queue=messages_in_queue,
            by_orbit=by_orbit,
            by_status=by_status,
        )

        with self._lock:
            self._snapshots.append(snapshot)
            if len(self._snapshots) > self._max_snapshots:
                self._snapshots = self._snapshots[-self._max_snapshots:]
            self._total_collections += 1

        return snapshot

    def record_failover(self, event: Dict[str, Any]) -> None:
        with self._lock:
            event["recorded_at"] = now_ts()
            self._failover_log.append(event)
            if len(self._failover_log) > self._max_snapshots:
                self._failover_log = self._failover_log[-self._max_snapshots:]

    def record_partition(self, event: Dict[str, Any]) -> None:
        with self._lock:
            event["recorded_at"] = now_ts()
            self._partition_log.append(event)
            if len(self._partition_log) > self._max_snapshots:
                self._partition_log = self._partition_log[-self._max_snapshots:]

    def record_sync(self, event: Dict[str, Any]) -> None:
        with self._lock:
            event["recorded_at"] = now_ts()
            self._sync_log.append(event)
            if len(self._sync_log) > self._max_snapshots:
                self._sync_log = self._sync_log[-self._max_snapshots:]

    def latest_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            if not self._snapshots:
                return {}
            return self._snapshots[-1].model_dump(mode="json")

    def snapshot_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [s.model_dump(mode="json") for s in reversed(self._snapshots)][:limit]

    def failover_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._failover_log))[:limit]

    def partition_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._partition_log))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            latest = self._snapshots[-1] if self._snapshots else None
            return {
                "ts": now_ts(),
                "total_collections": self._total_collections,
                "failover_events": len(self._failover_log),
                "partition_events": len(self._partition_log),
                "sync_events": len(self._sync_log),
                "latest": latest.model_dump(mode="json") if latest else {},
            }
