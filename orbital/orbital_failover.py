"""v16 Orbital Failover.

Manages failover procedures for orbital compute nodes including
disconnection handling, workload migration to ground or alternate
orbital nodes, and recovery after communication restoration.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("orbital.orbital_failover")


class FailoverTrigger(str, Enum):
    disconnection = "disconnection"
    blackout = "blackout"
    signal_loss = "signal_loss"
    compute_failure = "compute_failure"
    thermal_critical = "thermal_critical"
    power_loss = "power_loss"
    bandwidth_exhaustion = "bandwidth_exhaustion"


class FailoverState(str, Enum):
    monitoring = "monitoring"
    triggered = "triggered"
    migrating = "migrating"
    completed = "completed"
    recovered = "recovered"
    failed = "failed"


class OrbitalFailoverEvent(BaseModel):
    event_id: str
    source_node: str = ""
    target_node: str = ""
    trigger: FailoverTrigger = FailoverTrigger.disconnection
    state: FailoverState = FailoverState.monitoring
    workloads_affected: int = 0
    workloads_migrated: int = 0
    failover_time_ms: float = 0.0
    recovery_time_ms: float = 0.0
    data_preserved_pct: float = 100.0
    triggered_at: float = 0.0
    completed_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class OrbitalFailoverManager:
    """Manages failover for orbital nodes with disconnection-aware
    migration, autonomous fallback, and recovery orchestration."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._active_failovers: Dict[str, OrbitalFailoverEvent] = {}
        self._completed: List[OrbitalFailoverEvent] = []
        self._total_failovers = 0
        self._total_recovered = 0
        self._total_workloads_migrated = 0
        self._events: List[Dict[str, Any]] = []

    def trigger_failover(self, event: OrbitalFailoverEvent) -> OrbitalFailoverEvent:
        with self._lock:
            event.triggered_at = now_ts()
            event.state = FailoverState.triggered
            self._active_failovers[event.event_id] = event
            self._total_failovers += 1
            self._add_event("failover_triggered", event.event_id,
                            source=event.source_node,
                            trigger=event.trigger.value,
                            workloads=event.workloads_affected)
            return event

    def detect_failures(self, nodes: List[Dict[str, Any]],
                        timeout_s: float = 120.0) -> List[OrbitalFailoverEvent]:
        ts = now_ts()
        failovers = []
        with self._lock:
            for node in nodes:
                node_id = node.get("node_id", "")
                status = node.get("status", "active")
                last_hb = node.get("last_heartbeat", ts)

                already_active = any(
                    f.source_node == node_id and f.state in (
                        FailoverState.triggered, FailoverState.migrating)
                    for f in self._active_failovers.values()
                )
                if already_active:
                    continue

                trigger = None
                if status == "disconnected" or (ts - last_hb > timeout_s):
                    trigger = FailoverTrigger.disconnection
                elif status == "blackout":
                    trigger = FailoverTrigger.blackout
                elif node.get("thermal_c", 0) > 85:
                    trigger = FailoverTrigger.thermal_critical
                elif node.get("power_watts", 100) <= 0:
                    trigger = FailoverTrigger.power_loss

                if trigger:
                    event = OrbitalFailoverEvent(
                        event_id=f"ofail-{node_id}-{int(ts)}",
                        source_node=node_id,
                        trigger=trigger,
                        workloads_affected=node.get("active_workloads", 0),
                    )
                    failovers.append(self.trigger_failover(event))

        return failovers

    def execute_migration(self, event_id: str, target_node: str,
                          workloads_migrated: int = 0) -> Optional[OrbitalFailoverEvent]:
        with self._lock:
            event = self._active_failovers.get(event_id)
            if not event:
                return None
            event.state = FailoverState.migrating
            event.target_node = target_node
            event.workloads_migrated = workloads_migrated
            self._add_event("migration_started", event_id,
                            target=target_node, workloads=workloads_migrated)
            return event

    def complete_failover(self, event_id: str, success: bool = True,
                          data_preserved_pct: float = 100.0) -> Optional[OrbitalFailoverEvent]:
        with self._lock:
            event = self._active_failovers.get(event_id)
            if not event:
                return None
            event.completed_at = now_ts()
            event.failover_time_ms = round((event.completed_at - event.triggered_at) * 1000, 2)
            event.data_preserved_pct = data_preserved_pct
            event.state = FailoverState.completed if success else FailoverState.failed
            self._total_workloads_migrated += event.workloads_migrated
            self._finalize(event_id)
            self._add_event("failover_completed", event_id,
                            success=success,
                            time_ms=event.failover_time_ms)
            return event

    def recover_node(self, event_id: str) -> Optional[OrbitalFailoverEvent]:
        with self._lock:
            for completed in reversed(self._completed):
                if completed.event_id == event_id:
                    completed.state = FailoverState.recovered
                    completed.recovery_time_ms = round(
                        (now_ts() - completed.completed_at) * 1000, 2)
                    self._total_recovered += 1
                    self._add_event("node_recovered", event_id,
                                    source=completed.source_node)
                    return completed
            return None

    def _finalize(self, event_id: str) -> None:
        event = self._active_failovers.pop(event_id, None)
        if event:
            self._completed.append(event)
            if len(self._completed) > self._max_history:
                self._completed = self._completed[-self._max_history:]

    def active_failovers(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in self._active_failovers.values()]

    def recent_completed(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in reversed(self._completed)][:limit]

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._completed[-50:] if self._completed else []
            avg_failover = sum(f.failover_time_ms for f in recent) / len(recent) if recent else 0.0
            avg_preserved = sum(f.data_preserved_pct for f in recent) / len(recent) if recent else 100.0
            return {
                "ts": now_ts(),
                "total_failovers": self._total_failovers,
                "total_recovered": self._total_recovered,
                "total_workloads_migrated": self._total_workloads_migrated,
                "active_failovers": len(self._active_failovers),
                "avg_failover_time_ms": round(avg_failover, 1),
                "avg_data_preserved_pct": round(avg_preserved, 1),
            }
