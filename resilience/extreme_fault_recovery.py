"""v16 Extreme Fault Recovery.

Provides infrastructure partition recovery, delayed state reconciliation,
conflict resolution, and workload continuity restoration for environments
experiencing prolonged disconnection or catastrophic failures.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("resilience.extreme_fault_recovery")


class RecoveryPhase(str, Enum):
    detection = "detection"
    assessment = "assessment"
    reconciliation = "reconciliation"
    restoration = "restoration"
    verification = "verification"
    completed = "completed"
    failed = "failed"


class ConflictType(str, Enum):
    state_divergence = "state_divergence"
    version_mismatch = "version_mismatch"
    resource_conflict = "resource_conflict"
    schedule_overlap = "schedule_overlap"
    ownership_ambiguity = "ownership_ambiguity"


class RecoveryAction(BaseModel):
    action_id: str
    phase: RecoveryPhase = RecoveryPhase.detection
    description: str = ""
    source_region: str = ""
    target_region: str = ""
    resources_affected: int = 0
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    data_recovered_pct: float = 0.0
    workloads_restored: int = 0
    started_at: float = 0.0
    completed_at: float = 0.0
    duration_ms: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StateConflict(BaseModel):
    conflict_id: str
    conflict_type: ConflictType = ConflictType.state_divergence
    resource_type: str = ""
    resource_id: str = ""
    region_a: str = ""
    region_b: str = ""
    value_a: Dict[str, Any] = Field(default_factory=dict)
    value_b: Dict[str, Any] = Field(default_factory=dict)
    resolved: bool = False
    resolution: str = ""
    detected_at: float = 0.0
    resolved_at: float = 0.0


class ExtremeFaultRecovery:
    """Manages recovery from extreme infrastructure faults including
    prolonged partitions, state divergence, and catastrophic failures."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._active_recoveries: Dict[str, RecoveryAction] = {}
        self._completed: List[RecoveryAction] = []
        self._conflicts: List[StateConflict] = []
        self._total_recoveries = 0
        self._total_conflicts = 0
        self._total_resolved = 0
        self._total_workloads_restored = 0
        self._events: List[Dict[str, Any]] = []

    def initiate_recovery(self, action: RecoveryAction) -> RecoveryAction:
        with self._lock:
            action.started_at = now_ts()
            action.phase = RecoveryPhase.assessment
            self._active_recoveries[action.action_id] = action
            self._total_recoveries += 1
            self._add_event("recovery_initiated", action.action_id,
                            source=action.source_region,
                            target=action.target_region)
            return action

    def detect_conflicts(self, region_a: str, region_b: str,
                         state_a: Dict[str, Any],
                         state_b: Dict[str, Any]) -> List[StateConflict]:
        conflicts = []
        ts = now_ts()

        all_keys = set(list(state_a.keys()) + list(state_b.keys()))
        for key in all_keys:
            val_a = state_a.get(key)
            val_b = state_b.get(key)
            if val_a is not None and val_b is not None and val_a != val_b:
                conflict = StateConflict(
                    conflict_id=f"conflict-{region_a}-{region_b}-{key}-{int(ts)}",
                    conflict_type=ConflictType.state_divergence,
                    resource_type="state",
                    resource_id=key,
                    region_a=region_a,
                    region_b=region_b,
                    value_a={"value": val_a} if not isinstance(val_a, dict) else val_a,
                    value_b={"value": val_b} if not isinstance(val_b, dict) else val_b,
                    detected_at=ts,
                )
                conflicts.append(conflict)

        with self._lock:
            self._conflicts.extend(conflicts)
            self._total_conflicts += len(conflicts)
            if len(self._conflicts) > self._max_history:
                self._conflicts = self._conflicts[-self._max_history:]
            if conflicts:
                self._add_event("conflicts_detected", f"{region_a}-{region_b}",
                                count=len(conflicts))

        return conflicts

    def resolve_conflict(self, conflict_id: str,
                         resolution: str = "last_write_wins") -> bool:
        with self._lock:
            for conflict in self._conflicts:
                if conflict.conflict_id == conflict_id and not conflict.resolved:
                    conflict.resolved = True
                    conflict.resolution = resolution
                    conflict.resolved_at = now_ts()
                    self._total_resolved += 1
                    self._add_event("conflict_resolved", conflict_id,
                                    resolution=resolution)
                    return True
            return False

    def reconcile_state(self, action_id: str,
                        conflicts_resolved: int = 0) -> Optional[RecoveryAction]:
        with self._lock:
            action = self._active_recoveries.get(action_id)
            if not action:
                return None
            action.phase = RecoveryPhase.reconciliation
            action.conflicts_resolved = conflicts_resolved
            self._add_event("reconciliation_started", action_id,
                            resolved=conflicts_resolved)
            return action

    def restore_workloads(self, action_id: str,
                          workloads_restored: int = 0,
                          data_recovered_pct: float = 100.0) -> Optional[RecoveryAction]:
        with self._lock:
            action = self._active_recoveries.get(action_id)
            if not action:
                return None
            action.phase = RecoveryPhase.restoration
            action.workloads_restored = workloads_restored
            action.data_recovered_pct = data_recovered_pct
            self._total_workloads_restored += workloads_restored
            self._add_event("workloads_restoring", action_id,
                            count=workloads_restored,
                            data_pct=data_recovered_pct)
            return action

    def complete_recovery(self, action_id: str,
                          success: bool = True) -> Optional[RecoveryAction]:
        with self._lock:
            action = self._active_recoveries.get(action_id)
            if not action:
                return None
            action.completed_at = now_ts()
            action.duration_ms = round((action.completed_at - action.started_at) * 1000, 2)
            action.phase = RecoveryPhase.completed if success else RecoveryPhase.failed
            self._finalize(action_id)
            self._add_event("recovery_completed", action_id,
                            success=success, duration_ms=action.duration_ms)
            return action

    def _finalize(self, action_id: str) -> None:
        action = self._active_recoveries.pop(action_id, None)
        if action:
            self._completed.append(action)
            if len(self._completed) > self._max_history:
                self._completed = self._completed[-self._max_history:]

    def active_recoveries(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in self._active_recoveries.values()]

    def unresolved_conflicts(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [c.model_dump(mode="json") for c in self._conflicts if not c.resolved]

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
            avg_duration = sum(a.duration_ms for a in recent) / len(recent) if recent else 0.0
            avg_data = sum(a.data_recovered_pct for a in recent) / len(recent) if recent else 100.0
            return {
                "ts": now_ts(),
                "total_recoveries": self._total_recoveries,
                "active_recoveries": len(self._active_recoveries),
                "total_conflicts": self._total_conflicts,
                "total_resolved": self._total_resolved,
                "unresolved_conflicts": sum(1 for c in self._conflicts if not c.resolved),
                "total_workloads_restored": self._total_workloads_restored,
                "avg_recovery_duration_ms": round(avg_duration, 1),
                "avg_data_recovered_pct": round(avg_data, 1),
            }
