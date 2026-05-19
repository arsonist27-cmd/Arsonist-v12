"""v16 Disconnected Operations.

Enables each cluster to continue operating independently during
isolation with local scheduling, local healing, local optimization,
and temporary autonomous governance.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("resilience.disconnected_operations")


class OperatingMode(str, Enum):
    connected = "connected"
    degraded = "degraded"
    autonomous = "autonomous"
    isolated = "isolated"
    reconnecting = "reconnecting"


class GovernanceScope(str, Enum):
    scheduling = "scheduling"
    healing = "healing"
    scaling = "scaling"
    optimization = "optimization"
    failover = "failover"
    replication = "replication"


class DisconnectedState(BaseModel):
    region: str
    mode: OperatingMode = OperatingMode.connected
    disconnected_at: float = 0.0
    reconnected_at: float = 0.0
    autonomous_duration_s: float = 0.0
    local_decisions_made: int = 0
    local_workloads_scheduled: int = 0
    local_heals_performed: int = 0
    governance_scopes: List[GovernanceScope] = Field(default_factory=list)
    pending_sync_events: int = 0
    last_sync_ts: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LocalDecision(BaseModel):
    decision_id: str
    region: str = ""
    scope: GovernanceScope = GovernanceScope.scheduling
    description: str = ""
    made_at: float = 0.0
    synced: bool = False
    synced_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DisconnectedOperationsManager:
    """Manages autonomous operation of infrastructure clusters during
    isolation with local governance, decision journaling, and
    resynchronization on reconnection."""

    def __init__(self, local_region: str = "local",
                 max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._local_region = local_region
        self._max_history = max_history
        self._state = DisconnectedState(region=local_region)
        self._local_decisions: List[LocalDecision] = []
        self._sync_journal: List[Dict[str, Any]] = []
        self._peer_states: Dict[str, DisconnectedState] = {}
        self._events: List[Dict[str, Any]] = []

    def enter_autonomous(self, scopes: Optional[List[GovernanceScope]] = None) -> DisconnectedState:
        with self._lock:
            if scopes is None:
                scopes = list(GovernanceScope)
            self._state.mode = OperatingMode.autonomous
            self._state.disconnected_at = now_ts()
            self._state.governance_scopes = scopes
            self._add_event("entered_autonomous", self._local_region,
                            scopes=[s.value for s in scopes])
            return self._state.model_copy()

    def enter_isolated(self) -> DisconnectedState:
        with self._lock:
            self._state.mode = OperatingMode.isolated
            self._state.disconnected_at = now_ts()
            self._state.governance_scopes = list(GovernanceScope)
            self._add_event("entered_isolated", self._local_region)
            return self._state.model_copy()

    def reconnect(self) -> DisconnectedState:
        with self._lock:
            ts = now_ts()
            self._state.mode = OperatingMode.reconnecting
            self._state.reconnected_at = ts
            if self._state.disconnected_at > 0:
                self._state.autonomous_duration_s = round(
                    ts - self._state.disconnected_at, 3)
            self._add_event("reconnecting", self._local_region,
                            duration_s=self._state.autonomous_duration_s)
            return self._state.model_copy()

    def complete_resync(self) -> DisconnectedState:
        with self._lock:
            self._state.mode = OperatingMode.connected
            self._state.last_sync_ts = now_ts()
            self._state.pending_sync_events = 0
            unsynced = [d for d in self._local_decisions if not d.synced]
            for d in unsynced:
                d.synced = True
                d.synced_at = now_ts()
            self._add_event("resync_completed", self._local_region,
                            decisions_synced=len(unsynced))
            return self._state.model_copy()

    def record_decision(self, decision: LocalDecision) -> LocalDecision:
        with self._lock:
            decision.region = self._local_region
            decision.made_at = now_ts()
            self._local_decisions.append(decision)
            self._state.local_decisions_made += 1
            self._state.pending_sync_events += 1

            if decision.scope == GovernanceScope.scheduling:
                self._state.local_workloads_scheduled += 1
            elif decision.scope == GovernanceScope.healing:
                self._state.local_heals_performed += 1

            self._sync_journal.append({
                "type": "local_decision",
                "decision_id": decision.decision_id,
                "scope": decision.scope.value,
                "ts": decision.made_at,
                "synced": False,
            })

            if len(self._local_decisions) > self._max_history:
                self._local_decisions = self._local_decisions[-self._max_history:]
            if len(self._sync_journal) > self._max_history * 2:
                self._sync_journal = self._sync_journal[-self._max_history * 2:]

            return decision

    def get_sync_journal(self, since_ts: float = 0.0) -> List[Dict[str, Any]]:
        with self._lock:
            if since_ts <= 0:
                return list(self._sync_journal)
            return [e for e in self._sync_journal if e.get("ts", 0) > since_ts]

    def get_unsynced_decisions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.model_dump(mode="json") for d in self._local_decisions
                    if not d.synced]

    def has_governance(self, scope: GovernanceScope) -> bool:
        with self._lock:
            if self._state.mode == OperatingMode.connected:
                return False
            return scope in self._state.governance_scopes

    def update_peer(self, region: str, mode: str) -> None:
        with self._lock:
            if region not in self._peer_states:
                self._peer_states[region] = DisconnectedState(region=region)
            self._peer_states[region].mode = OperatingMode(mode)

    def peer_states(self) -> Dict[str, str]:
        with self._lock:
            return {r: s.mode.value for r, s in self._peer_states.items()}

    def current_state(self) -> Dict[str, Any]:
        with self._lock:
            return self._state.model_dump(mode="json")

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
            unsynced = sum(1 for d in self._local_decisions if not d.synced)
            return {
                "ts": now_ts(),
                "region": self._local_region,
                "mode": self._state.mode.value,
                "local_decisions_made": self._state.local_decisions_made,
                "local_workloads_scheduled": self._state.local_workloads_scheduled,
                "local_heals_performed": self._state.local_heals_performed,
                "pending_sync_events": unsynced,
                "governance_scopes": [s.value for s in self._state.governance_scopes],
                "autonomous_duration_s": self._state.autonomous_duration_s,
            }
