"""v16 Asynchronous Replication.

Provides eventual-consistency replication across high-latency links using
append-only event logs, CRDT-inspired merge strategies, and configurable
synchronization windows for disconnected or delay-tolerant regions.
"""
from __future__ import annotations

import threading
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("deep_space.async_replication")


class ReplicationState(str, Enum):
    synced = "synced"
    pending = "pending"
    replicating = "replicating"
    diverged = "diverged"
    reconciling = "reconciling"
    failed = "failed"


class ConflictStrategy(str, Enum):
    last_write_wins = "last_write_wins"
    source_priority = "source_priority"
    merge = "merge"
    manual = "manual"


class ReplicationEvent(BaseModel):
    event_id: str = ""
    source_region: str = ""
    target_region: str = ""
    event_type: str = ""
    resource_type: str = ""
    resource_id: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    vector_clock: Dict[str, int] = Field(default_factory=dict)
    created_at: float = 0.0
    replicated_at: float = 0.0
    state: ReplicationState = ReplicationState.pending


class ReplicationPeer(BaseModel):
    peer_id: str
    region: str = ""
    last_sync_ts: float = 0.0
    events_pending: int = 0
    events_synced: int = 0
    estimated_lag_s: float = 0.0
    state: ReplicationState = ReplicationState.pending
    link_latency_ms: float = 0.0


class AsyncReplicationManager:
    """Manages asynchronous replication across high-latency, potentially
    disconnected infrastructure regions using append-only event logs."""

    def __init__(self, local_region: str = "local",
                 conflict_strategy: ConflictStrategy = ConflictStrategy.last_write_wins,
                 max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._local_region = local_region
        self._conflict_strategy = conflict_strategy
        self._max_history = max_history
        self._event_log: List[ReplicationEvent] = []
        self._peers: Dict[str, ReplicationPeer] = {}
        self._outbound: Dict[str, List[ReplicationEvent]] = {}
        self._inbound: List[ReplicationEvent] = []
        self._vector_clock: Dict[str, int] = {local_region: 0}
        self._conflicts_resolved = 0
        self._total_replicated = 0

    def append_event(self, event: ReplicationEvent) -> ReplicationEvent:
        with self._lock:
            if not event.event_id:
                event.event_id = f"rev-{uuid.uuid4().hex[:12]}"
            event.source_region = self._local_region
            event.created_at = now_ts()
            self._vector_clock[self._local_region] = self._vector_clock.get(self._local_region, 0) + 1
            event.vector_clock = dict(self._vector_clock)
            self._event_log.append(event)
            if len(self._event_log) > self._max_history * 10:
                self._event_log = self._event_log[-self._max_history * 10:]

            for peer_id in self._peers:
                if peer_id not in self._outbound:
                    self._outbound[peer_id] = []
                self._outbound[peer_id].append(event)
                self._peers[peer_id].events_pending += 1

            return event

    def register_peer(self, peer: ReplicationPeer) -> None:
        with self._lock:
            self._peers[peer.peer_id] = peer
            self._outbound.setdefault(peer.peer_id, [])

    def remove_peer(self, peer_id: str) -> bool:
        with self._lock:
            if peer_id not in self._peers:
                return False
            del self._peers[peer_id]
            self._outbound.pop(peer_id, None)
            return True

    def get_outbound(self, peer_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            queue = self._outbound.get(peer_id, [])
            batch = queue[:limit]
            return [e.model_dump(mode="json") for e in batch]

    def confirm_sync(self, peer_id: str, count: int) -> bool:
        with self._lock:
            queue = self._outbound.get(peer_id)
            if queue is None:
                return False
            confirmed = queue[:count]
            self._outbound[peer_id] = queue[count:]
            peer = self._peers.get(peer_id)
            if peer:
                peer.events_synced += len(confirmed)
                peer.events_pending = len(self._outbound[peer_id])
                peer.last_sync_ts = now_ts()
                peer.state = ReplicationState.synced if not self._outbound[peer_id] else ReplicationState.pending
                peer.estimated_lag_s = 0.0 if not self._outbound[peer_id] else round(
                    now_ts() - self._outbound[peer_id][0].created_at, 3)
            self._total_replicated += len(confirmed)
            return True

    def receive_events(self, events: List[ReplicationEvent]) -> int:
        with self._lock:
            applied = 0
            for event in events:
                event.replicated_at = now_ts()
                event.state = ReplicationState.synced

                conflict = self._detect_conflict(event)
                if conflict:
                    self._resolve_conflict(event, conflict)
                    self._conflicts_resolved += 1
                else:
                    self._event_log.append(event)

                for region, clock_val in event.vector_clock.items():
                    self._vector_clock[region] = max(self._vector_clock.get(region, 0), clock_val)

                applied += 1

            if len(self._event_log) > self._max_history * 10:
                self._event_log = self._event_log[-self._max_history * 10:]
            return applied

    def _detect_conflict(self, incoming: ReplicationEvent) -> Optional[ReplicationEvent]:
        for existing in reversed(self._event_log[-500:]):
            if (existing.resource_type == incoming.resource_type
                    and existing.resource_id == incoming.resource_id
                    and existing.source_region != incoming.source_region):
                incoming_dominates = all(
                    incoming.vector_clock.get(r, 0) >= v
                    for r, v in existing.vector_clock.items()
                )
                existing_dominates = all(
                    existing.vector_clock.get(r, 0) >= v
                    for r, v in incoming.vector_clock.items()
                )
                if not incoming_dominates and not existing_dominates:
                    return existing
        return None

    def _resolve_conflict(self, incoming: ReplicationEvent, existing: ReplicationEvent) -> None:
        if self._conflict_strategy == ConflictStrategy.last_write_wins:
            if incoming.created_at >= existing.created_at:
                self._event_log.append(incoming)
            else:
                logger.info("conflict resolved: keeping existing event %s over %s",
                            existing.event_id, incoming.event_id)
        elif self._conflict_strategy == ConflictStrategy.source_priority:
            if incoming.source_region <= existing.source_region:
                self._event_log.append(incoming)
        else:
            self._event_log.append(incoming)

    def replication_lag(self) -> Dict[str, float]:
        with self._lock:
            ts = now_ts()
            lag = {}
            for peer_id, peer in self._peers.items():
                queue = self._outbound.get(peer_id, [])
                if queue:
                    lag[peer_id] = round(ts - queue[0].created_at, 3)
                else:
                    lag[peer_id] = 0.0
            return lag

    def vector_clock(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._vector_clock)

    def peer_status(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._peers.values()]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            total_pending = sum(len(q) for q in self._outbound.values())
            return {
                "ts": now_ts(),
                "local_region": self._local_region,
                "total_events": len(self._event_log),
                "total_replicated": self._total_replicated,
                "total_pending_outbound": total_pending,
                "conflicts_resolved": self._conflicts_resolved,
                "peers": len(self._peers),
                "vector_clock": dict(self._vector_clock),
                "conflict_strategy": self._conflict_strategy.value,
            }
