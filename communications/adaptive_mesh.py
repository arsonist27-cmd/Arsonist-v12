"""v16 Adaptive Communication Mesh.

Dynamically optimizes bandwidth, replication frequency, synchronization
timing, and data prioritization across degraded network conditions with
support for burst synchronization and intelligent compression.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("communications.adaptive_mesh")


class MeshLinkState(str, Enum):
    optimal = "optimal"
    degraded = "degraded"
    congested = "congested"
    intermittent = "intermittent"
    offline = "offline"


class SyncMode(str, Enum):
    realtime = "realtime"
    periodic = "periodic"
    burst = "burst"
    opportunistic = "opportunistic"
    deferred = "deferred"


class DataPriority(str, Enum):
    critical = "critical"
    operational = "operational"
    replication = "replication"
    telemetry = "telemetry"
    bulk = "bulk"


class MeshLink(BaseModel):
    link_id: str
    node_a: str = ""
    node_b: str = ""
    state: MeshLinkState = MeshLinkState.optimal
    bandwidth_kbps: float = 0.0
    max_bandwidth_kbps: float = 0.0
    latency_ms: float = 0.0
    packet_loss_pct: float = 0.0
    jitter_ms: float = 0.0
    utilization_pct: float = 0.0
    sync_mode: SyncMode = SyncMode.realtime
    compression_enabled: bool = False
    compression_ratio: float = 1.0
    last_activity_ts: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BurstWindow(BaseModel):
    window_id: str
    link_id: str = ""
    start_ts: float = 0.0
    end_ts: float = 0.0
    bytes_transferred: int = 0
    messages_transferred: int = 0
    compression_ratio: float = 1.0


class AdaptiveMesh:
    """Dynamically manages communication links with bandwidth adaptation,
    burst synchronization, and intelligent data prioritization."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._links: Dict[str, MeshLink] = {}
        self._burst_history: List[BurstWindow] = []
        self._priority_queues: Dict[str, List[Dict[str, Any]]] = {}
        self._total_bytes_transferred = 0
        self._total_bursts = 0
        self._total_adaptations = 0
        self._events: List[Dict[str, Any]] = []

    def register_link(self, link: MeshLink) -> None:
        with self._lock:
            link.last_activity_ts = now_ts()
            if link.max_bandwidth_kbps <= 0:
                link.max_bandwidth_kbps = link.bandwidth_kbps
            self._links[link.link_id] = link
            self._add_event("link_registered", link.link_id,
                            node_a=link.node_a, node_b=link.node_b)

    def update_link_quality(self, link_id: str,
                            bandwidth_kbps: Optional[float] = None,
                            latency_ms: Optional[float] = None,
                            packet_loss_pct: Optional[float] = None,
                            jitter_ms: Optional[float] = None) -> Optional[MeshLink]:
        with self._lock:
            link = self._links.get(link_id)
            if not link:
                return None
            if bandwidth_kbps is not None:
                link.bandwidth_kbps = bandwidth_kbps
            if latency_ms is not None:
                link.latency_ms = latency_ms
            if packet_loss_pct is not None:
                link.packet_loss_pct = packet_loss_pct
            if jitter_ms is not None:
                link.jitter_ms = jitter_ms
            link.last_activity_ts = now_ts()

            self._adapt_link(link)
            return link

    def _adapt_link(self, link: MeshLink) -> None:
        old_state = link.state
        old_mode = link.sync_mode

        if link.packet_loss_pct > 20 or link.bandwidth_kbps <= 0:
            link.state = MeshLinkState.offline
            link.sync_mode = SyncMode.deferred
        elif link.packet_loss_pct > 10 or link.jitter_ms > 200:
            link.state = MeshLinkState.intermittent
            link.sync_mode = SyncMode.burst
            link.compression_enabled = True
        elif link.utilization_pct > 80 or link.bandwidth_kbps < link.max_bandwidth_kbps * 0.3:
            link.state = MeshLinkState.congested
            link.sync_mode = SyncMode.periodic
            link.compression_enabled = True
        elif link.latency_ms > 1000 or link.bandwidth_kbps < link.max_bandwidth_kbps * 0.6:
            link.state = MeshLinkState.degraded
            link.sync_mode = SyncMode.periodic
        else:
            link.state = MeshLinkState.optimal
            link.sync_mode = SyncMode.realtime
            link.compression_enabled = False

        if old_state != link.state or old_mode != link.sync_mode:
            self._total_adaptations += 1
            self._add_event("link_adapted", link.link_id,
                            old_state=old_state.value, new_state=link.state.value,
                            old_mode=old_mode.value, new_mode=link.sync_mode.value)

    def execute_burst(self, link_id: str, data_bytes: int,
                      message_count: int) -> Optional[BurstWindow]:
        with self._lock:
            link = self._links.get(link_id)
            if not link or link.state == MeshLinkState.offline:
                return None

            ts = now_ts()
            compression = link.compression_ratio if link.compression_enabled else 1.0
            actual_bytes = int(data_bytes / compression) if compression > 0 else data_bytes

            window = BurstWindow(
                window_id=f"burst-{link_id}-{int(ts)}",
                link_id=link_id,
                start_ts=ts,
                end_ts=ts + (actual_bytes / (link.bandwidth_kbps * 125 + 1)),
                bytes_transferred=actual_bytes,
                messages_transferred=message_count,
                compression_ratio=compression,
            )
            self._burst_history.append(window)
            if len(self._burst_history) > self._max_history:
                self._burst_history = self._burst_history[-self._max_history:]

            self._total_bursts += 1
            self._total_bytes_transferred += actual_bytes
            link.last_activity_ts = ts
            self._add_event("burst_executed", link_id,
                            bytes=actual_bytes, messages=message_count)
            return window

    def enqueue_data(self, link_id: str, priority: DataPriority,
                     data: Dict[str, Any]) -> bool:
        with self._lock:
            key = f"{link_id}:{priority.value}"
            if key not in self._priority_queues:
                self._priority_queues[key] = []
            data["enqueued_at"] = now_ts()
            data["priority"] = priority.value
            self._priority_queues[key].append(data)
            return True

    def dequeue_data(self, link_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            result = []
            for priority in DataPriority:
                key = f"{link_id}:{priority.value}"
                queue = self._priority_queues.get(key, [])
                remaining = limit - len(result)
                if remaining <= 0:
                    break
                batch = queue[:remaining]
                self._priority_queues[key] = queue[len(batch):]
                result.extend(batch)
            return result

    def link_status(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [l.model_dump(mode="json") for l in self._links.values()]

    def healthy_links(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [l.model_dump(mode="json") for l in self._links.values()
                    if l.state in (MeshLinkState.optimal, MeshLinkState.degraded)]

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
            active = sum(1 for l in self._links.values()
                         if l.state != MeshLinkState.offline)
            degraded = sum(1 for l in self._links.values()
                           if l.state in (MeshLinkState.degraded, MeshLinkState.congested,
                                          MeshLinkState.intermittent))
            queued = sum(len(q) for q in self._priority_queues.values())
            return {
                "ts": now_ts(),
                "total_links": len(self._links),
                "active_links": active,
                "degraded_links": degraded,
                "total_bursts": self._total_bursts,
                "total_bytes_transferred": self._total_bytes_transferred,
                "total_adaptations": self._total_adaptations,
                "queued_messages": queued,
            }
