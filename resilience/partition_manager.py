"""v16 Partition Manager.

Detects, tracks, and manages network partitions across the distributed
infrastructure with support for partition healing, split-brain detection,
and coordinated reconvergence.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("resilience.partition_manager")


class PartitionState(str, Enum):
    suspected = "suspected"
    confirmed = "confirmed"
    healing = "healing"
    healed = "healed"
    chronic = "chronic"


class PartitionSeverity(str, Enum):
    minor = "minor"
    moderate = "moderate"
    major = "major"
    total = "total"


class NetworkPartition(BaseModel):
    partition_id: str
    side_a: List[str] = Field(default_factory=list)
    side_b: List[str] = Field(default_factory=list)
    state: PartitionState = PartitionState.suspected
    severity: PartitionSeverity = PartitionSeverity.minor
    detected_at: float = 0.0
    confirmed_at: float = 0.0
    healed_at: float = 0.0
    duration_s: float = 0.0
    affected_workloads: int = 0
    split_brain_detected: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PartitionManager:
    """Detects and manages network partitions with split-brain detection
    and coordinated partition healing."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._active: Dict[str, NetworkPartition] = {}
        self._healed: List[NetworkPartition] = []
        self._total_partitions = 0
        self._total_healed = 0
        self._total_split_brains = 0
        self._events: List[Dict[str, Any]] = []

    def detect_partition(self, partition: NetworkPartition) -> NetworkPartition:
        with self._lock:
            partition.detected_at = now_ts()
            partition.state = PartitionState.suspected
            self._active[partition.partition_id] = partition
            self._total_partitions += 1
            self._add_event("partition_detected", partition.partition_id,
                            severity=partition.severity.value,
                            side_a=len(partition.side_a),
                            side_b=len(partition.side_b))
            return partition

    def confirm_partition(self, partition_id: str,
                          split_brain: bool = False) -> Optional[NetworkPartition]:
        with self._lock:
            partition = self._active.get(partition_id)
            if not partition:
                return None
            partition.state = PartitionState.confirmed
            partition.confirmed_at = now_ts()
            partition.split_brain_detected = split_brain
            if split_brain:
                self._total_split_brains += 1
            self._add_event("partition_confirmed", partition_id,
                            split_brain=split_brain)
            return partition

    def begin_healing(self, partition_id: str) -> Optional[NetworkPartition]:
        with self._lock:
            partition = self._active.get(partition_id)
            if not partition:
                return None
            partition.state = PartitionState.healing
            self._add_event("partition_healing", partition_id)
            return partition

    def heal_partition(self, partition_id: str) -> Optional[NetworkPartition]:
        with self._lock:
            partition = self._active.get(partition_id)
            if not partition:
                return None
            partition.state = PartitionState.healed
            partition.healed_at = now_ts()
            partition.duration_s = round(partition.healed_at - partition.detected_at, 3)
            self._total_healed += 1
            self._finalize(partition_id)
            self._add_event("partition_healed", partition_id,
                            duration_s=partition.duration_s)
            return partition

    def mark_chronic(self, partition_id: str) -> Optional[NetworkPartition]:
        with self._lock:
            partition = self._active.get(partition_id)
            if not partition:
                return None
            partition.state = PartitionState.chronic
            self._add_event("partition_chronic", partition_id)
            return partition

    def check_partitions(self, connectivity_matrix: Dict[str, List[str]]) -> List[NetworkPartition]:
        """Detect partitions from a connectivity matrix where keys are node IDs
        and values are lists of reachable peer IDs."""
        with self._lock:
            all_nodes = set(connectivity_matrix.keys())
            if not all_nodes:
                return []

            groups: List[set] = []
            visited: set = set()
            for node in all_nodes:
                if node in visited:
                    continue
                group: set = set()
                stack = [node]
                while stack:
                    current = stack.pop()
                    if current in visited:
                        continue
                    visited.add(current)
                    group.add(current)
                    for peer in connectivity_matrix.get(current, []):
                        if peer in all_nodes and peer not in visited:
                            stack.append(peer)
                if group:
                    groups.append(group)

            if len(groups) <= 1:
                return []

            detected = []
            groups.sort(key=len, reverse=True)
            for i in range(1, len(groups)):
                side_a = sorted(groups[0])
                side_b = sorted(groups[i])
                severity = PartitionSeverity.minor
                ratio = len(side_b) / len(all_nodes)
                if ratio > 0.4:
                    severity = PartitionSeverity.major
                elif ratio > 0.2:
                    severity = PartitionSeverity.moderate

                pid = f"part-{int(now_ts())}-{i}"
                partition = NetworkPartition(
                    partition_id=pid,
                    side_a=side_a,
                    side_b=side_b,
                    severity=severity,
                )
                detected.append(self.detect_partition(partition))

            return detected

    def _finalize(self, partition_id: str) -> None:
        partition = self._active.pop(partition_id, None)
        if partition:
            self._healed.append(partition)
            if len(self._healed) > self._max_history:
                self._healed = self._healed[-self._max_history:]

    def active_partitions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._active.values()]

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
            recent = self._healed[-50:] if self._healed else []
            avg_duration = sum(p.duration_s for p in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_partitions": self._total_partitions,
                "active_partitions": len(self._active),
                "total_healed": self._total_healed,
                "total_split_brains": self._total_split_brains,
                "avg_partition_duration_s": round(avg_duration, 1),
            }
