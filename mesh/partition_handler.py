from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

from mesh.mesh_protocol import MeshEventType
from shared.utils import now_ts


@dataclass
class PartitionHandler:
    """
    Tracks unreachable peers and split-brain hints using simple quorum-free heuristics.
    After reconnection, peers are merged via gossip + anti-entropy; stale events trimmed downstream.
    """

    unreachable: Set[str] = field(default_factory=set)
    last_partition_ts: float = 0.0

    def mark_unreachable(self, cluster_ids: List[str]) -> None:
        for c in cluster_ids:
            self.unreachable.add(c)
        self.last_partition_ts = now_ts()

    def mark_reachable(self, cluster_id: str) -> None:
        self.unreachable.discard(cluster_id)

    def snapshot(self) -> Dict[str, object]:
        return {
            "unreachable_peers": sorted(self.unreachable),
            "last_partition_ts": self.last_partition_ts,
            "ts": now_ts(),
        }

    def classify_event(self, event: str) -> bool:
        if event == MeshEventType.PARTITION_DETECTED.value:
            self.last_partition_ts = now_ts()
            return True
        return False
