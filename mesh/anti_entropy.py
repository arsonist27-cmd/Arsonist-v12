from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple

from distributed_queue.event_log import EventLog
from mesh.peer_registry import PeerRegistry
from shared.utils import now_ts


def _digest(payload: Dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()[:16]


class AntiEntropyEngine:
    """
    Periodic digest exchange to detect drift; pulls missing tail events without infinite loops
    (bounded replay window, monotonic remote cursors).
    """

    def __init__(self, event_log: EventLog, registry: PeerRegistry) -> None:
        self.event_log = event_log
        self.registry = registry
        self._remote_cursors: Dict[str, int] = {}

    def local_summary(self) -> Dict[str, Any]:
        tail = self.event_log.tail(50)
        return {
            "last_seq": self.event_log.last_seq(),
            "digest": _digest({"ids": [e.event_id for e in tail]}),
            "ts": now_ts(),
        }

    def plan_pull(self, remote_cluster: str, remote_last_seq: int) -> Tuple[int, int]:
        """Return (from_seq, to_seq) inclusive-ish window for remote to send."""
        local = self.event_log.last_seq()
        last_cursor = self._remote_cursors.get(remote_cluster, 0)
        start = max(last_cursor, remote_last_seq - 200)
        end = local
        self._remote_cursors[remote_cluster] = max(last_cursor, remote_last_seq)
        return start, end

    def merge_events(self, events: List[Dict[str, Any]]) -> int:
        return self.event_log.merge_events(events)
