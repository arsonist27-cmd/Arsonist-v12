from __future__ import annotations

import os
from typing import Any, List

try:
    from pysyncobj import SyncObj
except ImportError:  # pragma: no cover
    SyncObj = None  # type: ignore[misc,assignment]


class RaftAdapter:
    """
    Optional Raft coordination via pysyncobj when ARSONIST_CONSENSUS_MODE=raft.
    Falls back to single-node leader if dependency or configuration is missing.
    """

    def __init__(self, self_node: str, partners: List[str]) -> None:
        self.self_node = self_node
        self.partners = partners
        self._obj: Any = None
        self._mode = os.getenv("ARSONIST_CONSENSUS_MODE", "disabled").lower()

    def start(self) -> None:
        if self._mode != "raft" or SyncObj is None:
            return
        bind = os.getenv("ARSONIST_RAFT_BIND", "127.0.0.1:9009")
        partner_addrs = [p.strip() for p in self.partners if p.strip()]
        if not partner_addrs:
            return
        try:
            self._obj = SyncObj(bind, partner_addrs)  # type: ignore[misc]
        except Exception:
            self._obj = None

    def is_leader(self) -> bool:
        if self._obj is None:
            return True
        try:
            return bool(self._obj.isLeader())
        except Exception:
            return True

    def stop(self) -> None:
        if self._obj is None:
            return
        try:
            self._obj.destroy()
        except Exception:
            pass
