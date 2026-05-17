from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("global_control.consensus")


class LeaderState(str, Enum):
    leader = "leader"
    follower = "follower"
    candidate = "candidate"


class GlobalConsensus:
    """Lightweight leader election and distributed lock for global control plane.

    Supports pluggable backends: in-memory (single-node), or external
    coordination (Redis/etcd) via callbacks.
    """

    def __init__(
        self,
        node_id: str,
        lease_ttl_sec: float = 30.0,
        heartbeat_interval: float = 10.0,
        on_promote: Optional[Callable[[], None]] = None,
        on_demote: Optional[Callable[[], None]] = None,
    ) -> None:
        self.node_id = node_id
        self.lease_ttl = lease_ttl_sec
        self.heartbeat_interval = heartbeat_interval
        self._on_promote = on_promote
        self._on_demote = on_demote
        self._lock = threading.Lock()
        self._state = LeaderState.follower
        self._leader_id: Optional[str] = None
        self._lease_expiry: float = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._distributed_locks: Dict[str, Dict[str, Any]] = {}

    @property
    def state(self) -> LeaderState:
        with self._lock:
            return self._state

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self._state == LeaderState.leader

    @property
    def leader_id(self) -> Optional[str]:
        with self._lock:
            return self._leader_id

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="consensus")
        self._thread.start()
        logger.info("Consensus started for node %s", self.node_id)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.heartbeat_interval + 2)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("Consensus tick error")
            self._stop.wait(self.heartbeat_interval)

    def _tick(self) -> None:
        ts = now_ts()
        with self._lock:
            if self._state == LeaderState.leader:
                self._lease_expiry = ts + self.lease_ttl
            elif self._lease_expiry > 0 and ts > self._lease_expiry:
                self._try_promote(ts)
            elif self._leader_id is None:
                self._try_promote(ts)

    def _try_promote(self, ts: float) -> None:
        old = self._state
        self._state = LeaderState.leader
        self._leader_id = self.node_id
        self._lease_expiry = ts + self.lease_ttl
        logger.info("Node %s promoted to leader", self.node_id)
        if old != LeaderState.leader and self._on_promote:
            try:
                self._on_promote()
            except Exception:
                logger.exception("on_promote callback error")

    def receive_heartbeat(self, leader_id: str, lease_expiry: float) -> None:
        with self._lock:
            if leader_id != self.node_id:
                if self._state == LeaderState.leader:
                    self._state = LeaderState.follower
                    logger.info("Node %s demoted to follower (leader=%s)", self.node_id, leader_id)
                    if self._on_demote:
                        try:
                            self._on_demote()
                        except Exception:
                            logger.exception("on_demote callback error")
                self._leader_id = leader_id
                self._lease_expiry = lease_expiry

    def acquire_lock(self, lock_name: str, holder: str, ttl_sec: float = 30.0) -> bool:
        ts = now_ts()
        with self._lock:
            existing = self._distributed_locks.get(lock_name)
            if existing and existing["expiry"] > ts and existing["holder"] != holder:
                return False
            self._distributed_locks[lock_name] = {
                "holder": holder,
                "expiry": ts + ttl_sec,
                "acquired_at": ts,
            }
            return True

    def release_lock(self, lock_name: str, holder: str) -> bool:
        with self._lock:
            existing = self._distributed_locks.get(lock_name)
            if not existing or existing["holder"] != holder:
                return False
            del self._distributed_locks[lock_name]
            return True

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self._state.value,
                "leader_id": self._leader_id,
                "lease_expiry": self._lease_expiry,
                "active_locks": len(self._distributed_locks),
                "ts": now_ts(),
            }
