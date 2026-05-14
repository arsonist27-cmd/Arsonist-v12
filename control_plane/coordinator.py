from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Callable, Optional

import psycopg

from shared.utils import setup_logging

logger = setup_logging("control.coordinator")

CoordinatorCallback = Callable[[bool], None]


class Coordinator:
    """Abstract leadership gate for control-plane side effects (scheduler, health, autoscaler)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._is_leader = True

    def start(self) -> None:
        return

    def is_leader(self) -> bool:
        with self._lock:
            return self._is_leader

    def instance_id(self) -> str:
        return os.getenv("ARSONIST_INSTANCE_ID", "") or str(uuid.uuid4())


class SingleCoordinator(Coordinator):
    """Default: one control plane; always leader."""

    pass


class PostgresLockCoordinator(Coordinator):
    """
    Etcd-style HA: PostgreSQL session advisory lock (shared registry is the database).
    Only the holder runs mutating cluster loops; all replicas may serve reads/writes that hit the DB.
    """

    def __init__(self, dsn: str, poll_sec: float = 2.0) -> None:
        super().__init__()
        self._dsn = dsn
        self._poll_sec = poll_sec
        with self._lock:
            self._is_leader = False
        self._conn: Optional[psycopg.Connection] = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="pg-leader-election")

    def start(self) -> None:
        try:
            ok = self._try_acquire()
            with self._lock:
                self._is_leader = ok
            if ok:
                logger.info("Acquired PostgreSQL advisory leadership at startup")
        except Exception:
            logger.exception("Initial leader election failed")
        self._thread.start()

    def _ensure_conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=True)
        return self._conn

    def _try_acquire(self) -> bool:
        conn = self._ensure_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext('arsonist-control-leader-v1'))")
            row = cur.fetchone()
            return bool(row and row[0])

    def _release(self) -> None:
        if not self._conn or self._conn.closed:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext('arsonist-control-leader-v1'))")
        except Exception:
            logger.exception("advisory unlock failed")
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = None

    def _loop(self) -> None:
        was_leader = False
        while not self._stop.is_set():
            try:
                ok = self._try_acquire()
            except Exception:
                logger.exception("leader election query failed")
                ok = False
            with self._lock:
                self._is_leader = ok
            if ok and not was_leader:
                logger.info("Became control-plane leader (PostgreSQL advisory lock)")
            if not ok and was_leader:
                logger.warning("Lost control-plane leadership; stepping down side loops")
                self._release()
            was_leader = ok
            if self._stop.wait(self._poll_sec):
                break
        self._release()
        with self._lock:
            self._is_leader = False


class RaftCoordinator(Coordinator):
    """
    Optional Raft cluster using pysyncobj. Set ARSONIST_RAFT_SELF and ARSONIST_RAFT_PARTNERS (comma hosts).
    Falls back to single-leader if partners are not configured.
    """

    def __init__(self) -> None:
        super().__init__()
        self._sync: object | None = None
        self._self_addr = os.getenv("ARSONIST_RAFT_SELF", "").strip()
        partners_raw = os.getenv("ARSONIST_RAFT_PARTNERS", "").strip()
        self._partners = [p.strip() for p in partners_raw.split(",") if p.strip()]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="raft-leadership-watch")

    def start(self) -> None:
        if not self._self_addr:
            logger.warning("Raft coordinator requested but ARSONIST_RAFT_SELF empty; staying single leader")
            with self._lock:
                self._is_leader = True
            return
        try:
            from pysyncobj import SyncObj  # type: ignore
        except ImportError:
            logger.error("pysyncobj not installed; cannot start Raft coordinator")
            return

        partners = [p for p in self._partners if p != self._self_addr]
        self._sync = SyncObj(self._self_addr, partners)
        self._thread.start()

    def _leader_from_sync(self) -> bool:
        obj = self._sync
        if obj is None:
            return True
        try:
            if hasattr(obj, "isLeader") and callable(obj.isLeader):
                return bool(obj.isLeader())
        except Exception:
            pass
        try:
            st = obj.getStatus()
            if isinstance(st, dict) and "leader" in st:
                return st.get("leader") == self._self_addr or st.get("leader") is True
        except Exception:
            pass
        try:
            return bool(obj._isLeader())  # type: ignore[attr-defined]
        except Exception:
            return True

    def _loop(self) -> None:
        while not self._stop.is_set():
            leader = self._leader_from_sync()
            with self._lock:
                self._is_leader = leader
            if self._stop.wait(1.0):
                break


def build_coordinator() -> Coordinator:
    mode = os.getenv("ARSONIST_COORDINATOR_MODE", "single").lower()
    dsn = os.getenv("ARSONIST_DATABASE_URL", "")
    if mode in ("postgres", "pg", "postgres_lock", "sql"):
        if not dsn.startswith("postgresql"):
            raise ValueError("ARSONIST_COORDINATOR_MODE=postgres requires ARSONIST_DATABASE_URL")
        coord = PostgresLockCoordinator(dsn)
        return coord
    if mode in ("raft",):
        coord = RaftCoordinator()
        return coord
    return SingleCoordinator()
