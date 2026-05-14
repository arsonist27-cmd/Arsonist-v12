from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

from shared.utils import now_ts


class LeaderElection:
    """SQLite-backed lease election (best-effort, not Paxos)."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_MESH_CONSENSUS_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), "mesh_consensus.db"),
        )
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leader_lease (
                    scope TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
        conn.close()

    def try_acquire(self, scope: str, holder: str, ttl_sec: float) -> bool:
        t = now_ts()
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                row = conn.execute("SELECT holder, expires_at FROM leader_lease WHERE scope = ?", (scope,)).fetchone()
                if row and float(row[1]) > t and row[0] != holder:
                    return False
                conn.execute(
                    "INSERT INTO leader_lease(scope, holder, expires_at) VALUES(?,?,?) "
                    "ON CONFLICT(scope) DO UPDATE SET holder=excluded.holder, expires_at=excluded.expires_at",
                    (scope, holder, t + ttl_sec),
                )
                conn.commit()
                return True
            finally:
                conn.close()

    def renew(self, scope: str, holder: str, ttl_sec: float) -> None:
        if self.try_acquire(scope, holder, ttl_sec):
            return
