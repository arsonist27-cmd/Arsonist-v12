from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Optional

from shared.utils import now_ts


class DistributedLock:
    """Cluster-wide mutex using expiring rows (SQLite)."""

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
                CREATE TABLE IF NOT EXISTS dist_locks (
                    name TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
        conn.close()

    def acquire(self, name: str, holder: str, ttl_sec: float) -> bool:
        t = now_ts()
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                row = conn.execute("SELECT holder, expires_at FROM dist_locks WHERE name = ?", (name,)).fetchone()
                if row and float(row[1]) > t and row[0] != holder:
                    return False
                conn.execute(
                    "INSERT INTO dist_locks(name, holder, expires_at) VALUES(?,?,?) "
                    "ON CONFLICT(name) DO UPDATE SET holder=excluded.holder, expires_at=excluded.expires_at",
                    (name, holder, t + ttl_sec),
                )
                conn.commit()
                return True
            finally:
                conn.close()

    def release(self, name: str, holder: str) -> None:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                row = conn.execute("SELECT holder FROM dist_locks WHERE name = ?", (name,)).fetchone()
                if row and row[0] == holder:
                    conn.execute("DELETE FROM dist_locks WHERE name = ?", (name,))
                    conn.commit()
            finally:
                conn.close()
