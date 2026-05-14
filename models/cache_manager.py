from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

from shared.utils import now_ts


class CacheManager:
    """Disk quota + LRU-ish eviction metadata for model artifacts."""

    def __init__(self, root: Path, quota_mb: int | None = None) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.quota_mb = quota_mb or int(os.getenv("ARSONIST_MODEL_CACHE_QUOTA_MB", "102400"))
        self._lock = threading.RLock()
        self.db_path = str(self.root / "cache_index.sqlite")
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    last_access REAL NOT NULL
                )
                """
            )
        conn.close()

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def touch(self, rel_path: str, size_bytes: int) -> None:
        ts = now_ts()
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT INTO cache_entries(path, size_bytes, last_access) VALUES(?,?,?) "
                    "ON CONFLICT(path) DO UPDATE SET size_bytes=excluded.size_bytes, last_access=excluded.last_access",
                    (rel_path, size_bytes, ts),
                )
            conn.close()

    def total_mb(self) -> float:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT SUM(size_bytes) AS s FROM cache_entries").fetchone()
                b = int(row[0] or 0)
                return b / (1024 * 1024)
            finally:
                conn.close()

    def evict_if_needed(self) -> int:
        removed = 0
        while self.total_mb() > self.quota_mb:
            with self._lock:
                conn = self._conn()
                row = conn.execute("SELECT path FROM cache_entries ORDER BY last_access ASC LIMIT 1").fetchone()
                if not row:
                    conn.close()
                    break
                rel = row[0]
                conn.execute("DELETE FROM cache_entries WHERE path = ?", (rel,))
                conn.commit()
                conn.close()
            p = self.root / rel
            if p.exists():
                p.unlink()
                removed += 1
            else:
                break
        return removed
