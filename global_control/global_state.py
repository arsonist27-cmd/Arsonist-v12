from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("global_control.state")


class GlobalState:
    """Distributed global state store (SQLite-backed, single-node primary)."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv("ARSONIST_GLOBAL_STATE_DB", "data/global_state.db")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS global_kv (
                    namespace TEXT NOT NULL,
                    k TEXT NOT NULL,
                    v TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (namespace, k)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state_changelog (
                    idx INTEGER PRIMARY KEY AUTOINCREMENT,
                    namespace TEXT NOT NULL,
                    k TEXT NOT NULL,
                    old_version INTEGER,
                    new_version INTEGER NOT NULL,
                    ts REAL NOT NULL
                )
                """
            )
        conn.close()

    def put(self, namespace: str, key: str, value: Any) -> int:
        serialized = json.dumps(value) if not isinstance(value, str) else value
        ts = now_ts()
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT version FROM global_kv WHERE namespace = ? AND k = ?",
                    (namespace, key),
                ).fetchone()
                old_ver = int(row["version"]) if row else 0
                new_ver = old_ver + 1
                conn.execute(
                    "INSERT OR REPLACE INTO global_kv(namespace, k, v, version, updated_at) VALUES (?,?,?,?,?)",
                    (namespace, key, serialized, new_ver, ts),
                )
                conn.execute(
                    "INSERT INTO state_changelog(namespace, k, old_version, new_version, ts) VALUES (?,?,?,?,?)",
                    (namespace, key, old_ver if old_ver else None, new_ver, ts),
                )
                conn.commit()
                return new_ver
            finally:
                conn.close()

    def get(self, namespace: str, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT v, version, updated_at FROM global_kv WHERE namespace = ? AND k = ?",
                    (namespace, key),
                ).fetchone()
                if not row:
                    return None
                try:
                    val = json.loads(row["v"])
                except (json.JSONDecodeError, TypeError):
                    val = row["v"]
                return {"value": val, "version": int(row["version"]), "updated_at": float(row["updated_at"])}
            finally:
                conn.close()

    def delete(self, namespace: str, key: str) -> bool:
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    "DELETE FROM global_kv WHERE namespace = ? AND k = ?",
                    (namespace, key),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                conn.close()

    def list_keys(self, namespace: str) -> List[str]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT k FROM global_kv WHERE namespace = ? ORDER BY k",
                    (namespace,),
                ).fetchall()
                return [r["k"] for r in rows]
            finally:
                conn.close()

    def list_namespaces(self) -> List[str]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT DISTINCT namespace FROM global_kv ORDER BY namespace"
                ).fetchall()
                return [r["namespace"] for r in rows]
            finally:
                conn.close()

    def changelog(self, namespace: Optional[str] = None, since_version: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                if namespace:
                    rows = conn.execute(
                        "SELECT * FROM state_changelog WHERE namespace = ? AND new_version > ? ORDER BY idx DESC LIMIT ?",
                        (namespace, since_version, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM state_changelog WHERE new_version > ? ORDER BY idx DESC LIMIT ?",
                        (since_version, limit),
                    ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def snapshot(self, namespace: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            conn = self._conn()
            try:
                if namespace:
                    rows = conn.execute(
                        "SELECT namespace, k, v, version FROM global_kv WHERE namespace = ?",
                        (namespace,),
                    ).fetchall()
                else:
                    rows = conn.execute("SELECT namespace, k, v, version FROM global_kv").fetchall()
                data: Dict[str, Dict[str, Any]] = {}
                for r in rows:
                    ns = r["namespace"]
                    if ns not in data:
                        data[ns] = {}
                    try:
                        val = json.loads(r["v"])
                    except (json.JSONDecodeError, TypeError):
                        val = r["v"]
                    data[ns][r["k"]] = {"value": val, "version": int(r["version"])}
                return {"ts": now_ts(), "state": data}
            finally:
                conn.close()
