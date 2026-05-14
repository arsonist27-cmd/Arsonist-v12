from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from federation.federation_models import ClusterRecord, FailoverEvent, GlobalJobRecord, GlobalJobStatus
from shared.utils import now_ts


class FederationRegistry:
    """Persistent cluster registry + global job queue (SQLite; survives restart)."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv("FEDERATION_DB_PATH", "data/federation.db")
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
                CREATE TABLE IF NOT EXISTS clusters (
                    cluster_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS global_jobs (
                    job_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS federation_kv (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS federation_events (
                    idx INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                )
                """
            )
        conn.close()

    def upsert_cluster(self, cluster: ClusterRecord) -> None:
        payload = json.dumps(cluster.model_dump(mode="json"))
        ts = now_ts()
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO clusters(cluster_id, payload, updated_at) VALUES (?,?,?)",
                    (cluster.cluster_id, payload, ts),
                )
            conn.close()

    def get_cluster(self, cluster_id: str) -> Optional[ClusterRecord]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT payload FROM clusters WHERE cluster_id = ?", (cluster_id,)).fetchone()
                if not row:
                    return None
                return ClusterRecord(**json.loads(row["payload"]))
            finally:
                conn.close()

    def list_clusters(self) -> List[ClusterRecord]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT payload FROM clusters ORDER BY cluster_id ASC").fetchall()
                return [ClusterRecord(**json.loads(r["payload"])) for r in rows]
            finally:
                conn.close()

    def delete_cluster(self, cluster_id: str) -> None:
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute("DELETE FROM clusters WHERE cluster_id = ?", (cluster_id,))
            conn.close()

    def save_global_job(self, job: GlobalJobRecord) -> None:
        payload = json.dumps(job.model_dump(mode="json"))
        ts = now_ts()
        job.updated_at = ts
        payload = json.dumps(job.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO global_jobs(job_id, payload, updated_at) VALUES (?,?,?)",
                    (job.id, payload, ts),
                )
            conn.close()

    def get_global_job(self, job_id: str) -> Optional[GlobalJobRecord]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT payload FROM global_jobs WHERE job_id = ?", (job_id,)).fetchone()
                if not row:
                    return None
                return GlobalJobRecord(**json.loads(row["payload"]))
            finally:
                conn.close()

    def list_global_jobs(self, status: Optional[str] = None) -> List[GlobalJobRecord]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT payload FROM global_jobs ORDER BY updated_at DESC").fetchall()
                jobs = [GlobalJobRecord(**json.loads(r["payload"])) for r in rows]
                if status:
                    jobs = [j for j in jobs if j.status.value == status]
                return jobs
            finally:
                conn.close()

    def increment_metric(self, key: str, delta: int = 1) -> int:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT v FROM federation_kv WHERE k = ?", (key,)).fetchone()
                cur = int(row["v"]) if row else 0
                nxt = cur + delta
                conn.execute(
                    "INSERT OR REPLACE INTO federation_kv(k, v) VALUES (?, ?)",
                    (key, str(nxt)),
                )
                conn.commit()
                return nxt
            finally:
                conn.close()

    def get_metric(self, key: str) -> int:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT v FROM federation_kv WHERE k = ?", (key,)).fetchone()
                return int(row["v"]) if row else 0
            finally:
                conn.close()

    def emit_event(self, event_type: str, details: Dict[str, Any]) -> None:
        payload = json.dumps({"ts": now_ts(), "event": event_type, "details": details})
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute("INSERT INTO federation_events(payload) VALUES (?)", (payload,))
            conn.close()

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT payload FROM federation_events ORDER BY idx DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [json.loads(r["payload"]) for r in rows]
            finally:
                conn.close()
