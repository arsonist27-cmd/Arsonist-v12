from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts


class ReplicatedJobState(BaseModel):
    job_id: str
    state: str = "queued"  # queued|assigned|running|migrated|completed|failed|orphaned
    owner_cluster_id: str = ""
    claim_token: str = Field(default_factory=lambda: str(uuid.uuid4()))
    updated_at: float = 0.0
    payload_digest: str = ""


class ReplicatedQueue:
    """
    Tracks cross-cluster job replicas with ownership + lease to prevent duplicate execution.
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_MESH_QUEUE_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), "mesh_queue.db"),
        )
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
                CREATE TABLE IF NOT EXISTS rep_jobs (
                    job_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    lease_until REAL NOT NULL DEFAULT 0
                )
                """
            )
        conn.close()

    def upsert(self, state: ReplicatedJobState) -> None:
        state.updated_at = now_ts()
        blob = json.dumps(state.model_dump())
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT INTO rep_jobs(job_id, payload, lease_until) VALUES(?,?,?) "
                    "ON CONFLICT(job_id) DO UPDATE SET payload = excluded.payload, lease_until = excluded.lease_until",
                    (state.job_id, blob, state.updated_at + float(os.getenv("ARSONIST_MESH_QUEUE_LEASE_SEC", "45"))),
                )
            conn.close()

    def get(self, job_id: str) -> Optional[ReplicatedJobState]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT payload FROM rep_jobs WHERE job_id = ?", (job_id,)).fetchone()
                if not row:
                    return None
                return ReplicatedJobState(**json.loads(row["payload"]))
            finally:
                conn.close()

    def try_claim(self, job_id: str, cluster_id: str) -> Optional[ReplicatedJobState]:
        """Grant ownership if unowned or lease expired."""
        st = self.get(job_id)
        if not st:
            return None
        lease_until = 0.0
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT lease_until FROM rep_jobs WHERE job_id = ?", (job_id,)).fetchone()
                lease_until = float(row["lease_until"]) if row else 0.0
            finally:
                conn.close()
        t = now_ts()
        if st.owner_cluster_id and st.owner_cluster_id != cluster_id and lease_until > t:
            return None
        st.owner_cluster_id = cluster_id
        st.state = "assigned"
        self.upsert(st)
        return st

    def reconcile_orphan(self, job_id: str) -> None:
        st = self.get(job_id)
        if not st:
            return
        st.state = "orphaned"
        self.upsert(st)

    def list_states(self, limit: int = 200) -> List[ReplicatedJobState]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT payload FROM rep_jobs ORDER BY job_id ASC LIMIT ?", (limit,)).fetchall()
                return [ReplicatedJobState(**json.loads(r["payload"])) for r in rows]
            finally:
                conn.close()
