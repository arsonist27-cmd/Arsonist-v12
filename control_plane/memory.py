from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from shared.models import EventRecord, JobRecord, NodeState
from shared.utils import now_ts

try:
    from psycopg.types.json import Json
    from psycopg_pool import ConnectionPool
except ImportError:
    ConnectionPool = None  # type: ignore[misc,assignment]
    Json = None  # type: ignore[misc,assignment]


class ClusterMemory:
    def __init__(self, db_path: str = "control_plane/arsonist.db", database_url: str | None = None) -> None:
        self._lock = threading.RLock()
        self.nodes: Dict[str, NodeState] = {}
        self.jobs: Dict[str, JobRecord] = {}
        self.queue: Deque[str] = deque()
        self.events: List[EventRecord] = []
        self.db_path = db_path
        self.database_url = (database_url or os.getenv("ARSONIST_DATABASE_URL", "")).strip()
        self._pool: ConnectionPool | None = None

        if self.database_url.startswith("postgresql"):
            if ConnectionPool is None:
                raise ImportError("psycopg_pool is required for PostgreSQL backend; pip install 'psycopg[binary]'")
            self._pool = ConnectionPool(conninfo=self.database_url, min_size=1, max_size=12)
            self._init_postgres()
            self._restore_postgres()
        else:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._init_sqlite()
            self._restore_sqlite()

    # --- SQLite -----------------------------------------------------------------
    def _conn_sqlite(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_sqlite(self) -> None:
        conn = self._conn_sqlite()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (
                    idx INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    idx INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS node_secrets (
                    node_id TEXT PRIMARY KEY,
                    node_secret TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS registry_kv (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
        conn.close()

    def _restore_sqlite(self) -> None:
        conn = self._conn_sqlite()
        try:
            node_rows = conn.execute("SELECT payload FROM nodes").fetchall()
            for row in node_rows:
                node = NodeState(**json.loads(row["payload"]))
                self.nodes[node.node_id] = node

            job_rows = conn.execute("SELECT payload FROM jobs").fetchall()
            for row in job_rows:
                job = JobRecord(**json.loads(row["payload"]))
                self.jobs[job.id] = job

            queue_rows = conn.execute("SELECT job_id FROM queue ORDER BY idx ASC").fetchall()
            for row in queue_rows:
                self.queue.append(row["job_id"])

            event_rows = conn.execute("SELECT payload FROM events ORDER BY idx ASC").fetchall()
            for row in event_rows:
                self.events.append(EventRecord(**json.loads(row["payload"])))
        finally:
            conn.close()

    def _persist_node_sqlite(self, node: NodeState) -> None:
        conn = self._conn_sqlite()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO nodes(node_id, payload) VALUES(?, ?)",
                (node.node_id, json.dumps(node.model_dump())),
            )
        conn.close()

    def _save_job_sqlite(self, job: JobRecord) -> None:
        conn = self._conn_sqlite()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO jobs(job_id, payload) VALUES(?, ?)",
                (job.id, json.dumps(job.model_dump())),
            )
        conn.close()

    def _persist_queue_sqlite(self) -> None:
        conn = self._conn_sqlite()
        with conn:
            conn.execute("DELETE FROM queue")
            conn.executemany("INSERT INTO queue(job_id) VALUES(?)", [(job_id,) for job_id in self.queue])
        conn.close()

    # --- PostgreSQL --------------------------------------------------------------
    def _init_postgres(self) -> None:
        assert self._pool is not None
        ddl = """
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            payload JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            payload JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS queue (
            queue_id BIGSERIAL PRIMARY KEY,
            job_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            event_id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS node_secrets (
            node_id TEXT PRIMARY KEY,
            node_secret TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS registry_kv (
            k TEXT PRIMARY KEY,
            v JSONB NOT NULL,
            updated_at DOUBLE PRECISION NOT NULL
        );
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def _restore_postgres(self) -> None:
        assert self._pool is not None
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT payload FROM nodes")
                for (payload,) in cur.fetchall():
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    node = NodeState(**dict(payload))
                    self.nodes[node.node_id] = node

                cur.execute("SELECT payload FROM jobs")
                for (payload,) in cur.fetchall():
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    job = JobRecord(**dict(payload))
                    self.jobs[job.id] = job

                cur.execute("SELECT job_id FROM queue ORDER BY queue_id ASC")
                for (job_id,) in cur.fetchall():
                    self.queue.append(job_id)

                cur.execute("SELECT payload FROM events ORDER BY event_id ASC")
                for (payload,) in cur.fetchall():
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    self.events.append(EventRecord(**dict(payload)))

    def _persist_node_pg(self, node: NodeState) -> None:
        assert self._pool is not None and Json is not None
        payload = Json(node.model_dump())
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO nodes(node_id, payload) VALUES (%s, %s) ON CONFLICT (node_id) DO UPDATE SET payload = EXCLUDED.payload",
                    (node.node_id, payload),
                )
            conn.commit()

    def _save_job_pg(self, job: JobRecord) -> None:
        assert self._pool is not None and Json is not None
        payload = Json(job.model_dump())
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs(job_id, payload) VALUES (%s, %s) ON CONFLICT (job_id) DO UPDATE SET payload = EXCLUDED.payload",
                    (job.id, payload),
                )
            conn.commit()

    def _append_queue_pg(self, job_id: str) -> None:
        assert self._pool is not None
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO queue (job_id)
                    SELECT %s WHERE NOT EXISTS (
                        SELECT 1 FROM queue WHERE job_id = %s
                    )
                    """,
                    (job_id, job_id),
                )
            conn.commit()

    def _emit_pg(self, record: EventRecord) -> None:
        assert self._pool is not None and Json is not None
        payload = Json(record.model_dump())
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO events(payload) VALUES (%s)", (payload,))
            conn.commit()

    def save_node_secret(self, node_id: str, node_secret: str) -> None:
        if self._pool:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO node_secrets(node_id, node_secret) VALUES (%s, %s) ON CONFLICT (node_id) DO UPDATE SET node_secret = EXCLUDED.node_secret",
                        (node_id, node_secret),
                    )
                conn.commit()
            return
        conn = self._conn_sqlite()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO node_secrets(node_id, node_secret) VALUES(?, ?)",
                (node_id, node_secret),
            )
        conn.close()

    def get_node_secret(self, node_id: str) -> str | None:
        if self._pool:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT node_secret FROM node_secrets WHERE node_id = %s", (node_id,))
                    row = cur.fetchone()
                    return row[0] if row else None
        conn = self._conn_sqlite()
        try:
            row = conn.execute("SELECT node_secret FROM node_secrets WHERE node_id = ?", (node_id,)).fetchone()
            return row["node_secret"] if row else None
        finally:
            conn.close()

    def save_job(self, job: JobRecord) -> None:
        self.jobs[job.id] = job
        if self._pool:
            self._save_job_pg(job)
        else:
            self._save_job_sqlite(job)

    def persist_queue(self) -> None:
        if self._pool:
            return
        self._persist_queue_sqlite()

    def queue_snapshot(self) -> List[str]:
        if not self._pool:
            return list(self.queue)
        assert self._pool is not None
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT job_id FROM queue ORDER BY queue_id ASC")
                return [row[0] for row in cur.fetchall()]

    def ensure_job_queued(self, job_id: str) -> None:
        if job_id not in self.queue:
            self.queue.append(job_id)
        if self._pool:
            self._append_queue_pg(job_id)
        else:
            self._persist_queue_sqlite()

    def enqueue_job(self, job: JobRecord) -> None:
        self.jobs[job.id] = job
        if job.id not in self.queue:
            self.queue.append(job.id)
        self.save_job(job)
        if self._pool:
            self._append_queue_pg(job.id)
        else:
            self._persist_queue_sqlite()

    def pop_next_job(self) -> JobRecord | None:
        if not self._pool:
            if not self.queue:
                return None
            job_id = self.queue.popleft()
            self._persist_queue_sqlite()
            return self.jobs.get(job_id)

        assert self._pool is not None
        job_id: str | None = None
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM queue
                    WHERE queue_id = (
                        SELECT queue_id FROM queue ORDER BY queue_id ASC LIMIT 1 FOR UPDATE SKIP LOCKED
                    )
                    RETURNING job_id
                    """
                )
                row = cur.fetchone()
                job_id = row[0] if row else None
            conn.commit()
        if not job_id:
            return None
        job = self.jobs.get(job_id)
        if job:
            try:
                self.queue.remove(job_id)
            except ValueError:
                pass
            return job
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT payload FROM jobs WHERE job_id = %s", (job_id,))
                r = cur.fetchone()
                if not r:
                    return None
                payload = r[0]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                job = JobRecord(**dict(payload))
                self.jobs[job.id] = job
        try:
            self.queue.remove(job_id)
        except ValueError:
            pass
        return job

    def emit(self, level: str, event: str, details: dict) -> None:
        record = EventRecord(ts=now_ts(), level=level, event=event, details=details)
        with self._lock:
            self.events.append(record)
        if self._pool:
            self._emit_pg(record)
        else:
            conn = self._conn_sqlite()
            with conn:
                conn.execute("INSERT INTO events(payload) VALUES(?)", (json.dumps(record.model_dump()),))
            conn.close()

    def registry_put(self, key: str, value: dict) -> None:
        ts = now_ts()
        blob = json.dumps(value)
        if self._pool:
            assert Json is not None
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO registry_kv(k, v, updated_at) VALUES (%s, %s, %s) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at",
                        (key, Json(value), ts),
                    )
                conn.commit()
            return
        conn = self._conn_sqlite()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO registry_kv(k, v, updated_at) VALUES(?, ?, ?)",
                (key, blob, ts),
            )
        conn.close()

    def registry_get(self, key: str) -> dict | None:
        if self._pool:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT v FROM registry_kv WHERE k = %s", (key,))
                    row = cur.fetchone()
                    if not row:
                        return None
                    v = row[0]
                    if isinstance(v, str):
                        return json.loads(v)
                    return dict(v)
        conn = self._conn_sqlite()
        try:
            row = conn.execute("SELECT v FROM registry_kv WHERE k = ?", (key,)).fetchone()
            if not row:
                return None
            return json.loads(row["v"])
        finally:
            conn.close()

    def add_node(self, node: NodeState) -> None:
        with self._lock:
            if node.node_id in self.nodes:
                existing = self.nodes[node.node_id]
                existing.host = node.host
                existing.port = node.port
                existing.node_type = node.node_type
                existing.has_gpu = node.has_gpu
                existing.healthy = True
            else:
                self.nodes[node.node_id] = node
            self.nodes[node.node_id].last_seen = now_ts()
            snapshot = self.nodes[node.node_id]
            if self._pool:
                self._persist_node_pg(snapshot)
            else:
                self._persist_node_sqlite(snapshot)
        self.emit("info", "node_joined", {"node_id": node.node_id})

    def remove_node(self, node_id: str) -> Optional[NodeState]:
        with self._lock:
            node = self.nodes.pop(node_id, None)
            if self._pool:
                with self._pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM nodes WHERE node_id = %s", (node_id,))
                    conn.commit()
            else:
                conn = self._conn_sqlite()
                with conn:
                    conn.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
                conn.close()
        if node:
            self.emit("warning", "node_removed", {"node_id": node_id})
        return node
