from __future__ import annotations

import json
import os
import sqlite3
import threading
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("replication.model")


class ReplicationTier(str, Enum):
    hot = "hot"
    warm = "warm"
    cold = "cold"


class ReplicationState(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"


class ModelReplica(BaseModel):
    model_id: str
    region_id: str
    tier: ReplicationTier = ReplicationTier.warm
    state: ReplicationState = ReplicationState.pending
    size_gb: float = 0.0
    request_frequency: float = 0.0
    last_accessed: float = 0.0
    replicated_at: float = 0.0
    version: int = 1


class ReplicationPolicy(BaseModel):
    model_id: str
    min_replicas: int = 1
    max_replicas: int = 5
    tier: ReplicationTier = ReplicationTier.warm
    frequency_threshold: float = 10.0
    latency_threshold_ms: float = 200.0


class ModelReplicationManager:
    """Manages automatic model replication across regions."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv("ARSONIST_REPLICATION_DB", "data/model_replication.db")
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._shared_conn: sqlite3.Connection | None = None
        self._init_db()
        self._policies: Dict[str, ReplicationPolicy] = {}
        self._replication_count = 0
        self._failure_count = 0

    def _conn(self) -> sqlite3.Connection:
        if self.db_path == ":memory:":
            if self._shared_conn is None:
                self._shared_conn = sqlite3.connect(":memory:", check_same_thread=False)
                self._shared_conn.row_factory = sqlite3.Row
            return self._shared_conn
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS model_replicas (
                    model_id TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (model_id, region_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS replication_events (
                    idx INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                )
                """
            )
        if self.db_path != ":memory:":
            conn.close()

    def add_replica(self, replica: ModelReplica) -> ModelReplica:
        ts = now_ts()
        if replica.replicated_at == 0.0:
            replica.replicated_at = ts
        payload = json.dumps(replica.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO model_replicas(model_id, region_id, payload, updated_at) VALUES (?,?,?,?)",
                    (replica.model_id, replica.region_id, payload, ts),
                )
            if self.db_path != ":memory:":
                conn.close()
            self._replication_count += 1
        self._emit_event("replica_added", {"model_id": replica.model_id, "region_id": replica.region_id})
        return replica

    def get_replica(self, model_id: str, region_id: str) -> Optional[ModelReplica]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT payload FROM model_replicas WHERE model_id = ? AND region_id = ?",
                    (model_id, region_id),
                ).fetchone()
                if not row:
                    return None
                return ModelReplica(**json.loads(row["payload"]))
            finally:
                if self.db_path != ":memory:":
                    conn.close()

    def list_replicas(self, model_id: Optional[str] = None, region_id: Optional[str] = None) -> List[ModelReplica]:
        with self._lock:
            conn = self._conn()
            try:
                if model_id and region_id:
                    rows = conn.execute(
                        "SELECT payload FROM model_replicas WHERE model_id = ? AND region_id = ?",
                        (model_id, region_id),
                    ).fetchall()
                elif model_id:
                    rows = conn.execute(
                        "SELECT payload FROM model_replicas WHERE model_id = ?", (model_id,)
                    ).fetchall()
                elif region_id:
                    rows = conn.execute(
                        "SELECT payload FROM model_replicas WHERE region_id = ?", (region_id,)
                    ).fetchall()
                else:
                    rows = conn.execute("SELECT payload FROM model_replicas").fetchall()
                return [ModelReplica(**json.loads(r["payload"])) for r in rows]
            finally:
                if self.db_path != ":memory:":
                    conn.close()

    def regions_for_model(self, model_id: str) -> List[str]:
        replicas = self.list_replicas(model_id=model_id)
        return [r.region_id for r in replicas if r.state == ReplicationState.completed]

    def update_replica_state(self, model_id: str, region_id: str, state: ReplicationState) -> Optional[ModelReplica]:
        replica = self.get_replica(model_id, region_id)
        if not replica:
            return None
        replica.state = state
        ts = now_ts()
        payload = json.dumps(replica.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE model_replicas SET payload = ?, updated_at = ? WHERE model_id = ? AND region_id = ?",
                    (payload, ts, model_id, region_id),
                )
            if self.db_path != ":memory:":
                conn.close()
        if state == ReplicationState.failed:
            self._failure_count += 1
        self._emit_event("replica_state_changed", {
            "model_id": model_id, "region_id": region_id, "state": state.value
        })
        return replica

    def remove_replica(self, model_id: str, region_id: str) -> None:
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "DELETE FROM model_replicas WHERE model_id = ? AND region_id = ?",
                    (model_id, region_id),
                )
            if self.db_path != ":memory:":
                conn.close()
        self._emit_event("replica_removed", {"model_id": model_id, "region_id": region_id})

    def record_access(self, model_id: str, region_id: str) -> None:
        replica = self.get_replica(model_id, region_id)
        if not replica:
            return
        replica.request_frequency += 1.0
        replica.last_accessed = now_ts()
        ts = now_ts()
        payload = json.dumps(replica.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE model_replicas SET payload = ?, updated_at = ? WHERE model_id = ? AND region_id = ?",
                    (payload, ts, model_id, region_id),
                )
            if self.db_path != ":memory:":
                conn.close()

    def set_policy(self, policy: ReplicationPolicy) -> None:
        with self._lock:
            self._policies[policy.model_id] = policy

    def evaluate_replication_needs(self, model_id: str) -> List[str]:
        policy = self._policies.get(model_id)
        if not policy:
            return []
        replicas = self.list_replicas(model_id=model_id)
        completed = [r for r in replicas if r.state == ReplicationState.completed]
        if len(completed) >= policy.max_replicas:
            return []
        current_regions = {r.region_id for r in completed}
        needed = max(0, policy.min_replicas - len(completed))
        return [f"need_{i}" for i in range(needed)]

    def _emit_event(self, event_type: str, details: Dict[str, Any]) -> None:
        payload = json.dumps({"ts": now_ts(), "event": event_type, "details": details})
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute("INSERT INTO replication_events(payload) VALUES (?)", (payload,))
            if self.db_path != ":memory:":
                conn.close()

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT payload FROM replication_events ORDER BY idx DESC LIMIT ?", (limit,)
                ).fetchall()
                return [json.loads(r["payload"]) for r in rows]
            finally:
                if self.db_path != ":memory:":
                    conn.close()

    def metrics(self) -> Dict[str, Any]:
        replicas = self.list_replicas()
        by_state: Dict[str, int] = {}
        by_tier: Dict[str, int] = {}
        for r in replicas:
            by_state[r.state.value] = by_state.get(r.state.value, 0) + 1
            by_tier[r.tier.value] = by_tier.get(r.tier.value, 0) + 1
        return {
            "ts": now_ts(),
            "total_replicas": len(replicas),
            "by_state": by_state,
            "by_tier": by_tier,
            "replication_count": self._replication_count,
            "failure_count": self._failure_count,
            "active_policies": len(self._policies),
        }
