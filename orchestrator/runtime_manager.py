from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from orchestrator.container_runtime import ContainerRuntime
from shared.utils import now_ts


class DeploymentState(str, Enum):
    pending = "pending"
    deploying = "deploying"
    healthy = "healthy"
    degraded = "degraded"
    failed = "failed"
    rolling_back = "rolling_back"


class RuntimeManager:
    """Tracks container deployments, restart policy, and rolling operations."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_RUNTIME_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), "runtime_v11.db"),
        )
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.runtime = ContainerRuntime(os.getenv("ARSONIST_CONTAINER_RUNTIME", "docker"))
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
                CREATE TABLE IF NOT EXISTS deployments (
                    deployment_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    image TEXT NOT NULL,
                    state TEXT NOT NULL,
                    restart_count INTEGER NOT NULL DEFAULT 0,
                    desired_replicas INTEGER NOT NULL DEFAULT 1,
                    healthy_replicas INTEGER NOT NULL DEFAULT 0,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
        conn.close()

    def create_deployment(self, name: str, image: str, desired_replicas: int = 1) -> str:
        dep_id = str(uuid.uuid4())
        payload = {"name": name, "image": image}
        ts = now_ts()
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT INTO deployments(deployment_id, name, image, state, desired_replicas, healthy_replicas, payload, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (dep_id, name, image, DeploymentState.pending.value, desired_replicas, 0, json.dumps(payload), ts),
                )
            conn.close()
        return dep_id

    def set_state(self, deployment_id: str, state: DeploymentState) -> None:
        ts = now_ts()
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE deployments SET state = ?, updated_at = ? WHERE deployment_id = ?",
                    (state.value, ts, deployment_id),
                )
            conn.close()

    def bump_restart(self, deployment_id: str) -> None:
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE deployments SET restart_count = restart_count + 1, updated_at = ? WHERE deployment_id = ?",
                    (now_ts(), deployment_id),
                )
            conn.close()

    def list_deployments(self) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT * FROM deployments ORDER BY updated_at DESC").fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def graceful_shutdown_marker(self, deployment_id: str) -> None:
        self.set_state(deployment_id, DeploymentState.rolling_back)
