from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

from shared.utils import now_ts


class EdgeNodeController:
    """Low-resource edge profile: local persistence + deferred upstream sync."""

    def __init__(self, node_id: str, db_path: str | None = None) -> None:
        self.node_id = node_id
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_EDGE_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), f"edge_{node_id}.db"),
        )
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS edge_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    sent INTEGER NOT NULL DEFAULT 0
                )
                """
            )
        conn.close()

    def enqueue_metric(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            with conn:
                conn.execute(
                    "INSERT INTO edge_outbox(payload, created_at) VALUES(?,?)",
                    (json.dumps(payload), now_ts()),
                )
            conn.close()

    def pending(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                rows = conn.execute(
                    "SELECT id, payload FROM edge_outbox WHERE sent = 0 ORDER BY id ASC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [{"id": int(r[0]), "payload": json.loads(r[1])} for r in rows]
            finally:
                conn.close()

    def mark_sent(self, row_id: int) -> None:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            with conn:
                conn.execute("UPDATE edge_outbox SET sent = 1 WHERE id = ?", (row_id,))
            conn.close()
