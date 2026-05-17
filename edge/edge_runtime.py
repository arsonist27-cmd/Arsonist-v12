from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("edge.runtime")


class EdgeNodeState(str, Enum):
    online = "online"
    offline = "offline"
    syncing = "syncing"
    degraded = "degraded"


class EdgeRuntime:
    """Lightweight edge inference runtime with offline support and local caching.

    Supports intermittent connectivity, local inference caching,
    temporary offline operation, and regional synchronization.
    """

    def __init__(
        self,
        node_id: str,
        region_id: str,
        db_path: str | None = None,
        cache_max_entries: int = 1000,
        sync_interval: float = 30.0,
        on_sync: Optional[Callable[[str, List[Dict[str, Any]]], None]] = None,
    ) -> None:
        self.node_id = node_id
        self.region_id = region_id
        self.cache_max_entries = cache_max_entries
        self.sync_interval = sync_interval
        self._on_sync = on_sync
        self.db_path = db_path or os.getenv(
            "ARSONIST_EDGE_RUNTIME_DB",
            f"data/edge_runtime_{node_id}.db",
        )
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._shared_conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        self._state = EdgeNodeState.online
        self._connected = True
        self._local_cache: Dict[str, Dict[str, Any]] = {}
        self._pending_results: List[Dict[str, Any]] = []
        self._requests_served = 0
        self._cache_hits = 0
        self._cache_misses = 0
        self._offline_requests = 0
        self._stop = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None
        self._init_db()

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
                CREATE TABLE IF NOT EXISTS edge_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    accessed_at REAL NOT NULL,
                    access_count INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS edge_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    synced INTEGER DEFAULT 0
                )
                """
            )
        if self.db_path != ":memory:":
            conn.close()

    def start_sync(self) -> None:
        if self._sync_thread and self._sync_thread.is_alive():
            return
        self._stop.clear()
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True, name=f"edge-sync-{self.node_id}")
        self._sync_thread.start()
        logger.info("Edge runtime %s sync started", self.node_id)

    def stop_sync(self) -> None:
        self._stop.set()
        if self._sync_thread:
            self._sync_thread.join(timeout=self.sync_interval + 2)

    def _sync_loop(self) -> None:
        while not self._stop.is_set():
            if self._connected:
                try:
                    self._flush_outbox()
                except Exception:
                    logger.exception("Edge sync error")
            self._stop.wait(self.sync_interval)

    def handle_request(self, request_key: str, request_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self._requests_served += 1
        cached = self._get_cache(request_key)
        if cached:
            self._cache_hits += 1
            return cached

        self._cache_misses += 1
        if not self._connected:
            self._offline_requests += 1
            return None

        return None

    def store_result(self, request_key: str, result: Dict[str, Any]) -> None:
        self._put_cache(request_key, result)
        self._enqueue_outbox({
            "type": "inference_result",
            "node_id": self.node_id,
            "request_key": request_key,
            "ts": now_ts(),
        })

    def set_connected(self, connected: bool) -> None:
        old = self._connected
        self._connected = connected
        if connected and not old:
            self._state = EdgeNodeState.syncing
            logger.info("Edge %s reconnected, syncing", self.node_id)
        elif not connected and old:
            self._state = EdgeNodeState.offline
            logger.info("Edge %s went offline", self.node_id)

    def _get_cache(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if key in self._local_cache:
                entry = self._local_cache[key]
                entry["accessed_at"] = now_ts()
                entry["access_count"] = entry.get("access_count", 0) + 1
                return entry.get("data")
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT payload FROM edge_cache WHERE cache_key = ?", (key,)
            ).fetchone()
            if row:
                data = json.loads(row["payload"])
                conn.execute(
                    "UPDATE edge_cache SET accessed_at = ?, access_count = access_count + 1 WHERE cache_key = ?",
                    (now_ts(), key),
                )
                conn.commit()
                with self._lock:
                    self._local_cache[key] = {"data": data, "accessed_at": now_ts(), "access_count": 1}
                return data
        finally:
            if self.db_path != ":memory:":
                conn.close()
        return None

    def _put_cache(self, key: str, data: Dict[str, Any]) -> None:
        ts = now_ts()
        payload = json.dumps(data)
        with self._lock:
            self._local_cache[key] = {"data": data, "accessed_at": ts, "access_count": 0}
            if len(self._local_cache) > self.cache_max_entries:
                oldest = min(self._local_cache, key=lambda k: self._local_cache[k]["accessed_at"])
                del self._local_cache[oldest]
        conn = self._conn()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO edge_cache(cache_key, payload, created_at, accessed_at) VALUES (?,?,?,?)",
                (key, payload, ts, ts),
            )
        if self.db_path != ":memory:":
            conn.close()

    def _enqueue_outbox(self, payload: Dict[str, Any]) -> None:
        conn = self._conn()
        with conn:
            conn.execute(
                "INSERT INTO edge_outbox(payload, created_at) VALUES (?,?)",
                (json.dumps(payload), now_ts()),
            )
        if self.db_path != ":memory:":
            conn.close()

    def _flush_outbox(self) -> None:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT id, payload FROM edge_outbox WHERE synced = 0 ORDER BY id ASC LIMIT 100"
            ).fetchall()
            if not rows:
                if self._state == EdgeNodeState.syncing:
                    self._state = EdgeNodeState.online
                return
            entries = [{"id": r["id"], "payload": json.loads(r["payload"])} for r in rows]
            if self._on_sync:
                self._on_sync(self.node_id, [e["payload"] for e in entries])
            for entry in entries:
                conn.execute("UPDATE edge_outbox SET synced = 1 WHERE id = ?", (entry["id"],))
            conn.commit()
            if self._state == EdgeNodeState.syncing:
                remaining = conn.execute("SELECT COUNT(*) FROM edge_outbox WHERE synced = 0").fetchone()
                if remaining and remaining[0] == 0:
                    self._state = EdgeNodeState.online
        finally:
            if self.db_path != ":memory:":
                conn.close()

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "node_id": self.node_id,
                "region_id": self.region_id,
                "state": self._state.value,
                "connected": self._connected,
                "requests_served": self._requests_served,
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
                "cache_hit_rate": round(
                    self._cache_hits / (self._cache_hits + self._cache_misses), 4
                ) if (self._cache_hits + self._cache_misses) > 0 else 0.0,
                "offline_requests": self._offline_requests,
                "local_cache_size": len(self._local_cache),
            }
