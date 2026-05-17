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

logger = setup_logging("regions.registry")


class RegionStatus(str, Enum):
    active = "active"
    degraded = "degraded"
    draining = "draining"
    offline = "offline"


class RegionType(str, Enum):
    cloud = "cloud"
    edge = "edge"
    hybrid = "hybrid"


class GPUInventory(BaseModel):
    total_gpus: int = 0
    available_gpus: int = 0
    gpu_types: Dict[str, int] = Field(default_factory=dict)
    total_vram_gb: float = 0.0
    available_vram_gb: float = 0.0


class RegionRecord(BaseModel):
    region_id: str
    display_name: str = ""
    geographic_location: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    region_type: RegionType = RegionType.cloud
    status: RegionStatus = RegionStatus.active
    capacity: float = 1.0
    gpu_inventory: GPUInventory = Field(default_factory=GPUInventory)
    avg_latency_ms: float = 0.0
    workload_saturation: float = 0.0
    edge_connectivity: bool = True
    failover_priority: int = 0
    failover_target: Optional[str] = None
    endpoint_url: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)
    registered_at: float = 0.0
    last_heartbeat: float = 0.0
    updated_at: float = 0.0


class RegionRegistry:
    """Persistent registry for global regions (SQLite-backed)."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv("ARSONIST_REGION_DB_PATH", "data/regions.db")
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._shared_conn: sqlite3.Connection | None = None
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
                CREATE TABLE IF NOT EXISTS regions (
                    region_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS region_events (
                    idx INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS region_kv (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                )
                """
            )
        if self.db_path != ":memory:":
            conn.close()

    def register(self, region: RegionRecord) -> RegionRecord:
        ts = now_ts()
        if region.registered_at == 0.0:
            region.registered_at = ts
        region.last_heartbeat = ts
        region.updated_at = ts
        payload = json.dumps(region.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO regions(region_id, payload, updated_at) VALUES (?,?,?)",
                    (region.region_id, payload, ts),
                )
            if self.db_path != ":memory:":
                conn.close()
        logger.info("Region registered: %s (%s)", region.region_id, region.geographic_location)
        self._emit_event("region_registered", {"region_id": region.region_id})
        return region

    def heartbeat(self, region_id: str, updates: Dict[str, Any] | None = None) -> Optional[RegionRecord]:
        region = self.get(region_id)
        if not region:
            return None
        ts = now_ts()
        region.last_heartbeat = ts
        region.updated_at = ts
        if updates:
            for k, v in updates.items():
                if hasattr(region, k):
                    setattr(region, k, v)
        payload = json.dumps(region.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE regions SET payload = ?, updated_at = ? WHERE region_id = ?",
                    (payload, ts, region_id),
                )
            if self.db_path != ":memory:":
                conn.close()
        return region

    def get(self, region_id: str) -> Optional[RegionRecord]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT payload FROM regions WHERE region_id = ?", (region_id,)).fetchone()
                if not row:
                    return None
                return RegionRecord(**json.loads(row["payload"]))
            finally:
                if self.db_path != ":memory:":
                    conn.close()

    def list_regions(self, status: Optional[RegionStatus] = None, region_type: Optional[RegionType] = None) -> List[RegionRecord]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT payload FROM regions ORDER BY region_id ASC").fetchall()
                regions = [RegionRecord(**json.loads(r["payload"])) for r in rows]
            finally:
                if self.db_path != ":memory:":
                    conn.close()
        if status:
            regions = [r for r in regions if r.status == status]
        if region_type:
            regions = [r for r in regions if r.region_type == region_type]
        return regions

    def active_regions(self) -> List[RegionRecord]:
        return [r for r in self.list_regions() if r.status in (RegionStatus.active, RegionStatus.degraded)]

    def remove(self, region_id: str) -> None:
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute("DELETE FROM regions WHERE region_id = ?", (region_id,))
            if self.db_path != ":memory:":
                conn.close()
        self._emit_event("region_removed", {"region_id": region_id})

    def update_status(self, region_id: str, status: RegionStatus) -> Optional[RegionRecord]:
        region = self.get(region_id)
        if not region:
            return None
        old = region.status
        region.status = status
        region.updated_at = now_ts()
        payload = json.dumps(region.model_dump(mode="json"))
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE regions SET payload = ?, updated_at = ? WHERE region_id = ?",
                    (payload, region.updated_at, region_id),
                )
            if self.db_path != ":memory:":
                conn.close()
        self._emit_event("region_status_changed", {"region_id": region_id, "old": old.value, "new": status.value})
        return region

    def _emit_event(self, event_type: str, details: Dict[str, Any]) -> None:
        payload = json.dumps({"ts": now_ts(), "event": event_type, "details": details})
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute("INSERT INTO region_events(payload) VALUES (?)", (payload,))
            if self.db_path != ":memory:":
                conn.close()

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT payload FROM region_events ORDER BY idx DESC LIMIT ?", (limit,)
                ).fetchall()
                return [json.loads(r["payload"]) for r in rows]
            finally:
                if self.db_path != ":memory:":
                    conn.close()
