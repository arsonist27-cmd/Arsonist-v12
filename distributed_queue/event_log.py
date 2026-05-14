from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from mesh.mesh_protocol import MeshEventType
from shared.utils import now_ts


class MeshEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    seq: int = 0
    ts: float = 0.0
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    source_cluster_id: str = ""


class EventLog:
    """Append-only mesh event stream with replay and deduplication by event_id."""

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_MESH_EVENT_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), "mesh_events.db"),
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
                CREATE TABLE IF NOT EXISTS mesh_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    ts REAL NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    source_cluster_id TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mesh_events_ts ON mesh_events(ts)")
        conn.close()

    def append(self, event_type: MeshEventType | str, payload: Dict[str, Any], source_cluster_id: str) -> MeshEvent:
        et = event_type.value if isinstance(event_type, MeshEventType) else str(event_type)
        ev = MeshEvent(ts=now_ts(), event_type=et, payload=payload, source_cluster_id=source_cluster_id)
        with self._lock:
            conn = self._conn()
            with conn:
                cur = conn.execute(
                    "INSERT INTO mesh_events(event_id, ts, event_type, payload, source_cluster_id) VALUES(?,?,?,?,?)",
                    (ev.event_id, ev.ts, ev.event_type, json.dumps(payload), source_cluster_id),
                )
                seq = int(cur.lastrowid)
            conn.close()
        ev.seq = seq
        return ev

    def last_seq(self) -> int:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT MAX(seq) AS m FROM mesh_events").fetchone()
                return int(row["m"] or 0)
            finally:
                conn.close()

    def replay(self, since_seq: int = 0, limit: int = 500) -> List[MeshEvent]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT seq, event_id, ts, event_type, payload, source_cluster_id FROM mesh_events WHERE seq > ? ORDER BY seq ASC LIMIT ?",
                    (since_seq, limit),
                ).fetchall()
                out: List[MeshEvent] = []
                for r in rows:
                    out.append(
                        MeshEvent(
                            seq=int(r["seq"]),
                            event_id=str(r["event_id"]),
                            ts=float(r["ts"]),
                            event_type=str(r["event_type"]),
                            payload=json.loads(r["payload"]),
                            source_cluster_id=str(r["source_cluster_id"]),
                        )
                    )
                return out
            finally:
                conn.close()

    def tail(self, limit: int = 50) -> List[MeshEvent]:
        last = self.last_seq()
        if last == 0:
            return []
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT seq, event_id, ts, event_type, payload, source_cluster_id FROM mesh_events WHERE seq > ? ORDER BY seq ASC",
                    (max(0, last - limit),),
                ).fetchall()
                return [
                    MeshEvent(
                        seq=int(r["seq"]),
                        event_id=str(r["event_id"]),
                        ts=float(r["ts"]),
                        event_type=str(r["event_type"]),
                        payload=json.loads(r["payload"]),
                        source_cluster_id=str(r["source_cluster_id"]),
                    )
                    for r in rows
                ]
            finally:
                conn.close()

    def merge_events(self, events: List[Dict[str, Any]]) -> int:
        """Idempotent insert by event_id; returns inserted count."""
        inserted = 0
        with self._lock:
            conn = self._conn()
            try:
                for e in events:
                    event_id = str(e.get("event_id") or "")
                    if not event_id:
                        continue
                    try:
                        conn.execute(
                            "INSERT INTO mesh_events(event_id, ts, event_type, payload, source_cluster_id) VALUES(?,?,?,?,?)",
                            (
                                event_id,
                                float(e.get("ts", now_ts())),
                                str(e.get("event_type", "UNKNOWN")),
                                json.dumps(e.get("payload") or {}),
                                str(e.get("source_cluster_id", "")),
                            ),
                        )
                        inserted += 1
                    except sqlite3.IntegrityError:
                        continue
                conn.commit()
            finally:
                conn.close()
        return inserted
