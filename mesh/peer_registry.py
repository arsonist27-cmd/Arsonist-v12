from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

from mesh.mesh_protocol import ClusterGossipState
from shared.utils import now_ts


class PeerRecord(BaseModel):
    cluster_id: str
    public_url: str
    region: str = "default"
    gpu_capacity: int = 0
    load: float = 0.0
    health: str = "healthy"
    latency_estimate_ms: float = 0.0
    last_seen: float = 0.0
    queue_depth: int = 0
    version: int = 0
    reliability_score: float = 1.0
    hop_distance: int = 1


class PeerRegistry:
    """
    Decentralized peer cache with SQLite persistence (no central SoT).
    Merges CRDT-style by max(version, last_seen) per cluster_id.
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self.db_path = db_path or os.getenv(
            "ARSONIST_MESH_PEER_DB",
            os.path.join(os.path.dirname(os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db")), "mesh_peers.db"),
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
                CREATE TABLE IF NOT EXISTS peers (
                    cluster_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
        conn.close()

    def _row_to_peer(self, row: sqlite3.Row) -> PeerRecord:
        return PeerRecord(**json.loads(row["payload"]))

    def get(self, cluster_id: str) -> Optional[PeerRecord]:
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT payload FROM peers WHERE cluster_id = ?", (cluster_id,)).fetchone()
                return self._row_to_peer(row) if row else None
            finally:
                conn.close()

    def list_peers(self) -> List[PeerRecord]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT payload FROM peers ORDER BY cluster_id ASC").fetchall()
                return [self._row_to_peer(r) for r in rows]
            finally:
                conn.close()

    def merge_state(self, states: Iterable[ClusterGossipState | PeerRecord]) -> int:
        """Upsert peers from gossip; returns number of rows written/changed."""
        changed = 0
        ts = now_ts()
        with self._lock:
            conn = self._conn()
            try:
                for s in states:
                    pr = self._to_peer_record(s)
                    existing = conn.execute("SELECT payload FROM peers WHERE cluster_id = ?", (pr.cluster_id,)).fetchone()
                    if existing:
                        cur = PeerRecord(**json.loads(existing["payload"]))
                        if self._should_replace(cur, pr):
                            conn.execute(
                                "UPDATE peers SET payload = ?, updated_at = ? WHERE cluster_id = ?",
                                (json.dumps(pr.model_dump()), ts, pr.cluster_id),
                            )
                            changed += 1
                    else:
                        conn.execute(
                            "INSERT INTO peers(cluster_id, payload, updated_at) VALUES(?,?,?)",
                            (pr.cluster_id, json.dumps(pr.model_dump()), ts),
                        )
                        changed += 1
                conn.commit()
            finally:
                conn.close()
        return changed

    def _to_peer_record(self, s: ClusterGossipState | PeerRecord) -> PeerRecord:
        if isinstance(s, PeerRecord):
            return s
        return PeerRecord(
            cluster_id=s.cluster_id,
            public_url=s.public_url.rstrip("/"),
            region=s.region,
            gpu_capacity=s.gpu_capacity,
            load=s.load,
            health=s.health,
            latency_estimate_ms=s.latency_ms,
            last_seen=s.heartbeat_ts or now_ts(),
            queue_depth=s.queue_depth,
            version=s.version,
            reliability_score=s.reliability_score,
            hop_distance=s.hop_distance or 1,
        )

    def _should_replace(self, cur: PeerRecord, nxt: PeerRecord) -> bool:
        if nxt.version > cur.version:
            return True
        if nxt.version == cur.version and nxt.last_seen >= cur.last_seen:
            return True
        return nxt.last_seen > cur.last_seen + 1.0

    def expire_stale(self, ttl_sec: float, now: float | None = None) -> int:
        """Remove peers not refreshed within TTL (wall clock vs last_seen)."""
        t = now if now is not None else now_ts()
        removed = 0
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT cluster_id, payload FROM peers").fetchall()
                for row in rows:
                    peer = PeerRecord(**json.loads(row["payload"]))
                    if t - peer.last_seen > ttl_sec:
                        conn.execute("DELETE FROM peers WHERE cluster_id = ?", (peer.cluster_id,))
                        removed += 1
                conn.commit()
            finally:
                conn.close()
        return removed

    def score_peer(self, peer: PeerRecord) -> float:
        """Higher is better for routing."""
        load_penalty = peer.load * 2.0
        q_penalty = min(peer.queue_depth, 500) * 0.05
        lat_penalty = min(peer.latency_estimate_ms, 5000) / 500.0
        health = 1.0 if peer.health == "healthy" else 0.5 if peer.health == "degraded" else 0.1
        hop_penalty = min(peer.hop_distance, 6) * 0.07
        return peer.reliability_score * health * 10.0 - load_penalty - q_penalty - lat_penalty - hop_penalty

    def pick_random_peers(self, exclude: str, fanout: int) -> List[PeerRecord]:
        peers = [p for p in self.list_peers() if p.cluster_id != exclude]
        if not peers:
            return []
        k = min(fanout, len(peers))
        return random.sample(peers, k)

    def update_latency_hint(self, cluster_id: str, sample_ms: float) -> None:
        peer = self.get(cluster_id)
        if not peer:
            return
        beta = 0.35
        peer.latency_estimate_ms = beta * sample_ms + (1.0 - beta) * peer.latency_estimate_ms
        peer.last_seen = now_ts()
        with self._lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "UPDATE peers SET payload = ?, updated_at = ? WHERE cluster_id = ?",
                    (json.dumps(peer.model_dump()), peer.last_seen, cluster_id),
                )
            conn.close()
