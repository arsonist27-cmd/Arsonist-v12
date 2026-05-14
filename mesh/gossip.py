from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Callable, List, Optional

import httpx

from mesh.mesh_protocol import ClusterGossipState, GossipAck, GossipEnvelope, mesh_auth_headers, verify_mesh_payload, verify_mesh_timestamp
from mesh.peer_registry import PeerRecord, PeerRegistry
from observability.metrics import MeshMetricsCollector
from shared.utils import now_ts, setup_logging

logger = setup_logging("mesh.gossip")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


class GossipService:
    """
    Periodic random peer fan-out gossip with signed payloads.
    Merges remote peer knowledge into local PeerRegistry (eventual consistency).
    """

    def __init__(
        self,
        *,
        local_cluster_id: str,
        local_public_url: str,
        region: str,
        registry: PeerRegistry,
        build_snapshot: Callable[[], ClusterGossipState],
        metrics: MeshMetricsCollector | None = None,
        on_merge: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.local_cluster_id = local_cluster_id
        self.local_public_url = local_public_url.rstrip("/")
        self.region = region
        self.registry = registry
        self.build_snapshot = build_snapshot
        self.metrics = metrics or MeshMetricsCollector()
        self.on_merge = on_merge
        self.interval = _env_float("ARSONIST_GOSSIP_INTERVAL", 4.0)
        self.peer_ttl = _env_float("ARSONIST_PEER_TTL", 120.0)
        self.fanout = max(1, _env_int("ARSONIST_GOSSIP_FANOUT", 3))
        self._stop = asyncio.Event()
        self._client: httpx.AsyncClient | None = None

    def bind_stop(self, ev: asyncio.Event) -> None:
        """Share stop event with SyncEngine so background tasks exit together."""
        self._stop = ev

    async def run(self) -> None:
        limits = httpx.Limits(max_connections=32, max_keepalive_connections=16)
        timeout = httpx.Timeout(8.0, connect=3.0)
        self._client = httpx.AsyncClient(http2=False, limits=limits, timeout=timeout)
        try:
            while not self._stop.is_set():
                await self._tick_once()
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
        finally:
            if self._client:
                await self._client.aclose()
                self._client = None

    def stop(self) -> None:
        self._stop.set()

    async def handle_incoming(self, envelope: GossipEnvelope) -> GossipAck:
        """Merge inbound gossip; duplicate prevention via version+cluster_id."""
        merged = self.registry.merge_state([envelope.sender, *envelope.peers])
        if self.on_merge:
            self.on_merge(merged)
        self.metrics.gossip_messages_in += 1
        return GossipAck(merged_count=merged, accepted=True)

    async def _tick_once(self) -> None:
        self.registry.expire_stale(self.peer_ttl)
        snap = self.build_snapshot()
        peers = self.registry.pick_random_peers(self.local_cluster_id, self.fanout)
        if not peers and self._seed_urls():
            await self._gossip_seeds(snap)
            return
        if self._client is None:
            return
        tasks = [self._push_to_peer(snap, p) for p in peers]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _seed_urls(self) -> List[str]:
        raw = os.getenv("ARSONIST_MESH_SEED_URLS", "").strip()
        if not raw:
            return []
        return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]

    async def _gossip_seeds(self, snap: ClusterGossipState) -> None:
        if self._client is None:
            return
        for base in self._seed_urls():
            fake = PeerRecord(
                cluster_id=f"seed::{base}",
                public_url=base,
                region=self.region,
                last_seen=now_ts(),
            )
            await self._push_to_peer(snap, fake)

    async def _push_to_peer(self, snap: ClusterGossipState, peer: PeerRecord) -> None:
        assert self._client is not None
        local_peers = self.registry.list_peers()
        gossip_peers: List[ClusterGossipState] = []
        # include subset of known peers for propagation (bounded)
        cap = max(self.fanout * 4, 8)
        for p in local_peers[:cap]:
            if p.cluster_id == self.local_cluster_id:
                continue
            gossip_peers.append(
                ClusterGossipState(
                    cluster_id=p.cluster_id,
                    public_url=p.public_url,
                    region=p.region,
                    gpu_capacity=p.gpu_capacity,
                    load=p.load,
                    health=p.health,
                    queue_depth=p.queue_depth,
                    latency_ms=p.latency_estimate_ms,
                    heartbeat_ts=p.last_seen,
                    version=p.version,
                    reliability_score=p.reliability_score,
                    hop_distance=p.hop_distance,
                )
            )
        envelope = GossipEnvelope(sender=snap, peers=gossip_peers, trace_id=str(uuid.uuid4()), nonce=str(uuid.uuid4()))
        payload = envelope.model_dump(mode="json")
        url = f"{peer.public_url}/mesh/gossip"
        headers = {"Content-Type": "application/json", **mesh_auth_headers(payload)}
        t0 = time.perf_counter()
        try:
            resp = await self._client.post(url, json=payload, headers=headers)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code == 200:
                self.metrics.gossip_success += 1
                self.registry.update_latency_hint(peer.cluster_id, dt_ms)
            else:
                self.metrics.gossip_failures += 1
        except Exception:
            self.metrics.gossip_failures += 1
            logger.debug("gossip push failed peer=%s", peer.public_url, exc_info=True)


def verify_incoming_gossip(body: dict[str, Any], sig: str | None, ts: str | None) -> bool:
    if not verify_mesh_timestamp(ts):
        return False
    return verify_mesh_payload(body, sig)
