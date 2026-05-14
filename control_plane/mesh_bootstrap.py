from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass
from typing import Any, Callable

from consensus.raft_adapter import RaftAdapter
from distributed_queue.event_log import EventLog
from distributed_queue.replicated_queue import ReplicatedQueue
from mesh.anti_entropy import AntiEntropyEngine
from mesh.gossip import GossipService
from mesh.mesh_protocol import ClusterGossipState, MeshEventType
from mesh.partition_handler import PartitionHandler
from mesh.peer_registry import PeerRegistry
from mesh.sync_engine import SyncEngine
from observability.metrics import MeshMetricsCollector
from shared.utils import now_ts, setup_logging

logger = setup_logging("mesh.bootstrap")


def mesh_enabled() -> bool:
    mode = os.getenv("ARSONIST_ORCHESTRATION_MODE", "").lower().strip()
    if mode == "mesh":
        return True
    return os.getenv("ARSONIST_MESH_ENABLED", "").lower() in ("1", "true", "yes")


@dataclass
class MeshRuntime:
    cluster_id: str
    public_url: str
    region: str
    registry: PeerRegistry
    event_log: EventLog
    replicated: ReplicatedQueue
    gossip: GossipService
    engine: SyncEngine
    metrics: MeshMetricsCollector
    partition: PartitionHandler
    raft: RaftAdapter | None = None


_RUNTIME: MeshRuntime | None = None


def get_runtime() -> MeshRuntime | None:
    return _RUNTIME


def _build_snapshot(memory: Any, cluster_id: str, public_url: str, region: str) -> ClusterGossipState:
    nodes = list(memory.nodes.values())
    qd = len(memory.queue_snapshot())
    load = sum(n.current_load for n in nodes) / len(nodes) if nodes else 0.0
    lat = sum(n.avg_latency_ms for n in nodes) / len(nodes) if nodes else 0.0
    gpu_cap = sum(1 for n in nodes if n.has_gpu)
    health = "healthy" if nodes else "degraded"
    return ClusterGossipState(
        cluster_id=cluster_id,
        public_url=public_url.rstrip("/"),
        region=region,
        gpu_capacity=gpu_cap,
        load=load,
        health=health,
        queue_depth=qd,
        latency_ms=lat,
        heartbeat_ts=now_ts(),
        version=int(now_ts()),
        reliability_score=1.0,
        hop_distance=0,
    )


def maybe_start_mesh(memory: Any, _schedule_once: Callable[[], None]) -> None:
    global _RUNTIME
    if not mesh_enabled():
        return
    if _RUNTIME is not None:
        return
    cluster_id = os.getenv("ARSONIST_CLUSTER_ID", "").strip()
    public_url = os.getenv("ARSONIST_CONTROL_PLANE_PUBLIC_URL", "").strip()
    region = os.getenv("ARSONIST_CLUSTER_REGION", "default").strip()
    if not cluster_id or not public_url:
        logger.warning("Mesh mode requested but ARSONIST_CLUSTER_ID or ARSONIST_CONTROL_PLANE_PUBLIC_URL missing")
        return

    registry = PeerRegistry()
    event_log = EventLog()
    replicated = ReplicatedQueue()
    metrics = MeshMetricsCollector()
    partition = PartitionHandler()
    anti = AntiEntropyEngine(event_log, registry)

    gossip = GossipService(
        local_cluster_id=cluster_id,
        local_public_url=public_url,
        region=region,
        registry=registry,
        build_snapshot=lambda: _build_snapshot(memory, cluster_id, public_url, region),
        metrics=metrics,
        on_merge=lambda n: event_log.append(MeshEventType.PEER_DISCOVERED, {"merged": n}, cluster_id) if n else None,
    )
    engine = SyncEngine(gossip, anti, metrics, anti_entropy_interval_sec=float(os.getenv("ARSONIST_ANTI_ENTROPY_INTERVAL_SEC", "30")))

    partners = [p.strip() for p in os.getenv("ARSONIST_RAFT_PARTNERS", "").split(",") if p.strip()]
    raft: RaftAdapter | None = RaftAdapter(self_node=cluster_id, partners=partners)
    raft.start()

    # Seed self into local registry for dashboards / routing baselines
    registry.merge_state([_build_snapshot(memory, cluster_id, public_url, region)])

    _RUNTIME = MeshRuntime(
        cluster_id=cluster_id,
        public_url=public_url,
        region=region,
        registry=registry,
        event_log=event_log,
        replicated=replicated,
        gossip=gossip,
        engine=engine,
        metrics=metrics,
        partition=partition,
        raft=raft,
    )

    def runner() -> None:
        asyncio.run(engine.run())

    threading.Thread(target=runner, daemon=True, name="mesh-asyncio").start()
    logger.info("Mesh runtime started cluster_id=%s", cluster_id)
