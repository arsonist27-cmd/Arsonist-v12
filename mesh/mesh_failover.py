from __future__ import annotations

from typing import Any, Dict, List, Optional

from mesh.peer_registry import PeerRegistry
from shared.utils import now_ts


def detect_orphaned_jobs(memory: Any, stale_sec: float = 180.0) -> List[str]:
    """Jobs marked running but no healthy assigned nodes recently — heuristic."""
    t = now_ts()
    bad: List[str] = []
    for job in memory.jobs.values():
        if job.status != "running":
            continue
        alive = False
        for nid in job.assigned_nodes:
            node = memory.nodes.get(nid)
            if node and node.healthy and (t - node.last_seen) < stale_sec:
                alive = True
                break
        if not alive and job.assigned_nodes:
            bad.append(job.id)
    return bad


def suggest_failover_target(
    registry: PeerRegistry,
    *,
    prefer_region: str,
    require_gpu: bool,
) -> Optional[str]:
    peers = registry.list_peers()
    best = None
    best_score = -1e9
    for p in peers:
        if require_gpu and p.gpu_capacity <= 0:
            continue
        if p.health == "offline":
            continue
        s = registry.score_peer(p)
        if p.region == prefer_region:
            s += 0.5
        if s > best_score:
            best_score = s
            best = p.public_url.rstrip("/")
    return best


def mesh_failover_snapshot(memory: Any, registry: PeerRegistry) -> Dict[str, Any]:
    orphans = detect_orphaned_jobs(memory)
    return {
        "orphan_candidates": orphans,
        "peer_count": len(registry.list_peers()),
        "ts": now_ts(),
    }
