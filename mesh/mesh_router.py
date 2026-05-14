from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from mesh.peer_registry import PeerRecord, PeerRegistry
from shared.models import JobRecord
from shared.utils import now_ts


class RouteDecision(BaseModel):
    target_cluster_id: str
    target_public_url: str
    score: float
    reason: str
    hops: int = 1


class MeshRouter:
    """
    Local routing decisions across mesh peers (multi-hop via hop_distance hints).
    Uses GPU availability, backlog, latency, reliability, region affinity.
    """

    def __init__(self, registry: PeerRegistry, local_cluster_id: str, local_region: str) -> None:
        self.registry = registry
        self.local_cluster_id = local_cluster_id
        self.local_region = local_region

    def rank_peers_for_job(self, job: JobRecord) -> List[RouteDecision]:
        peers = [p for p in self.registry.list_peers() if p.cluster_id != self.local_cluster_id]
        scored: List[RouteDecision] = []
        for p in peers:
            score, reason = self._score_peer(job, p)
            scored.append(
                RouteDecision(
                    target_cluster_id=p.cluster_id,
                    target_public_url=p.public_url,
                    score=score,
                    reason=reason,
                    hops=max(1, int(p.hop_distance or 1)),
                )
            )
        scored.sort(key=lambda r: r.score, reverse=True)
        return scored

    def _score_peer(self, job: JobRecord, peer: PeerRecord) -> Tuple[float, str]:
        reasons: List[str] = []
        base = self.registry.score_peer(peer)
        if job.gpu_required and peer.gpu_capacity <= 0:
            return -1e9, "no_gpu"
        if peer.health == "offline":
            return -1e9, "offline"
        region_bonus = 0.35 if peer.region == self.local_region else 0.0
        reasons.append("region" if region_bonus else "remote_region")
        net = float(os.getenv("ARSONIST_MESH_NETWORK_DISTANCE_WEIGHT", "0.08"))
        hop_penalty = net * min(peer.hop_distance, 8)
        gpu_bonus = min(peer.gpu_capacity, 32) * 0.04 if job.gpu_required else min(peer.gpu_capacity, 32) * 0.01
        final = base + region_bonus + gpu_bonus - hop_penalty
        return final, "+".join(reasons)

    def best_forward(self, job: JobRecord) -> Optional[RouteDecision]:
        ranked = self.rank_peers_for_job(job)
        return ranked[0] if ranked and ranked[0].score > -1e8 else None

    def routing_metrics(self) -> Dict[str, Any]:
        peers = self.registry.list_peers()
        return {
            "peer_count": len(peers),
            "ts": now_ts(),
            "regions": sorted({p.region for p in peers}),
        }
