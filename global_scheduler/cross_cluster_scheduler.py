from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

from federation.federation_models import ClusterHealth, ClusterRecord, GlobalJobRecord


def score_cluster(job: GlobalJobRecord, cluster: ClusterRecord) -> float:
    """Higher is better. Target <500ms decision time — pure CPU scoring."""
    if cluster.health_state == ClusterHealth.offline:
        return -1.0
    load_penalty = cluster.current_load * 0.35
    queue_penalty = min(cluster.queue_depth, 500) / 500.0 * 0.22
    latency_component = max(0.0, 1.0 - min(cluster.avg_latency_ms, 3000.0) / 3000.0) * 0.18
    gpu_bonus = 0.0
    if job.gpu_required:
        gpu_bonus = min(cluster.gpu_capacity, 64) / 64.0 * 0.40
    else:
        gpu_bonus = min(cluster.gpu_capacity, 64) / 64.0 * 0.10
    health_map = {
        ClusterHealth.healthy: 0.20,
        ClusterHealth.degraded: 0.10,
        ClusterHealth.unknown: 0.05,
        ClusterHealth.offline: 0.0,
    }
    health_w = health_map.get(cluster.health_state, 0.0)
    node_capacity = min(cluster.node_count, 100) / 100.0 * 0.05
    region_bonus = 0.0
    if job.preferred_region and cluster.region and job.preferred_region.strip().lower() == cluster.region.strip().lower():
        region_bonus = 0.12
    return round(
        1.0 - load_penalty - queue_penalty + latency_component + gpu_bonus + health_w + node_capacity + region_bonus,
        6,
    )


def rank_clusters(
    job: GlobalJobRecord,
    clusters: List[ClusterRecord],
) -> List[Tuple[ClusterRecord, float]]:
    t0 = time.perf_counter()
    ranked: List[Tuple[ClusterRecord, float]] = []
    for c in clusters:
        if c.health_state == ClusterHealth.offline:
            continue
        s = score_cluster(job, c)
        if s < 0:
            continue
        ranked.append((c, s))
    ranked.sort(key=lambda x: x[1], reverse=True)
    _ = time.perf_counter() - t0
    return ranked


def best_cluster(
    job: GlobalJobRecord,
    clusters: List[ClusterRecord],
) -> Tuple[ClusterRecord | None, List[Dict[str, Any]], float]:
    t0 = time.perf_counter()
    ranked = rank_clusters(job, clusters)
    out_list: List[Dict[str, Any]] = [
        {"cluster_id": c.cluster_id, "region": c.region, "score": s} for c, s in ranked
    ]
    best = ranked[0][0] if ranked else None
    ms = (time.perf_counter() - t0) * 1000.0
    return best, out_list, ms
