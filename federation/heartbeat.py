from __future__ import annotations

import os
from typing import List, Set

from federation.federation_models import ClusterHealth, ClusterRecord, FederationHeartbeatPayload
from federation.registry import FederationRegistry
from shared.utils import now_ts


def apply_heartbeat(reg: FederationRegistry, payload: FederationHeartbeatPayload) -> ClusterRecord:
    existing = reg.get_cluster(payload.cluster_id)
    if not existing:
        raise KeyError("unknown cluster; register first")
    was_offline = existing.health_state == ClusterHealth.offline
    rec = existing.model_copy(
        update={
            "node_count": payload.node_count,
            "gpu_capacity": payload.gpu_capacity,
            "current_load": payload.current_load,
            "queue_depth": payload.queue_depth,
            "avg_latency_ms": payload.avg_latency_ms,
            "health_state": payload.health_state,
            "last_heartbeat": now_ts(),
            "consecutive_misses": 0,
        }
    )
    reg.upsert_cluster(rec)
    if was_offline and payload.health_state != ClusterHealth.offline:
        reg.emit_event(
            "cluster_recovered",
            {"cluster_id": payload.cluster_id, "health_state": payload.health_state.value},
        )
    return rec


def sweep_stale_clusters(
    reg: FederationRegistry,
    heartbeat_timeout_sec: float | None = None,
) -> List[str]:
    """Mark clusters offline when heartbeat stale; return newly offline ids."""
    timeout = heartbeat_timeout_sec if heartbeat_timeout_sec is not None else float(
        os.getenv("FEDERATION_HEARTBEAT_TIMEOUT_SEC", "45")
    )
    now = now_ts()
    offline_ids: List[str] = []
    for cluster in reg.list_clusters():
        if cluster.health_state == ClusterHealth.offline:
            continue
        if cluster.last_heartbeat <= 0:
            continue
        if now - cluster.last_heartbeat > timeout:
            offline = cluster.model_copy(
                update={
                    "health_state": ClusterHealth.offline,
                    "consecutive_misses": cluster.consecutive_misses + 1,
                }
            )
            reg.upsert_cluster(offline)
            reg.emit_event(
                "cluster_offline",
                {"cluster_id": cluster.cluster_id, "last_heartbeat": cluster.last_heartbeat, "timeout_sec": timeout},
            )
            offline_ids.append(cluster.cluster_id)
    return offline_ids


def revive_cluster_if_heartbeat(reg: FederationRegistry, cluster_id: str) -> None:
    c = reg.get_cluster(cluster_id)
    if c and c.health_state == ClusterHealth.offline:
        reg.upsert_cluster(c.model_copy(update={"health_state": ClusterHealth.degraded}))
