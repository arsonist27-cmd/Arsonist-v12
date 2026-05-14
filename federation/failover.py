from __future__ import annotations

from typing import Callable, List, Optional, Tuple

from federation.federation_models import ClusterRecord, FailoverEvent, GlobalJobRecord, GlobalJobStatus
from federation.registry import FederationRegistry
from global_scheduler.cross_cluster_scheduler import best_cluster
from shared.utils import now_ts


def reroute_jobs_from_dead_cluster(
    reg: FederationRegistry,
    dead_cluster_id: str,
    push_fn: Optional[Callable[[GlobalJobRecord, ClusterRecord], None]] = None,
) -> Tuple[FailoverEvent, List[Tuple[GlobalJobRecord, ClusterRecord]]]:
    """
    Reassign jobs that were assigned to dead_cluster_id to the next-best clusters.
    Returns failover event plus (job, target_cluster) pairs for async push.
    """
    others = [
        c
        for c in reg.list_clusters()
        if c.cluster_id != dead_cluster_id and c.health_state.value != "offline"
    ]
    rerouted = 0
    pushes: List[Tuple[GlobalJobRecord, ClusterRecord]] = []
    for job in reg.list_global_jobs():
        if job.assigned_cluster_id != dead_cluster_id:
            continue
        if job.status not in (GlobalJobStatus.queued, GlobalJobStatus.routed, GlobalJobStatus.running, GlobalJobStatus.migrated):
            continue
        logs = list(job.execution_logs)
        logs.append(f"{now_ts()}: failover from {dead_cluster_id}")
        base = job.model_dump()
        base["execution_logs"] = logs
        gjr = GlobalJobRecord(**base)
        pick, _ranked, _ms = best_cluster(gjr, others)
        if not pick:
            gjr.status = GlobalJobStatus.queued
            gjr.assigned_cluster_id = None
            reg.save_global_job(gjr)
            continue
        gjr.assigned_cluster_id = pick.cluster_id
        gjr.status = GlobalJobStatus.migrated
        gjr.execution_logs.append(f"{now_ts()}: rerouted to {pick.cluster_id} (failover)")
        reg.save_global_job(gjr)
        rerouted += 1
        reg.increment_metric("failover_reroutes_total")
        pushes.append((gjr, pick))
        if push_fn:
            push_fn(gjr, pick)

    ev = FailoverEvent(ts=now_ts(), dead_cluster_id=dead_cluster_id, jobs_rerouted=rerouted)
    reg.emit_event("failover", ev.model_dump())
    reg.increment_metric("failover_events_total")
    return ev, pushes
