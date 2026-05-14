from __future__ import annotations

from federation.federation_models import GlobalJobRecord, RoutingDecision
from federation.registry import FederationRegistry
from global_scheduler.cross_cluster_scheduler import best_cluster


def decide_route(reg: FederationRegistry, job: GlobalJobRecord) -> RoutingDecision:
    clusters = reg.list_clusters()
    healthy = [c for c in clusters if c.health_state.value != "offline"]
    target, ranked, ms = best_cluster(job, healthy)
    if not target:
        return RoutingDecision(target_cluster_id="", score=0.0, ranked=ranked, decision_ms=ms)
    top_score = ranked[0]["score"] if ranked else 0.0
    return RoutingDecision(target_cluster_id=target.cluster_id, score=top_score, ranked=ranked, decision_ms=ms)
