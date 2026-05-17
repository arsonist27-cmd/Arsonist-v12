from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from regions.latency_map import LatencyMap
from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from regions.regional_capacity import RegionalCapacityTracker
from shared.utils import now_ts, setup_logging

logger = setup_logging("routing.global_router")


class RoutingStrategy(str, Enum):
    nearest = "nearest"
    weighted = "weighted"
    least_loaded = "least_loaded"
    gpu_affinity = "gpu_affinity"
    model_locality = "model_locality"


class RoutingRequest(BaseModel):
    request_id: str
    client_region: str = ""
    client_lat: float = 0.0
    client_lon: float = 0.0
    model_id: str = ""
    require_gpu: bool = False
    min_vram_gb: float = 0.0
    preferred_region: str = ""
    strategy: RoutingStrategy = RoutingStrategy.weighted
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RoutingDecision(BaseModel):
    request_id: str
    target_region: str
    score: float = 0.0
    latency_ms: float = 0.0
    strategy_used: RoutingStrategy = RoutingStrategy.weighted
    fallback_regions: List[str] = Field(default_factory=list)
    decision_time_ms: float = 0.0
    ts: float = 0.0


class GlobalRouter:
    """Routes inference requests across regions using multi-factor scoring."""

    WEIGHT_LATENCY = 0.30
    WEIGHT_LOAD = 0.25
    WEIGHT_GPU = 0.20
    WEIGHT_QUEUE = 0.15
    WEIGHT_BANDWIDTH = 0.10

    def __init__(
        self,
        registry: RegionRegistry,
        latency_map: LatencyMap,
        capacity_tracker: RegionalCapacityTracker,
        model_locations: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        self.registry = registry
        self.latency_map = latency_map
        self.capacity = capacity_tracker
        self._model_locations: Dict[str, List[str]] = model_locations or {}
        self._lock = threading.Lock()
        self._decisions: List[RoutingDecision] = []
        self._total_routed = 0
        self._reroute_count = 0

    def route(self, request: RoutingRequest) -> Optional[RoutingDecision]:
        start = now_ts()
        candidates = self._get_candidates(request)
        if not candidates:
            logger.warning("No candidates for request %s", request.request_id)
            return None

        scored = self._score_candidates(request, candidates)
        if not scored:
            return None

        scored.sort(key=lambda x: x[1], reverse=True)
        best_region, best_score = scored[0]
        fallbacks = [r for r, _ in scored[1:4]]

        decision = RoutingDecision(
            request_id=request.request_id,
            target_region=best_region,
            score=round(best_score, 4),
            strategy_used=request.strategy,
            fallback_regions=fallbacks,
            decision_time_ms=round((now_ts() - start) * 1000, 2),
            ts=now_ts(),
        )

        with self._lock:
            self._decisions.append(decision)
            if len(self._decisions) > 500:
                self._decisions = self._decisions[-500:]
            self._total_routed += 1

        logger.info(
            "Routed %s -> %s (score=%.3f, time=%.1fms)",
            request.request_id, best_region, best_score, decision.decision_time_ms,
        )
        return decision

    def _get_candidates(self, request: RoutingRequest) -> List[RegionRecord]:
        candidates = self.registry.active_regions()
        if request.require_gpu:
            candidates = [r for r in candidates if r.gpu_inventory.available_gpus > 0]
        if request.min_vram_gb > 0:
            candidates = [r for r in candidates if r.gpu_inventory.available_vram_gb >= request.min_vram_gb]
        return candidates

    def _score_candidates(self, request: RoutingRequest, candidates: List[RegionRecord]) -> List[tuple[str, float]]:
        scored: List[tuple[str, float]] = []
        for region in candidates:
            score = self._compute_score(request, region)
            scored.append((region.region_id, score))
        return scored

    def _compute_score(self, request: RoutingRequest, region: RegionRecord) -> float:
        latency_score = self._latency_score(request, region)
        load_score = 1.0 - region.workload_saturation
        gpu_score = self._gpu_score(region)
        queue_score = 1.0 - min(region.workload_saturation * 0.8, 1.0)
        bandwidth_score = 1.0 if region.edge_connectivity else 0.5

        score = (
            self.WEIGHT_LATENCY * latency_score
            + self.WEIGHT_LOAD * load_score
            + self.WEIGHT_GPU * gpu_score
            + self.WEIGHT_QUEUE * queue_score
            + self.WEIGHT_BANDWIDTH * bandwidth_score
        )

        if request.preferred_region and region.region_id == request.preferred_region:
            score += 0.15

        if request.model_id and request.strategy == RoutingStrategy.model_locality:
            locations = self._model_locations.get(request.model_id, [])
            if region.region_id in locations:
                score += 0.20

        if region.status == RegionStatus.degraded:
            score *= 0.7

        return score

    def _latency_score(self, request: RoutingRequest, region: RegionRecord) -> float:
        if request.client_region:
            measured = self.latency_map.get_inter_region(request.client_region, region.region_id)
            if measured is not None:
                return max(0.0, 1.0 - measured / 500.0)
        if region.avg_latency_ms > 0:
            return max(0.0, 1.0 - region.avg_latency_ms / 500.0)
        return 0.5

    def _gpu_score(self, region: RegionRecord) -> float:
        inv = region.gpu_inventory
        if inv.total_gpus == 0:
            return 0.0
        return inv.available_gpus / inv.total_gpus

    def update_model_locations(self, model_id: str, regions: List[str]) -> None:
        with self._lock:
            self._model_locations[model_id] = regions

    def recent_decisions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.model_dump() for d in reversed(self._decisions)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._decisions[-100:] if self._decisions else []
            avg_time = (
                sum(d.decision_time_ms for d in recent) / len(recent) if recent else 0.0
            )
            return {
                "ts": now_ts(),
                "total_routed": self._total_routed,
                "reroute_count": self._reroute_count,
                "avg_decision_time_ms": round(avg_time, 2),
                "recent_decisions": len(recent),
            }
