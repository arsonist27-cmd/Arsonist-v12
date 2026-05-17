from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("intelligence.recommendation")


class RecommendationPriority(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    urgent = "urgent"


class RecommendationStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    executed = "executed"
    dismissed = "dismissed"


class Recommendation(BaseModel):
    recommendation_id: str
    category: str = ""
    priority: RecommendationPriority = RecommendationPriority.medium
    status: RecommendationStatus = RecommendationStatus.pending
    title: str = ""
    description: str = ""
    impact: str = ""
    estimated_improvement_pct: float = 0.0
    target_region: str = ""
    target_workload: str = ""
    action_plan: List[str] = Field(default_factory=list)
    auto_executable: bool = False
    created_at: float = 0.0
    executed_at: float = 0.0


class RecommendationEngine:
    """Generates actionable infrastructure recommendations based on
    analysis from optimization, anomaly, and prediction engines."""

    def __init__(self, max_history: int = 500, auto_approve: bool = False) -> None:
        self._lock = threading.RLock()
        self._recommendations: List[Recommendation] = []
        self._max_history = max_history
        self._auto_approve = auto_approve
        self._total_generated = 0
        self._total_executed = 0
        self._events: List[Dict[str, Any]] = []

    def generate_from_inefficiencies(self, inefficiencies: List[Dict[str, Any]]) -> List[Recommendation]:
        results: List[Recommendation] = []
        ts = now_ts()

        for ineff in inefficiencies:
            severity = ineff.get("severity", 0.0)
            category = ineff.get("category", "unknown")
            region_id = ineff.get("region_id", "")
            priority = RecommendationPriority.urgent if severity > 0.9 else (
                RecommendationPriority.high if severity > 0.7 else RecommendationPriority.medium
            )

            rec = Recommendation(
                recommendation_id=f"rec-{category}-{region_id}-{int(ts)}",
                category=category,
                priority=priority,
                title=f"Optimize {category} in {region_id}",
                description=ineff.get("description", ""),
                impact=ineff.get("recommendation", ""),
                estimated_improvement_pct=ineff.get("potential_improvement_pct", 0.0),
                target_region=region_id,
                action_plan=[ineff.get("recommendation", "Apply optimization")],
                auto_executable=severity < 0.8,
                created_at=ts,
            )
            results.append(rec)

        with self._lock:
            self._recommendations.extend(results)
            if len(self._recommendations) > self._max_history:
                self._recommendations = self._recommendations[-self._max_history:]
            self._total_generated += len(results)

        return results

    def generate_from_predictions(self, predictions: List[Dict[str, Any]]) -> List[Recommendation]:
        results: List[Recommendation] = []
        ts = now_ts()

        for pred in predictions:
            recommendation_text = pred.get("recommendation", "")
            if not recommendation_text:
                continue
            trend = pred.get("trend", "stable")
            metric = pred.get("metric_name", "")
            region_id = pred.get("region_id", "")

            priority = RecommendationPriority.high if trend == "increasing" else RecommendationPriority.medium

            rec = Recommendation(
                recommendation_id=f"rec-pred-{metric}-{region_id}-{int(ts)}",
                category="predictive_scaling",
                priority=priority,
                title=f"Predictive action for {metric} in {region_id}",
                description=recommendation_text,
                impact=f"Prevent {metric} threshold breach in {region_id}",
                estimated_improvement_pct=15.0,
                target_region=region_id,
                action_plan=[recommendation_text],
                auto_executable=True,
                created_at=ts,
            )
            results.append(rec)

        with self._lock:
            self._recommendations.extend(results)
            if len(self._recommendations) > self._max_history:
                self._recommendations = self._recommendations[-self._max_history:]
            self._total_generated += len(results)

        return results

    def approve(self, recommendation_id: str) -> bool:
        with self._lock:
            for r in self._recommendations:
                if r.recommendation_id == recommendation_id and r.status == RecommendationStatus.pending:
                    r.status = RecommendationStatus.approved
                    self._events.append({"type": "recommendation_approved", "id": recommendation_id, "ts": now_ts()})
                    return True
        return False

    def execute(self, recommendation_id: str) -> bool:
        with self._lock:
            for r in self._recommendations:
                if r.recommendation_id == recommendation_id and r.status in (
                    RecommendationStatus.pending, RecommendationStatus.approved
                ):
                    r.status = RecommendationStatus.executed
                    r.executed_at = now_ts()
                    self._total_executed += 1
                    self._events.append({"type": "recommendation_executed", "id": recommendation_id, "ts": r.executed_at})
                    return True
        return False

    def dismiss(self, recommendation_id: str) -> bool:
        with self._lock:
            for r in self._recommendations:
                if r.recommendation_id == recommendation_id and r.status == RecommendationStatus.pending:
                    r.status = RecommendationStatus.dismissed
                    self._events.append({"type": "recommendation_dismissed", "id": recommendation_id, "ts": now_ts()})
                    return True
        return False

    def pending_recommendations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._recommendations
                    if r.status == RecommendationStatus.pending]

    def recent_recommendations(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in reversed(self._recommendations)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            pending = sum(1 for r in self._recommendations if r.status == RecommendationStatus.pending)
            return {
                "ts": now_ts(),
                "total_generated": self._total_generated,
                "total_executed": self._total_executed,
                "pending": pending,
            }
