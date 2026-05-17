"""v15 Infrastructure Intelligence.

Provides intelligent analysis and insights across the planetary infrastructure,
combining telemetry data with historical patterns to generate actionable
intelligence about infrastructure health, efficiency, and optimization
opportunities.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.infrastructure_intelligence")


class InsightSeverity(str, Enum):
    info = "info"
    warning = "warning"
    critical = "critical"
    opportunity = "opportunity"


class InsightCategory(str, Enum):
    performance = "performance"
    efficiency = "efficiency"
    reliability = "reliability"
    cost = "cost"
    carbon = "carbon"
    capacity = "capacity"
    thermal = "thermal"


class InfrastructureInsight(BaseModel):
    insight_id: str
    category: InsightCategory = InsightCategory.performance
    severity: InsightSeverity = InsightSeverity.info
    title: str = ""
    description: str = ""
    affected_regions: List[str] = Field(default_factory=list)
    affected_continent: str = ""
    recommended_action: str = ""
    estimated_impact: str = ""
    score: float = 0.0
    ts: float = 0.0


class HealthScore(BaseModel):
    region_id: str = ""
    continent: str = ""
    overall: float = 1.0
    performance: float = 1.0
    reliability: float = 1.0
    efficiency: float = 1.0
    thermal: float = 1.0
    carbon: float = 1.0
    ts: float = 0.0


class InfrastructureIntelligence:
    """Analyzes planetary infrastructure telemetry to generate actionable
    insights, health scores, and optimization recommendations."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._insights: List[InfrastructureInsight] = []
        self._health_scores: Dict[str, HealthScore] = {}
        self._global_health: float = 1.0
        self._total_insights = 0
        self._events: List[Dict[str, Any]] = []

    def analyze(self, telemetry: Dict[str, Any]) -> List[InfrastructureInsight]:
        insights = []
        ts = now_ts()
        regions = telemetry.get("regions", [])

        for r in regions:
            rid = r.get("region_id", "")
            continent = r.get("continent", "")

            health = self._compute_health(r)
            with self._lock:
                self._health_scores[rid] = health

            sat = r.get("workload_saturation", 0)
            if sat > 0.9:
                insights.append(InfrastructureInsight(
                    insight_id=f"cap-{rid}-{int(ts)}",
                    category=InsightCategory.capacity,
                    severity=InsightSeverity.critical,
                    title=f"Capacity critical in {rid}",
                    description=f"Workload saturation at {sat:.0%}, risk of queue overflow",
                    affected_regions=[rid],
                    affected_continent=continent,
                    recommended_action="Scale up or migrate workloads to underutilized regions",
                    estimated_impact="Prevents queue drops and latency degradation",
                    score=sat,
                    ts=ts,
                ))
            elif sat > 0.75:
                insights.append(InfrastructureInsight(
                    insight_id=f"cap-warn-{rid}-{int(ts)}",
                    category=InsightCategory.capacity,
                    severity=InsightSeverity.warning,
                    title=f"Capacity warning in {rid}",
                    description=f"Workload saturation at {sat:.0%}, approaching limits",
                    affected_regions=[rid],
                    affected_continent=continent,
                    recommended_action="Consider pre-scaling or load balancing",
                    score=sat,
                    ts=ts,
                ))

            thermal = r.get("thermal_pressure", 0)
            if thermal > 0.85:
                insights.append(InfrastructureInsight(
                    insight_id=f"thermal-{rid}-{int(ts)}",
                    category=InsightCategory.thermal,
                    severity=InsightSeverity.critical,
                    title=f"Thermal pressure critical in {rid}",
                    description=f"Thermal pressure at {thermal:.0%}, GPU throttling likely",
                    affected_regions=[rid],
                    affected_continent=continent,
                    recommended_action="Reduce GPU workload or activate enhanced cooling",
                    score=thermal,
                    ts=ts,
                ))

            carbon = r.get("carbon_intensity", 0.5)
            renewable = r.get("renewable_pct", 0)
            if carbon > 0.7 and renewable < 0.2:
                insights.append(InfrastructureInsight(
                    insight_id=f"carbon-{rid}-{int(ts)}",
                    category=InsightCategory.carbon,
                    severity=InsightSeverity.opportunity,
                    title=f"Carbon optimization opportunity in {rid}",
                    description=f"High carbon intensity ({carbon:.2f}) with low renewables ({renewable:.0%})",
                    affected_regions=[rid],
                    affected_continent=continent,
                    recommended_action="Shift deferrable workloads to greener regions",
                    score=carbon,
                    ts=ts,
                ))

            latency = r.get("avg_latency_ms", 0)
            if latency > 300:
                insights.append(InfrastructureInsight(
                    insight_id=f"perf-{rid}-{int(ts)}",
                    category=InsightCategory.performance,
                    severity=InsightSeverity.warning,
                    title=f"High latency in {rid}",
                    description=f"Average latency {latency:.0f}ms exceeds target",
                    affected_regions=[rid],
                    affected_continent=continent,
                    recommended_action="Investigate network path or reroute traffic",
                    score=min(1.0, latency / 500),
                    ts=ts,
                ))

        scores = list(self._health_scores.values())
        if scores:
            self._global_health = round(sum(s.overall for s in scores) / len(scores), 3)

        with self._lock:
            self._insights.extend(insights)
            if len(self._insights) > self._max_history:
                self._insights = self._insights[-self._max_history:]
            self._total_insights += len(insights)

        return insights

    def _compute_health(self, region: Dict[str, Any]) -> HealthScore:
        sat = region.get("workload_saturation", 0)
        latency = region.get("avg_latency_ms", 0)
        thermal = region.get("thermal_pressure", 0)
        carbon = region.get("carbon_intensity", 0.5)
        status = region.get("status", "active")

        perf = max(0.0, 1.0 - latency / 500)
        reliability = 1.0 if status == "active" else (0.5 if status == "degraded" else 0.0)
        efficiency = max(0.0, 1.0 - sat)
        thermal_health = max(0.0, 1.0 - thermal)
        carbon_health = max(0.0, 1.0 - carbon)

        overall = (perf * 0.25 + reliability * 0.30 + efficiency * 0.20 +
                   thermal_health * 0.15 + carbon_health * 0.10)

        return HealthScore(
            region_id=region.get("region_id", ""),
            continent=region.get("continent", ""),
            overall=round(overall, 3),
            performance=round(perf, 3),
            reliability=round(reliability, 3),
            efficiency=round(efficiency, 3),
            thermal=round(thermal_health, 3),
            carbon=round(carbon_health, 3),
            ts=now_ts(),
        )

    def global_health(self) -> float:
        return self._global_health

    def health_scores(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: v.model_dump(mode="json") for k, v in self._health_scores.items()}

    def recent_insights(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [i.model_dump(mode="json") for i in reversed(self._insights)][:limit]

    def critical_insights(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [i.model_dump(mode="json") for i in self._insights
                    if i.severity == InsightSeverity.critical]

    def insights_by_category(self, category: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [i.model_dump(mode="json") for i in self._insights
                    if i.category.value == category]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_insights": self._total_insights,
                "global_health": self._global_health,
                "regions_monitored": len(self._health_scores),
            }
