from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("learning.telemetry")


class LearnedPattern(BaseModel):
    pattern_id: str
    metric_name: str = ""
    region_id: str = ""
    pattern_type: str = ""
    description: str = ""
    confidence: float = 0.0
    sample_count: int = 0
    avg_value: float = 0.0
    stddev: float = 0.0
    trend: str = "stable"
    actionable: bool = False
    recommendation: str = ""
    learned_at: float = 0.0


class TelemetryLearner:
    """Continuously learns from telemetry data to identify workload patterns,
    traffic behavior, scaling history, deployment success rates, and routing
    efficiency. Produces actionable insights for improving scheduling,
    failover, and replication decisions."""

    def __init__(self, min_samples: int = 10, max_patterns: int = 500) -> None:
        self._lock = threading.RLock()
        self._min_samples = min_samples
        self._max_patterns = max_patterns
        self._series: Dict[str, List[float]] = {}
        self._patterns: Dict[str, LearnedPattern] = {}
        self._total_learned = 0
        self._events: List[Dict[str, Any]] = []

    def _record(self, key: str, value: float) -> None:
        if key not in self._series:
            self._series[key] = []
        self._series[key].append(value)
        if len(self._series[key]) > 500:
            self._series[key] = self._series[key][-500:]

    def _compute_stats(self, values: List[float]) -> tuple[float, float]:
        if not values:
            return 0.0, 0.0
        mean = sum(values) / len(values)
        if len(values) < 2:
            return mean, 0.0
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return mean, variance ** 0.5

    def _detect_trend(self, values: List[float]) -> str:
        if len(values) < 5:
            return "stable"
        first_half = sum(values[:len(values) // 2]) / (len(values) // 2)
        second_half = sum(values[len(values) // 2:]) / (len(values) - len(values) // 2)
        if first_half == 0:
            return "stable"
        ratio = second_half / first_half
        if ratio > 1.15:
            return "increasing"
        if ratio < 0.85:
            return "decreasing"
        return "stable"

    def learn(self, telemetry: Dict[str, Any]) -> List[LearnedPattern]:
        patterns: List[LearnedPattern] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")

            for metric in ["avg_latency_ms", "workload_saturation", "gpu_utilization",
                           "requests_per_second", "error_rate", "bandwidth_usage_mbps"]:
                value = r.get(metric, 0.0)
                key = f"{metric}:{region_id}"
                self._record(key, value)

                values = self._series.get(key, [])
                if len(values) < self._min_samples:
                    continue

                avg, sd = self._compute_stats(values)
                trend = self._detect_trend(values)
                confidence = min(0.95, 0.3 + len(values) * 0.005)

                recommendation = ""
                actionable = False

                if metric == "workload_saturation" and avg > 0.75 and trend == "increasing":
                    recommendation = f"Pre-scale {region_id}: sustained high saturation trending up"
                    actionable = True
                elif metric == "gpu_utilization" and avg < 0.3:
                    recommendation = f"Consolidate GPU workloads in {region_id}: low average utilization"
                    actionable = True
                elif metric == "error_rate" and avg > 0.05:
                    recommendation = f"Investigate {region_id}: elevated error rate pattern"
                    actionable = True
                elif metric == "avg_latency_ms" and avg > 200 and trend == "increasing":
                    recommendation = f"Reroute from {region_id}: latency trending upward"
                    actionable = True

                pattern = LearnedPattern(
                    pattern_id=f"pattern-{metric}-{region_id}",
                    metric_name=metric,
                    region_id=region_id,
                    pattern_type=f"{trend}_{metric}",
                    description=f"{metric} in {region_id}: avg={avg:.2f}, sd={sd:.2f}, trend={trend}",
                    confidence=round(confidence, 3),
                    sample_count=len(values),
                    avg_value=round(avg, 3),
                    stddev=round(sd, 3),
                    trend=trend,
                    actionable=actionable,
                    recommendation=recommendation,
                    learned_at=ts,
                )
                patterns.append(pattern)

                with self._lock:
                    self._patterns[pattern.pattern_id] = pattern

        scaling = telemetry.get("scaling_history", [])
        for event in scaling:
            key = f"scaling_success:{event.get('region_id', 'unknown')}"
            self._record(key, 1.0 if event.get("success", False) else 0.0)

        deployments = telemetry.get("deployment_history", [])
        for d in deployments:
            key = f"deploy_success:{d.get('region_id', 'unknown')}"
            self._record(key, 1.0 if d.get("success", False) else 0.0)

        with self._lock:
            self._total_learned += len(patterns)
            if len(self._patterns) > self._max_patterns:
                sorted_keys = sorted(self._patterns, key=lambda k: self._patterns[k].learned_at)
                for k in sorted_keys[:len(self._patterns) - self._max_patterns]:
                    del self._patterns[k]

        return patterns

    def actionable_insights(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._patterns.values() if p.actionable]

    def patterns_for_region(self, region_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._patterns.values()
                    if p.region_id == region_id]

    def all_patterns(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._patterns.values()]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            actionable = sum(1 for p in self._patterns.values() if p.actionable)
            return {
                "ts": now_ts(),
                "total_learned": self._total_learned,
                "active_patterns": len(self._patterns),
                "actionable_insights": actionable,
                "tracked_series": len(self._series),
            }
