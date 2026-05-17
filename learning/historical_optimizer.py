from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("learning.historical")


class HistoricalInsight(BaseModel):
    insight_id: str
    category: str = ""
    region_id: str = ""
    description: str = ""
    historical_avg: float = 0.0
    current_value: float = 0.0
    deviation_pct: float = 0.0
    recommendation: str = ""
    confidence: float = 0.0
    sample_count: int = 0
    created_at: float = 0.0


class HistoricalOptimizer:
    """Uses historical performance data to identify long-term trends and
    produce optimization recommendations based on past outcomes."""

    def __init__(self, window_size: int = 100, max_insights: int = 500) -> None:
        self._lock = threading.RLock()
        self._window = window_size
        self._max_insights = max_insights
        self._history: Dict[str, List[float]] = {}
        self._insights: List[HistoricalInsight] = []
        self._total_insights = 0

    def _record(self, key: str, value: float) -> None:
        if key not in self._history:
            self._history[key] = []
        self._history[key].append(value)
        if len(self._history[key]) > self._window:
            self._history[key] = self._history[key][-self._window:]

    def analyze(self, telemetry: Dict[str, Any]) -> List[HistoricalInsight]:
        results: List[HistoricalInsight] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")

            for metric, category in [
                ("avg_latency_ms", "latency"),
                ("gpu_utilization", "gpu"),
                ("workload_saturation", "capacity"),
                ("error_rate", "reliability"),
                ("cost_per_hour", "cost"),
            ]:
                value = r.get(metric, 0.0)
                key = f"hist:{metric}:{region_id}"
                self._record(key, value)

                values = self._history.get(key, [])
                if len(values) < 10:
                    continue

                avg = sum(values) / len(values)
                if avg == 0:
                    continue
                deviation = ((value - avg) / avg) * 100

                recommendation = ""
                if abs(deviation) > 30:
                    if metric == "avg_latency_ms" and deviation > 30:
                        recommendation = f"Latency in {region_id} is {deviation:.0f}% above historical average"
                    elif metric == "gpu_utilization" and deviation < -30:
                        recommendation = f"GPU utilization in {region_id} dropped {abs(deviation):.0f}% below average"
                    elif metric == "error_rate" and deviation > 50:
                        recommendation = f"Error rate in {region_id} spiked {deviation:.0f}% above normal"
                    elif metric == "cost_per_hour" and deviation > 20:
                        recommendation = f"Costs in {region_id} are {deviation:.0f}% above historical average"

                if recommendation:
                    insight = HistoricalInsight(
                        insight_id=f"hist-{metric}-{region_id}-{int(ts)}",
                        category=category,
                        region_id=region_id,
                        description=f"{metric} in {region_id}: current={value:.3f}, avg={avg:.3f}",
                        historical_avg=round(avg, 3),
                        current_value=round(value, 3),
                        deviation_pct=round(deviation, 1),
                        recommendation=recommendation,
                        confidence=min(0.9, 0.3 + len(values) * 0.006),
                        sample_count=len(values),
                        created_at=ts,
                    )
                    results.append(insight)

        with self._lock:
            self._insights.extend(results)
            if len(self._insights) > self._max_insights:
                self._insights = self._insights[-self._max_insights:]
            self._total_insights += len(results)

        return results

    def recent_insights(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [i.model_dump(mode="json") for i in reversed(self._insights)][:limit]

    def insights_for_region(self, region_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [i.model_dump(mode="json") for i in self._insights if i.region_id == region_id]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_insights": self._total_insights,
                "tracked_series": len(self._history),
            }
