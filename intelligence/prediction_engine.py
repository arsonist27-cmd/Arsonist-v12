from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("intelligence.prediction")


class Prediction(BaseModel):
    prediction_id: str
    metric_name: str
    region_id: str = ""
    current_value: float = 0.0
    predicted_value: float = 0.0
    confidence: float = 0.0
    horizon_minutes: int = 15
    trend: str = "stable"
    recommendation: str = ""
    predicted_at: float = 0.0


class PredictionEngine:
    """Generates short-term predictions for infrastructure metrics using
    exponential smoothing and trend analysis."""

    def __init__(self, alpha: float = 0.3, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._alpha = alpha
        self._max_history = max_history
        self._series: Dict[str, List[float]] = {}
        self._predictions: List[Prediction] = []
        self._total_predictions = 0

    def _record(self, key: str, value: float) -> None:
        if key not in self._series:
            self._series[key] = []
        self._series[key].append(value)
        if len(self._series[key]) > 200:
            self._series[key] = self._series[key][-200:]

    def _exponential_smooth(self, key: str) -> float:
        values = self._series.get(key, [])
        if not values:
            return 0.0
        smoothed = values[0]
        for v in values[1:]:
            smoothed = self._alpha * v + (1 - self._alpha) * smoothed
        return smoothed

    def _trend(self, key: str) -> str:
        values = self._series.get(key, [])
        if len(values) < 3:
            return "stable"
        recent = values[-3:]
        if recent[-1] > recent[0] * 1.1:
            return "increasing"
        elif recent[-1] < recent[0] * 0.9:
            return "decreasing"
        return "stable"

    def _confidence(self, key: str) -> float:
        values = self._series.get(key, [])
        n = len(values)
        if n < 5:
            return 0.3
        if n < 15:
            return 0.5
        if n < 50:
            return 0.7
        return 0.85

    def predict(self, telemetry: Dict[str, Any], horizon_minutes: int = 15) -> List[Prediction]:
        results: List[Prediction] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")

            for metric in ["avg_latency_ms", "workload_saturation", "gpu_utilization",
                           "requests_per_second", "bandwidth_usage_mbps"]:
                value = r.get(metric, 0.0)
                if value == 0.0 and metric != "workload_saturation":
                    continue
                key = f"{metric}:{region_id}"
                self._record(key, value)
                smoothed = self._exponential_smooth(key)
                trend = self._trend(key)

                if trend == "increasing":
                    predicted = smoothed * (1 + 0.1 * (horizon_minutes / 15))
                elif trend == "decreasing":
                    predicted = smoothed * (1 - 0.05 * (horizon_minutes / 15))
                else:
                    predicted = smoothed

                recommendation = ""
                if metric == "workload_saturation" and predicted > 0.85:
                    recommendation = f"Pre-scale {region_id}: predicted saturation {predicted:.0%}"
                elif metric == "gpu_utilization" and predicted > 0.9:
                    recommendation = f"Add GPU capacity in {region_id}: predicted utilization {predicted:.0%}"
                elif metric == "avg_latency_ms" and predicted > 300:
                    recommendation = f"Reroute from {region_id}: predicted latency {predicted:.0f}ms"

                results.append(Prediction(
                    prediction_id=f"pred-{metric}-{region_id}-{int(ts)}",
                    metric_name=metric,
                    region_id=region_id,
                    current_value=round(value, 3),
                    predicted_value=round(predicted, 3),
                    confidence=self._confidence(key),
                    horizon_minutes=horizon_minutes,
                    trend=trend,
                    recommendation=recommendation,
                    predicted_at=ts,
                ))

        with self._lock:
            self._predictions.extend(results)
            if len(self._predictions) > self._max_history:
                self._predictions = self._predictions[-self._max_history:]
            self._total_predictions += len(results)

        return results

    def recent_predictions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in reversed(self._predictions)][:limit]

    def predictions_for_region(self, region_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._predictions if p.region_id == region_id]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_predictions": self._total_predictions,
                "tracked_series": len(self._series),
            }
