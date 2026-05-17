from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("prediction.traffic")


class TrafficForecast(BaseModel):
    forecast_id: str
    region_id: str = ""
    current_rps: float = 0.0
    predicted_rps: float = 0.0
    peak_predicted_rps: float = 0.0
    horizon_minutes: int = 30
    confidence: float = 0.0
    trend: str = "stable"
    seasonality_factor: float = 1.0
    recommendation: str = ""
    forecasted_at: float = 0.0


class TrafficForecaster:
    """Forecasts traffic patterns using exponential smoothing with
    seasonality detection for proactive capacity planning."""

    def __init__(self, alpha: float = 0.3, beta: float = 0.1, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._alpha = alpha
        self._beta = beta
        self._max_history = max_history
        self._series: Dict[str, List[float]] = {}
        self._forecasts: List[TrafficForecast] = []
        self._total_forecasts = 0

    def _record(self, key: str, value: float) -> None:
        if key not in self._series:
            self._series[key] = []
        self._series[key].append(value)
        if len(self._series[key]) > 300:
            self._series[key] = self._series[key][-300:]

    def _double_exponential_smooth(self, values: List[float]) -> tuple[float, float]:
        if not values:
            return 0.0, 0.0
        level = values[0]
        trend = 0.0
        for v in values[1:]:
            prev_level = level
            level = self._alpha * v + (1 - self._alpha) * (level + trend)
            trend = self._beta * (level - prev_level) + (1 - self._beta) * trend
        return level, trend

    def _detect_seasonality(self, values: List[float]) -> float:
        if len(values) < 20:
            return 1.0
        recent_avg = sum(values[-10:]) / 10
        older_avg = sum(values[-20:-10]) / 10
        if older_avg == 0:
            return 1.0
        return round(recent_avg / older_avg, 3)

    def forecast(self, telemetry: Dict[str, Any], horizon_minutes: int = 30) -> List[TrafficForecast]:
        results: List[TrafficForecast] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            rps = r.get("requests_per_second", 0.0)
            key = f"traffic:{region_id}"
            self._record(key, rps)

            values = self._series.get(key, [])
            level, trend = self._double_exponential_smooth(values)
            seasonality = self._detect_seasonality(values)

            steps = horizon_minutes // 5
            predicted = (level + trend * steps) * seasonality
            peak = predicted * 1.3

            confidence = min(0.9, 0.3 + len(values) * 0.01)
            trend_dir = "increasing" if trend > 0.5 else ("decreasing" if trend < -0.5 else "stable")

            recommendation = ""
            if predicted > rps * 1.5:
                recommendation = f"Pre-scale {region_id}: traffic expected to increase {predicted/max(rps,1):.1f}x"
            elif predicted < rps * 0.5 and rps > 10:
                recommendation = f"Scale down {region_id}: traffic expected to decrease"

            results.append(TrafficForecast(
                forecast_id=f"tf-{region_id}-{int(ts)}",
                region_id=region_id,
                current_rps=round(rps, 2),
                predicted_rps=round(max(0, predicted), 2),
                peak_predicted_rps=round(max(0, peak), 2),
                horizon_minutes=horizon_minutes,
                confidence=round(confidence, 3),
                trend=trend_dir,
                seasonality_factor=seasonality,
                recommendation=recommendation,
                forecasted_at=ts,
            ))

        with self._lock:
            self._forecasts.extend(results)
            if len(self._forecasts) > self._max_history:
                self._forecasts = self._forecasts[-self._max_history:]
            self._total_forecasts += len(results)

        return results

    def recent_forecasts(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in reversed(self._forecasts)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_forecasts": self._total_forecasts,
                "tracked_regions": len(self._series),
            }
