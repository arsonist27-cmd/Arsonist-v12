from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("prediction.scaling")


class ScalingDirection(str, Enum):
    scale_up = "scale_up"
    scale_down = "scale_down"
    no_change = "no_change"


class ScalingForecast(BaseModel):
    forecast_id: str
    region_id: str = ""
    resource_type: str = ""
    current_usage: float = 0.0
    predicted_usage: float = 0.0
    direction: ScalingDirection = ScalingDirection.no_change
    recommended_capacity_delta: float = 0.0
    confidence: float = 0.0
    horizon_minutes: int = 15
    trigger_threshold: float = 0.85
    warm_standby_count: int = 0
    forecasted_at: float = 0.0


class ScalingForecaster:
    """Predicts future resource demand and recommends pre-scaling actions
    including warm standby allocation to avoid cold-start latency."""

    def __init__(
        self,
        scale_up_threshold: float = 0.80,
        scale_down_threshold: float = 0.30,
        alpha: float = 0.3,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._scale_up_threshold = scale_up_threshold
        self._scale_down_threshold = scale_down_threshold
        self._alpha = alpha
        self._max_history = max_history
        self._series: Dict[str, List[float]] = {}
        self._forecasts: List[ScalingForecast] = []
        self._total_forecasts = 0
        self._pre_scale_events: List[Dict[str, Any]] = []

    def _record(self, key: str, value: float) -> None:
        if key not in self._series:
            self._series[key] = []
        self._series[key].append(value)
        if len(self._series[key]) > 200:
            self._series[key] = self._series[key][-200:]

    def _smooth_predict(self, key: str, steps: int = 3) -> float:
        values = self._series.get(key, [])
        if not values:
            return 0.0
        smoothed = values[0]
        trend = 0.0
        for v in values[1:]:
            prev = smoothed
            smoothed = self._alpha * v + (1 - self._alpha) * (smoothed + trend)
            trend = 0.1 * (smoothed - prev) + 0.9 * trend
        return smoothed + trend * steps

    def forecast(self, telemetry: Dict[str, Any], horizon_minutes: int = 15) -> List[ScalingForecast]:
        results: List[ScalingForecast] = []
        ts = now_ts()
        steps = max(1, horizon_minutes // 5)

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")

            for resource, usage_key in [
                ("gpu", "gpu_utilization"),
                ("cpu", "cpu_utilization"),
                ("memory", "memory_utilization"),
                ("bandwidth", "bandwidth_utilization"),
            ]:
                usage = r.get(usage_key, 0.0)
                if usage == 0.0 and resource != "gpu":
                    continue

                key = f"scaling:{resource}:{region_id}"
                self._record(key, usage)
                predicted = self._smooth_predict(key, steps)
                predicted = max(0.0, min(1.0, predicted))

                if predicted > self._scale_up_threshold:
                    direction = ScalingDirection.scale_up
                    delta = round((predicted - 0.7) * 100, 1)
                    warm = max(1, int(delta / 10))
                elif predicted < self._scale_down_threshold:
                    direction = ScalingDirection.scale_down
                    delta = round((0.5 - predicted) * -100, 1)
                    warm = 0
                else:
                    direction = ScalingDirection.no_change
                    delta = 0.0
                    warm = 0

                confidence = min(0.9, 0.3 + len(self._series.get(key, [])) * 0.01)

                results.append(ScalingForecast(
                    forecast_id=f"sf-{resource}-{region_id}-{int(ts)}",
                    region_id=region_id,
                    resource_type=resource,
                    current_usage=round(usage, 3),
                    predicted_usage=round(predicted, 3),
                    direction=direction,
                    recommended_capacity_delta=delta,
                    confidence=round(confidence, 3),
                    horizon_minutes=horizon_minutes,
                    trigger_threshold=self._scale_up_threshold,
                    warm_standby_count=warm,
                    forecasted_at=ts,
                ))

                if direction == ScalingDirection.scale_up:
                    self._pre_scale_events.append({
                        "type": "pre_scale_recommended",
                        "region_id": region_id,
                        "resource": resource,
                        "predicted_usage": round(predicted, 3),
                        "warm_standby": warm,
                        "ts": ts,
                    })

        with self._lock:
            self._forecasts.extend(results)
            if len(self._forecasts) > self._max_history:
                self._forecasts = self._forecasts[-self._max_history:]
            self._total_forecasts += len(results)
            if len(self._pre_scale_events) > self._max_history:
                self._pre_scale_events = self._pre_scale_events[-self._max_history:]

        return results

    def scale_up_recommendations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in self._forecasts
                    if f.direction == ScalingDirection.scale_up]

    def recent_forecasts(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in reversed(self._forecasts)][:limit]

    def recent_pre_scale_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._pre_scale_events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            scale_ups = sum(1 for f in self._forecasts if f.direction == ScalingDirection.scale_up)
            return {
                "ts": now_ts(),
                "total_forecasts": self._total_forecasts,
                "pending_scale_ups": scale_ups,
                "tracked_series": len(self._series),
            }
