from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("prediction.gpu_demand")


class GPUDemandForecast(BaseModel):
    forecast_id: str
    region_id: str = ""
    gpu_type: str = ""
    current_demand: float = 0.0
    predicted_demand: float = 0.0
    current_supply: int = 0
    recommended_supply: int = 0
    deficit: int = 0
    confidence: float = 0.0
    horizon_minutes: int = 30
    trend: str = "stable"
    forecasted_at: float = 0.0


class GPUDemandPredictor:
    """Predicts GPU demand per region and type using historical usage patterns
    to enable proactive GPU provisioning."""

    def __init__(self, alpha: float = 0.3, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._alpha = alpha
        self._max_history = max_history
        self._series: Dict[str, List[float]] = {}
        self._forecasts: List[GPUDemandForecast] = []
        self._total_forecasts = 0

    def _record(self, key: str, value: float) -> None:
        if key not in self._series:
            self._series[key] = []
        self._series[key].append(value)
        if len(self._series[key]) > 200:
            self._series[key] = self._series[key][-200:]

    def _predict_value(self, key: str, steps: int) -> float:
        values = self._series.get(key, [])
        if not values:
            return 0.0
        smoothed = values[0]
        trend = 0.0
        for v in values[1:]:
            prev = smoothed
            smoothed = self._alpha * v + (1 - self._alpha) * (smoothed + trend)
            trend = 0.1 * (smoothed - prev) + 0.9 * trend
        return max(0.0, smoothed + trend * steps)

    def forecast(self, telemetry: Dict[str, Any], horizon_minutes: int = 30) -> List[GPUDemandForecast]:
        results: List[GPUDemandForecast] = []
        ts = now_ts()
        steps = max(1, horizon_minutes // 5)

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            gpu_types = r.get("gpu_types", ["generic"])
            total_gpus = r.get("total_gpus", 0)
            gpu_util = r.get("gpu_utilization", 0.0)

            for gpu_type in gpu_types:
                demand = gpu_util * total_gpus if total_gpus > 0 else gpu_util * 10
                key = f"gpu_demand:{gpu_type}:{region_id}"
                self._record(key, demand)

                predicted = self._predict_value(key, steps)
                recommended = max(total_gpus, int(predicted * 1.2) + 1)
                deficit = max(0, recommended - total_gpus)

                values = self._series.get(key, [])
                if len(values) >= 3:
                    trend = "increasing" if values[-1] > values[-3] * 1.1 else (
                        "decreasing" if values[-1] < values[-3] * 0.9 else "stable")
                else:
                    trend = "stable"

                confidence = min(0.9, 0.3 + len(values) * 0.01)

                results.append(GPUDemandForecast(
                    forecast_id=f"gpu-{gpu_type}-{region_id}-{int(ts)}",
                    region_id=region_id,
                    gpu_type=gpu_type,
                    current_demand=round(demand, 2),
                    predicted_demand=round(predicted, 2),
                    current_supply=total_gpus,
                    recommended_supply=recommended,
                    deficit=deficit,
                    confidence=round(confidence, 3),
                    horizon_minutes=horizon_minutes,
                    trend=trend,
                    forecasted_at=ts,
                ))

        with self._lock:
            self._forecasts.extend(results)
            if len(self._forecasts) > self._max_history:
                self._forecasts = self._forecasts[-self._max_history:]
            self._total_forecasts += len(results)

        return results

    def gpu_deficit_regions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in self._forecasts if f.deficit > 0]

    def recent_forecasts(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in reversed(self._forecasts)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            deficit_regions = sum(1 for f in self._forecasts if f.deficit > 0)
            return {
                "ts": now_ts(),
                "total_forecasts": self._total_forecasts,
                "deficit_regions": deficit_regions,
                "tracked_series": len(self._series),
            }
