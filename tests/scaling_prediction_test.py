"""Scaling prediction test suite for v14 infrastructure intelligence.

Tests traffic forecasting, scaling forecasts, GPU demand prediction,
and prediction engine accuracy.
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prediction.traffic_forecasting import TrafficForecaster
from prediction.scaling_forecast import ScalingForecaster, ScalingDirection
from prediction.gpu_demand_predictor import GPUDemandPredictor
from intelligence.prediction_engine import PredictionEngine


def _make_telemetry(rps=100.0, gpu_util=0.5, saturation=0.5, latency=50.0):
    return {
        "regions": [
            {
                "region_id": "us-east",
                "requests_per_second": rps,
                "gpu_utilization": gpu_util,
                "cpu_utilization": 0.4,
                "memory_utilization": 0.5,
                "bandwidth_utilization": 0.3,
                "workload_saturation": saturation,
                "avg_latency_ms": latency,
                "bandwidth_usage_mbps": 500,
                "total_gpus": 16,
                "gpu_types": ["A100"],
            },
            {
                "region_id": "eu-west",
                "requests_per_second": rps * 0.8,
                "gpu_utilization": gpu_util * 0.9,
                "cpu_utilization": 0.35,
                "memory_utilization": 0.45,
                "bandwidth_utilization": 0.25,
                "workload_saturation": saturation * 0.9,
                "avg_latency_ms": latency * 1.2,
                "bandwidth_usage_mbps": 400,
                "total_gpus": 12,
                "gpu_types": ["A100"],
            },
        ]
    }


def test_traffic_forecasting_basic():
    forecaster = TrafficForecaster()
    telemetry = _make_telemetry(rps=100)
    forecasts = forecaster.forecast(telemetry)
    assert len(forecasts) == 2, f"Expected 2 forecasts (one per region), got {len(forecasts)}"
    for f in forecasts:
        assert f.region_id, "Expected region_id"
        assert f.current_rps > 0, "Expected current_rps > 0"
        assert f.predicted_rps >= 0, "Expected predicted_rps >= 0"
        assert f.confidence > 0, "Expected confidence > 0"
    print("  PASS: test_traffic_forecasting_basic")


def test_traffic_forecasting_trend():
    forecaster = TrafficForecaster()
    for rps in [50, 60, 70, 80, 90, 100, 120, 140, 160, 180]:
        forecaster.forecast(_make_telemetry(rps=rps))
    forecasts = forecaster.forecast(_make_telemetry(rps=200))
    us_east = [f for f in forecasts if f.region_id == "us-east"][0]
    assert us_east.trend == "increasing", f"Expected increasing trend, got {us_east.trend}"
    assert us_east.predicted_rps > us_east.current_rps, "Expected predicted > current for increasing trend"
    print("  PASS: test_traffic_forecasting_trend")


def test_scaling_forecast_scale_up():
    forecaster = ScalingForecaster()
    for i in range(15):
        util = 0.5 + i * 0.03
        forecaster.forecast(_make_telemetry(gpu_util=min(util, 0.95)))
    forecasts = forecaster.forecast(_make_telemetry(gpu_util=0.92))
    gpu_forecasts = [f for f in forecasts if f.resource_type == "gpu"]
    assert any(f.direction == ScalingDirection.scale_up for f in gpu_forecasts), \
        f"Expected scale_up for high GPU util, got {[f.direction.value for f in gpu_forecasts]}"
    print("  PASS: test_scaling_forecast_scale_up")


def test_scaling_forecast_scale_down():
    forecaster = ScalingForecaster()
    for i in range(15):
        util = 0.5 - i * 0.02
        forecaster.forecast(_make_telemetry(gpu_util=max(util, 0.1)))
    forecasts = forecaster.forecast(_make_telemetry(gpu_util=0.1))
    gpu_forecasts = [f for f in forecasts if f.resource_type == "gpu"]
    assert any(f.direction == ScalingDirection.scale_down for f in gpu_forecasts), \
        f"Expected scale_down for low GPU util, got {[f.direction.value for f in gpu_forecasts]}"
    print("  PASS: test_scaling_forecast_scale_down")


def test_gpu_demand_predictor():
    predictor = GPUDemandPredictor()
    for util in [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]:
        predictor.forecast(_make_telemetry(gpu_util=util))
    forecasts = predictor.forecast(_make_telemetry(gpu_util=0.92))
    assert len(forecasts) > 0, "Expected GPU demand forecasts"
    for f in forecasts:
        assert f.current_demand >= 0, "Expected non-negative demand"
        assert f.predicted_demand >= 0, "Expected non-negative predicted demand"
    m = predictor.metrics()
    assert m["total_forecasts"] > 0
    print("  PASS: test_gpu_demand_predictor")


def test_prediction_engine():
    engine = PredictionEngine()
    for i in range(10):
        engine.predict(_make_telemetry(latency=50 + i * 5, saturation=0.4 + i * 0.03))
    predictions = engine.predict(_make_telemetry(latency=100, saturation=0.7))
    assert len(predictions) > 0, "Expected predictions"
    for p in predictions:
        assert p.metric_name, "Expected metric_name"
        assert p.confidence > 0, "Expected confidence > 0"
    m = engine.metrics()
    assert m["total_predictions"] > 0
    assert m["tracked_series"] > 0
    print("  PASS: test_prediction_engine")


def test_scaling_forecast_warm_standby():
    forecaster = ScalingForecaster()
    for _ in range(15):
        forecaster.forecast(_make_telemetry(gpu_util=0.92))
    forecasts = forecaster.forecast(_make_telemetry(gpu_util=0.95))
    scale_ups = [f for f in forecasts if f.direction == ScalingDirection.scale_up]
    for su in scale_ups:
        assert su.warm_standby_count >= 0, "Expected warm_standby_count >= 0"
    events = forecaster.recent_pre_scale_events()
    assert len(events) > 0, "Expected pre-scale events"
    print("  PASS: test_scaling_forecast_warm_standby")


if __name__ == "__main__":
    tests = [
        test_traffic_forecasting_basic,
        test_traffic_forecasting_trend,
        test_scaling_forecast_scale_up,
        test_scaling_forecast_scale_down,
        test_gpu_demand_predictor,
        test_prediction_engine,
        test_scaling_forecast_warm_standby,
    ]
    print(f"Running {len(tests)} scaling prediction tests...")
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {t.__name__}: {e}")
    print(f"\n{passed}/{len(tests)} tests passed")
    sys.exit(0 if passed == len(tests) else 1)
