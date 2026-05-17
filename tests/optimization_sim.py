"""Optimization simulation for v14 infrastructure intelligence.

Simulates the full v14 intelligence loop: telemetry analysis, anomaly detection,
prediction, optimization, thermal balancing, cost optimization, adaptive scaling,
dynamic routing, learning, and autonomous healing.
"""
from __future__ import annotations

import sys
import os
import time
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from intelligence.optimization_engine import OptimizationEngine
from intelligence.anomaly_detector import AnomalyDetector
from intelligence.prediction_engine import PredictionEngine
from intelligence.recommendation_engine import RecommendationEngine
from intelligence.workload_analyzer import WorkloadAnalyzer
from optimization.gpu_optimizer import GPUOptimizer
from optimization.thermal_balancer import ThermalBalancer
from optimization.cost_optimizer import CostOptimizer
from optimization.energy_scheduler import EnergyScheduler
from adaptation.dynamic_routing import DynamicRoutingAdapter
from adaptation.adaptive_scaling import AdaptiveScaler
from adaptation.topology_optimizer import TopologyOptimizer
from learning.telemetry_learning import TelemetryLearner
from learning.workload_patterns import WorkloadPatternAnalyzer
from learning.historical_optimizer import HistoricalOptimizer
from repair.auto_healing import AutoHealingSystem


REGIONS = [
    {
        "region_id": "us-east",
        "avg_latency_ms": 25, "gpu_utilization": 0.7, "gpu_temp_c": 72,
        "workload_saturation": 0.65, "total_gpus": 32, "gpu_types": ["A100", "H100"],
        "requests_per_second": 500, "cpu_utilization": 0.55, "memory_utilization": 0.6,
        "bandwidth_utilization": 0.4, "bandwidth_usage_mbps": 800,
        "power_consumption_w": 12000, "energy_cost_per_kwh": 0.12,
        "renewable_pct": 0.3, "gpu_cost_per_hour": 2.50, "bandwidth_cost_per_gb": 0.08,
        "cost_per_hour": 80.0, "error_rate": 0.01, "worker_count": 20, "replica_count": 5,
        "thermal_pressure": 0.4, "replication_overhead_pct": 0.08,
        "workloads": [{"workload_id": f"wl-us-{i}"} for i in range(8)],
    },
    {
        "region_id": "eu-west",
        "avg_latency_ms": 45, "gpu_utilization": 0.92, "gpu_temp_c": 88,
        "workload_saturation": 0.88, "total_gpus": 24, "gpu_types": ["A100"],
        "requests_per_second": 400, "cpu_utilization": 0.8, "memory_utilization": 0.75,
        "bandwidth_utilization": 0.7, "bandwidth_usage_mbps": 600,
        "power_consumption_w": 10000, "energy_cost_per_kwh": 0.18,
        "renewable_pct": 0.6, "gpu_cost_per_hour": 3.00, "bandwidth_cost_per_gb": 0.10,
        "cost_per_hour": 72.0, "error_rate": 0.03, "worker_count": 15, "replica_count": 4,
        "thermal_pressure": 0.75, "replication_overhead_pct": 0.12,
        "workloads": [{"workload_id": f"wl-eu-{i}"} for i in range(6)],
    },
    {
        "region_id": "ap-south",
        "avg_latency_ms": 80, "gpu_utilization": 0.25, "gpu_temp_c": 58,
        "workload_saturation": 0.3, "total_gpus": 16, "gpu_types": ["A100"],
        "requests_per_second": 150, "cpu_utilization": 0.25, "memory_utilization": 0.3,
        "bandwidth_utilization": 0.2, "bandwidth_usage_mbps": 200,
        "power_consumption_w": 5000, "energy_cost_per_kwh": 0.08,
        "renewable_pct": 0.7, "gpu_cost_per_hour": 1.80, "bandwidth_cost_per_gb": 0.05,
        "cost_per_hour": 30.0, "error_rate": 0.005, "worker_count": 10, "replica_count": 3,
        "thermal_pressure": 0.2, "replication_overhead_pct": 0.05,
        "workloads": [{"workload_id": f"wl-ap-{i}"} for i in range(3)],
    },
    {
        "region_id": "sa-east",
        "avg_latency_ms": 120, "gpu_utilization": 0.4, "gpu_temp_c": 65,
        "workload_saturation": 0.45, "total_gpus": 8, "gpu_types": ["A100"],
        "requests_per_second": 80, "cpu_utilization": 0.35, "memory_utilization": 0.4,
        "bandwidth_utilization": 0.3, "bandwidth_usage_mbps": 150,
        "power_consumption_w": 3000, "energy_cost_per_kwh": 0.10,
        "renewable_pct": 0.8, "gpu_cost_per_hour": 1.50, "bandwidth_cost_per_gb": 0.06,
        "cost_per_hour": 15.0, "error_rate": 0.02, "worker_count": 6, "replica_count": 2,
        "thermal_pressure": 0.3, "replication_overhead_pct": 0.06,
        "workloads": [{"workload_id": f"wl-sa-{i}"} for i in range(2)],
    },
]

LATENCY_MAP = {
    "us-east": {"eu-west": 85, "ap-south": 180, "sa-east": 120},
    "eu-west": {"us-east": 85, "ap-south": 150, "sa-east": 200},
    "ap-south": {"us-east": 180, "eu-west": 150, "sa-east": 250},
    "sa-east": {"us-east": 120, "eu-west": 200, "ap-south": 250},
}


def _build_telemetry(regions=None, inject_failures=False):
    regs = regions or REGIONS
    telemetry = {
        "regions": regs,
        "latency_map": LATENCY_MAP,
        "nodes": [],
        "deployments": [],
        "services": [],
        "replicas": [],
    }

    if inject_failures:
        telemetry["deployments"].append({
            "deployment_id": "deploy-fail-1", "status": "failed",
            "error_rate": 0.9, "region_id": "eu-west",
        })
        telemetry["nodes"].append({
            "node_id": "node-unstable", "status": "healthy",
            "restart_count_1h": 8, "region_id": "eu-west", "cpu_usage": 0.5,
        })
        telemetry["services"].append({
            "service_id": "svc-crash", "status": "crashed", "region_id": "eu-west",
        })

    return telemetry


def run_simulation():
    print("=" * 70)
    print("ARSONIST OS v14 — OPTIMIZATION SIMULATION")
    print("=" * 70)

    opt_engine = OptimizationEngine()
    anomaly_detector = AnomalyDetector()
    prediction_engine = PredictionEngine()
    recommendation_engine = RecommendationEngine()
    workload_analyzer = WorkloadAnalyzer()
    gpu_optimizer = GPUOptimizer()
    thermal_balancer = ThermalBalancer()
    cost_optimizer = CostOptimizer()
    energy_scheduler = EnergyScheduler()
    dynamic_router = DynamicRoutingAdapter()
    adaptive_scaler = AdaptiveScaler()
    topology_optimizer = TopologyOptimizer()
    telemetry_learner = TelemetryLearner()
    workload_patterns = WorkloadPatternAnalyzer()
    historical_optimizer = HistoricalOptimizer()
    auto_healer = AutoHealingSystem()

    num_rounds = 5
    start = time.time()

    for round_num in range(1, num_rounds + 1):
        print(f"\n--- Round {round_num}/{num_rounds} ---")
        inject = round_num >= 3

        regions = []
        for r in REGIONS:
            region = dict(r)
            region["avg_latency_ms"] += random.uniform(-10, 15)
            region["gpu_utilization"] = min(1.0, max(0.0, region["gpu_utilization"] + random.uniform(-0.05, 0.05)))
            region["workload_saturation"] = min(1.0, max(0.0, region["workload_saturation"] + random.uniform(-0.03, 0.03)))
            region["gpu_temp_c"] += random.uniform(-3, 5)
            region["requests_per_second"] += random.uniform(-20, 30)
            regions.append(region)

        if inject and round_num == 3:
            regions[1]["avg_latency_ms"] = 600
            regions[1]["gpu_utilization"] = 0.98
            regions[1]["gpu_temp_c"] = 93
            regions[1]["workload_saturation"] = 0.95

        telemetry = _build_telemetry(regions, inject_failures=inject)

        # 1. Optimization engine
        opt_actions = opt_engine.run_optimization_loop(telemetry)
        print(f"  Optimization: {len(opt_actions)} actions")

        # 2. Anomaly detection
        anomalies = anomaly_detector.detect(telemetry)
        print(f"  Anomalies detected: {len(anomalies)}")

        # 3. Predictions
        predictions = prediction_engine.predict(telemetry)
        print(f"  Predictions generated: {len(predictions)}")

        # 4. Recommendations
        ineff = opt_engine.recent_inefficiencies(20)
        recs_ineff = recommendation_engine.generate_from_inefficiencies(ineff)
        pred_dicts = [p.model_dump(mode="json") for p in predictions if p.recommendation]
        recs_pred = recommendation_engine.generate_from_predictions(pred_dicts)
        print(f"  Recommendations: {len(recs_ineff)} from inefficiencies, {len(recs_pred)} from predictions")

        # 5. Workload analysis
        workloads = []
        for r in regions:
            for w in r.get("workloads", []):
                workloads.append({
                    **w,
                    "region_id": r["region_id"],
                    "gpu_usage": r["gpu_utilization"],
                    "cpu_usage": r.get("cpu_utilization", 0.5),
                    "latency_ms": r["avg_latency_ms"],
                    "error_rate": r.get("error_rate", 0.01),
                    "memory_mb": 2048,
                })
        profiles = workload_analyzer.analyze(workloads)
        print(f"  Workload profiles: {len(profiles)}")

        # 6. GPU optimization
        gpu_actions = gpu_optimizer.optimize(telemetry)
        print(f"  GPU optimizations: {len(gpu_actions)}")

        # 7. Thermal balancing
        thermal_actions = thermal_balancer.balance(telemetry)
        print(f"  Thermal actions: {len(thermal_actions)}")

        # 8. Cost optimization
        cost_actions = cost_optimizer.optimize(telemetry)
        print(f"  Cost optimizations: {len(cost_actions)}")

        # 9. Energy scheduling
        energy_actions = energy_scheduler.schedule(telemetry)
        print(f"  Energy actions: {len(energy_actions)}")

        # 10. Dynamic routing / migration
        migrations = dynamic_router.migrate(telemetry)
        print(f"  Workload migrations: {len(migrations)}")

        # 11. Adaptive scaling
        scale_actions = adaptive_scaler.scale(telemetry)
        print(f"  Scale actions: {len(scale_actions)}")

        # 12. Topology optimization
        topo_actions = topology_optimizer.optimize(telemetry)
        print(f"  Topology optimizations: {len(topo_actions)}")

        # 13. Telemetry learning
        patterns = telemetry_learner.learn(telemetry)
        actionable = telemetry_learner.actionable_insights()
        print(f"  Learned patterns: {len(patterns)}, actionable: {len(actionable)}")

        # 14. Workload pattern analysis
        wp_results = workload_patterns.analyze(workloads)
        print(f"  Workload patterns: {len(wp_results)}")

        # 15. Historical optimization
        hist_insights = historical_optimizer.analyze(telemetry)
        print(f"  Historical insights: {len(hist_insights)}")

        # 16. Autonomous healing (on failure rounds)
        if inject:
            heal_results = auto_healer.heal(telemetry)
            print(f"  Healing actions: {len(heal_results)}")

    elapsed = time.time() - start

    print("\n" + "=" * 70)
    print("SIMULATION RESULTS")
    print("=" * 70)
    print(f"Total simulation time: {elapsed:.2f}s ({num_rounds} rounds)")
    print(f"Avg round time: {elapsed / num_rounds * 1000:.1f}ms")
    print()

    print("Optimization Engine:", opt_engine.metrics())
    print("Anomaly Detector:", anomaly_detector.metrics())
    print("Prediction Engine:", prediction_engine.metrics())
    print("Recommendation Engine:", recommendation_engine.metrics())
    print("Workload Analyzer:", workload_analyzer.metrics())
    print("GPU Optimizer:", gpu_optimizer.metrics())
    print("Thermal Balancer:", thermal_balancer.metrics())
    print("Cost Optimizer:", cost_optimizer.metrics())
    print("Energy Scheduler:", energy_scheduler.metrics())
    print("Dynamic Router:", dynamic_router.metrics())
    print("Adaptive Scaler:", adaptive_scaler.metrics())
    print("Topology Optimizer:", topology_optimizer.metrics())
    print("Telemetry Learner:", telemetry_learner.metrics())
    print("Workload Patterns:", workload_patterns.metrics())
    print("Historical Optimizer:", historical_optimizer.metrics())
    print("Auto Healer:", auto_healer.metrics())

    # Verification assertions
    assert opt_engine.metrics()["total_optimizations"] > 0, "Expected optimizations"
    assert anomaly_detector.metrics()["total_detected"] > 0, "Expected anomalies detected"
    assert prediction_engine.metrics()["total_predictions"] > 0, "Expected predictions"
    assert thermal_balancer.metrics()["total_rebalances"] > 0, "Expected thermal rebalances"
    assert cost_optimizer.metrics()["total_optimizations"] > 0, "Expected cost optimizations"
    assert auto_healer.metrics()["total_healed"] > 0, "Expected healing actions"
    assert telemetry_learner.metrics()["tracked_series"] > 0, "Expected tracked series"

    print("\nAll simulation assertions passed!")
    print("=" * 70)


if __name__ == "__main__":
    try:
        run_simulation()
        sys.exit(0)
    except Exception as e:
        print(f"\nSIMULATION FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
