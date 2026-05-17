"""v14 Intelligence dashboard panel.

Extends the dashboard with anomaly heatmaps, predictive scaling graphs,
infrastructure health scoring, GPU thermal maps, optimization recommendations,
repair event timeline, and AI optimization insights.
"""

from __future__ import annotations

from typing import Any, Dict

import requests
from flask import Flask, jsonify


def register(app: Flask, control_url: str, control_headers: Dict[str, str]) -> None:
    base = control_url.rstrip("/")

    def _proxy(path: str) -> Dict[str, Any]:
        try:
            r = requests.get(f"{base}{path}", headers=control_headers, timeout=4)
            return r.json() if r.ok else {"error": r.text, "status": r.status_code}
        except requests.RequestException as exc:
            return {"error": str(exc)}

    @app.get("/api/v14/intelligence/overview")
    def intelligence_overview() -> Any:
        return jsonify({
            "optimization_metrics": _proxy("/v14/optimization/metrics"),
            "anomaly_metrics": _proxy("/v14/anomalies/metrics"),
            "prediction_metrics": _proxy("/v14/predictions/metrics"),
            "healing_metrics": _proxy("/v14/healing/metrics"),
            "cost_metrics": _proxy("/v14/cost/metrics"),
        })

    @app.get("/api/v14/intelligence/anomalies")
    def intelligence_anomalies() -> Any:
        return jsonify({
            "anomalies": _proxy("/v14/anomalies/recent"),
            "heatmap": _proxy("/v14/anomalies/heatmap"),
        })

    @app.get("/api/v14/intelligence/predictions")
    def intelligence_predictions() -> Any:
        return jsonify({
            "scaling_forecasts": _proxy("/v14/predictions/scaling"),
            "traffic_forecasts": _proxy("/v14/predictions/traffic"),
            "gpu_demand": _proxy("/v14/predictions/gpu_demand"),
        })

    @app.get("/api/v14/intelligence/health")
    def intelligence_health() -> Any:
        return jsonify({
            "global_health_score": _proxy("/v14/health/score"),
            "region_health": _proxy("/v14/health/regions"),
            "component_health": _proxy("/v14/health/components"),
        })

    @app.get("/api/v14/intelligence/thermal")
    def intelligence_thermal() -> Any:
        return jsonify({
            "thermal_map": _proxy("/v14/thermal/map"),
            "hotspots": _proxy("/v14/thermal/hotspots"),
            "thermal_actions": _proxy("/v14/thermal/actions"),
        })

    @app.get("/api/v14/intelligence/cost")
    def intelligence_cost() -> Any:
        return jsonify({
            "cost_map": _proxy("/v14/cost/map"),
            "cost_actions": _proxy("/v14/cost/actions"),
            "cheapest_regions": _proxy("/v14/cost/cheapest"),
        })

    @app.get("/api/v14/intelligence/energy")
    def intelligence_energy() -> Any:
        return jsonify({
            "energy_map": _proxy("/v14/energy/map"),
            "green_regions": _proxy("/v14/energy/green"),
            "energy_actions": _proxy("/v14/energy/actions"),
        })

    @app.get("/api/v14/intelligence/optimization")
    def intelligence_optimization() -> Any:
        return jsonify({
            "recent_actions": _proxy("/v14/optimization/actions"),
            "inefficiencies": _proxy("/v14/optimization/inefficiencies"),
            "recommendations": _proxy("/v14/optimization/recommendations"),
        })

    @app.get("/api/v14/intelligence/healing")
    def intelligence_healing() -> Any:
        return jsonify({
            "active_actions": _proxy("/v14/healing/active"),
            "recent_actions": _proxy("/v14/healing/recent"),
            "repair_timeline": _proxy("/v14/healing/timeline"),
        })

    @app.get("/api/v14/intelligence/repair")
    def intelligence_repair() -> Any:
        return jsonify({
            "deployment_repairs": _proxy("/v14/repair/deployments"),
            "workload_rebuilds": _proxy("/v14/repair/workloads"),
            "failure_recoveries": _proxy("/v14/repair/failures"),
        })

    @app.get("/api/v14/intelligence/migration")
    def intelligence_migration() -> Any:
        return jsonify({
            "active_migrations": _proxy("/v14/migration/active"),
            "recent_migrations": _proxy("/v14/migration/recent"),
            "migration_metrics": _proxy("/v14/migration/metrics"),
        })

    @app.get("/api/v14/intelligence/scaling")
    def intelligence_scaling() -> Any:
        return jsonify({
            "scale_actions": _proxy("/v14/scaling/actions"),
            "scaling_metrics": _proxy("/v14/scaling/metrics"),
        })

    @app.get("/api/v14/intelligence/topology")
    def intelligence_topology() -> Any:
        return jsonify({
            "latency_matrix": _proxy("/v14/topology/latency"),
            "topology_actions": _proxy("/v14/topology/actions"),
        })

    @app.get("/api/v14/intelligence/learning")
    def intelligence_learning() -> Any:
        return jsonify({
            "patterns": _proxy("/v14/learning/patterns"),
            "actionable_insights": _proxy("/v14/learning/insights"),
            "workload_patterns": _proxy("/v14/learning/workloads"),
            "historical_insights": _proxy("/v14/learning/historical"),
        })

    @app.get("/api/v14/intelligence/gpu")
    def intelligence_gpu() -> Any:
        return jsonify({
            "gpu_optimization": _proxy("/v14/gpu/actions"),
            "gpu_metrics": _proxy("/v14/gpu/metrics"),
        })
