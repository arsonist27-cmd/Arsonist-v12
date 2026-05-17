"""v15 Planetary dashboard panel.

Extends the dashboard with global topology globe, continental traffic maps,
infrastructure intelligence view, carbon efficiency graphs, planetary
workload visualization, and disaster recovery simulation controls.
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

    @app.get("/api/v15/planetary/overview")
    def planetary_overview() -> Any:
        return jsonify({
            "fabric_status": _proxy("/v15/fabric/status"),
            "planetary_metrics": _proxy("/v15/planetary/metrics"),
            "scheduler_metrics": _proxy("/v15/scheduler/metrics"),
            "graph_summary": _proxy("/v15/graph/summary"),
            "carbon_summary": _proxy("/v15/carbon/summary"),
        })

    @app.get("/api/v15/planetary/topology")
    def planetary_topology() -> Any:
        return jsonify({
            "graph_summary": _proxy("/v15/graph/summary"),
            "graph_metrics": _proxy("/v15/graph/metrics"),
            "hottest_nodes": _proxy("/v15/graph/hottest"),
            "most_utilized": _proxy("/v15/graph/most_utilized"),
            "congested_links": _proxy("/v15/graph/congested"),
        })

    @app.get("/api/v15/planetary/continental")
    def planetary_continental() -> Any:
        return jsonify({
            "continental_breakdown": _proxy("/v15/planetary/continental"),
            "failover_events": _proxy("/v15/failover/events"),
            "zone_summary": _proxy("/v15/zones/summary"),
        })

    @app.get("/api/v15/planetary/scheduler")
    def planetary_scheduler() -> Any:
        return jsonify({
            "scheduler_metrics": _proxy("/v15/scheduler/metrics"),
            "recent_decisions": _proxy("/v15/scheduler/decisions"),
            "recent_events": _proxy("/v15/scheduler/events"),
        })

    @app.get("/api/v15/planetary/runtime")
    def planetary_runtime() -> Any:
        return jsonify({
            "runtime_metrics": _proxy("/v15/runtime/metrics"),
            "queue_stats": _proxy("/v15/runtime/queue"),
            "streaming_metrics": _proxy("/v15/streaming/metrics"),
            "execution_metrics": _proxy("/v15/execution/metrics"),
        })

    @app.get("/api/v15/planetary/coordination")
    def planetary_coordination() -> Any:
        return jsonify({
            "decision_metrics": _proxy("/v15/decisions/metrics"),
            "pending_proposals": _proxy("/v15/decisions/pending"),
            "consensus_metrics": _proxy("/v15/consensus/metrics"),
            "policy_metrics": _proxy("/v15/policies/metrics"),
        })

    @app.get("/api/v15/planetary/carbon")
    def planetary_carbon() -> Any:
        return jsonify({
            "carbon_summary": _proxy("/v15/carbon/summary"),
            "carbon_metrics": _proxy("/v15/carbon/metrics"),
            "greenest_regions": _proxy("/v15/carbon/greenest"),
            "brownest_regions": _proxy("/v15/carbon/brownest"),
            "recent_placements": _proxy("/v15/carbon/placements"),
        })

    @app.get("/api/v15/planetary/energy")
    def planetary_energy() -> Any:
        return jsonify({
            "grid_summary": _proxy("/v15/energy/grid_summary"),
            "grid_recommendations": _proxy("/v15/energy/recommendations"),
            "best_batch_regions": _proxy("/v15/energy/best_batch"),
        })

    @app.get("/api/v15/planetary/cooling")
    def planetary_cooling() -> Any:
        return jsonify({
            "cooling_summary": _proxy("/v15/cooling/summary"),
            "hottest_regions": _proxy("/v15/cooling/hottest"),
            "coolest_regions": _proxy("/v15/cooling/coolest"),
            "cooling_recommendations": _proxy("/v15/cooling/recommendations"),
        })

    @app.get("/api/v15/planetary/intelligence")
    def planetary_intelligence() -> Any:
        return jsonify({
            "global_health": _proxy("/v15/intelligence/health"),
            "health_scores": _proxy("/v15/intelligence/scores"),
            "recent_insights": _proxy("/v15/intelligence/insights"),
            "critical_insights": _proxy("/v15/intelligence/critical"),
        })

    @app.get("/api/v15/planetary/failover")
    def planetary_failover() -> Any:
        return jsonify({
            "failover_metrics": _proxy("/v15/failover/metrics"),
            "active_events": _proxy("/v15/failover/active"),
            "recent_events": _proxy("/v15/failover/events"),
            "failover_log": _proxy("/v15/failover/log"),
        })

    @app.get("/api/v15/planetary/simulation")
    def planetary_simulation() -> Any:
        return jsonify({
            "simulation_metrics": _proxy("/v15/simulation/metrics"),
            "recent_results": _proxy("/v15/simulation/results"),
            "resilience_summary": _proxy("/v15/simulation/resilience"),
        })

    @app.get("/api/v15/planetary/load_tests")
    def planetary_load_tests() -> Any:
        return jsonify({
            "load_test_metrics": _proxy("/v15/loadtest/metrics"),
            "recent_results": _proxy("/v15/loadtest/results"),
        })

    @app.get("/api/v15/planetary/outage_tests")
    def planetary_outage_tests() -> Any:
        return jsonify({
            "outage_test_metrics": _proxy("/v15/outage/metrics"),
            "recent_results": _proxy("/v15/outage/results"),
            "pass_rate": _proxy("/v15/outage/pass_rate"),
        })

    @app.get("/api/v15/planetary/policies")
    def planetary_policies() -> Any:
        return jsonify({
            "all_policies": _proxy("/v15/policies/all"),
            "recent_evaluations": _proxy("/v15/policies/evaluations"),
            "policy_metrics": _proxy("/v15/policies/metrics"),
        })

    @app.get("/api/v15/planetary/geo")
    def planetary_geo() -> Any:
        return jsonify({
            "geo_metrics": _proxy("/v15/geo/metrics"),
            "recent_placements": _proxy("/v15/geo/placements"),
        })
