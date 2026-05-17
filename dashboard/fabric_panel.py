"""v13 Global Fabric dashboard panel.

Extends the dashboard with world-map visualization endpoints,
regional traffic flow, edge node visualization, replication monitoring,
global failover visibility, routing heatmaps, regional GPU utilization,
and cross-region latency maps.
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

    @app.get("/api/v13/fabric/overview")
    def fabric_overview() -> Any:
        return jsonify({
            "global_metrics": _proxy("/global_metrics"),
            "region_metrics": _proxy("/region_metrics"),
            "routing_metrics": _proxy("/routing_metrics"),
        })

    @app.get("/api/v13/fabric/regions")
    def fabric_regions() -> Any:
        return jsonify(_proxy("/v13/regions"))

    @app.get("/api/v13/fabric/regions/<region_id>")
    def fabric_region_detail(region_id: str) -> Any:
        return jsonify(_proxy(f"/v13/regions/{region_id}"))

    @app.get("/api/v13/fabric/topology")
    def fabric_topology() -> Any:
        return jsonify(_proxy("/v13/topology"))

    @app.get("/api/v13/fabric/routing")
    def fabric_routing() -> Any:
        return jsonify({
            "routing_metrics": _proxy("/routing_metrics"),
            "routing_decisions": _proxy("/v13/routing/recent"),
        })

    @app.get("/api/v13/fabric/replication")
    def fabric_replication() -> Any:
        return jsonify({
            "replication_metrics": _proxy("/v13/replication/metrics"),
            "replication_events": _proxy("/v13/replication/events"),
        })

    @app.get("/api/v13/fabric/edge")
    def fabric_edge() -> Any:
        return jsonify(_proxy("/v13/edge/nodes"))

    @app.get("/api/v13/fabric/failover")
    def fabric_failover() -> Any:
        return jsonify({
            "failover_metrics": _proxy("/v13/failover/metrics"),
            "failover_events": _proxy("/v13/failover/events"),
        })

    @app.get("/api/v13/fabric/latency_map")
    def fabric_latency_map() -> Any:
        return jsonify(_proxy("/v13/latency_map"))

    @app.get("/api/v13/fabric/gpu_utilization")
    def fabric_gpu_utilization() -> Any:
        return jsonify(_proxy("/v13/gpu_utilization"))

    @app.get("/api/v13/fabric/cache")
    def fabric_cache() -> Any:
        return jsonify(_proxy("/v13/cache/metrics"))

    @app.get("/api/v13/fabric/network")
    def fabric_network() -> Any:
        return jsonify({
            "overlay_metrics": _proxy("/v13/network/overlay"),
            "bandwidth_matrix": _proxy("/v13/network/bandwidth"),
        })

    @app.get("/api/v13/fabric/workloads")
    def fabric_workloads() -> Any:
        return jsonify(_proxy("/v13/workloads"))

    @app.get("/api/v13/fabric/world_map")
    def fabric_world_map() -> Any:
        return jsonify({
            "regions": _proxy("/v13/regions"),
            "topology": _proxy("/v13/topology"),
            "latency_map": _proxy("/v13/latency_map"),
            "traffic_flow": _proxy("/v13/traffic_flow"),
        })
