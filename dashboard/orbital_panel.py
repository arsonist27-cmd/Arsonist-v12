"""v16 Orbital dashboard panel.

Extends the dashboard with orbital visualization, disconnected operation
status, partition recovery status, delay-tolerant queue monitoring,
communication mesh health, and interplanetary simulation controls.
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

    @app.get("/api/v16/orbital/overview")
    def orbital_overview() -> Any:
        return jsonify({
            "scheduler_metrics": _proxy("/v16/orbital/scheduler/metrics"),
            "registry_metrics": _proxy("/v16/orbital/registry/metrics"),
            "routing_metrics": _proxy("/v16/orbital/routing/metrics"),
            "failover_metrics": _proxy("/v16/orbital/failover/metrics"),
            "mesh_metrics": _proxy("/v16/communications/mesh/metrics"),
            "queue_metrics": _proxy("/v16/deep_space/queue/metrics"),
        })

    @app.get("/api/v16/orbital/nodes")
    def orbital_nodes() -> Any:
        return jsonify({
            "node_summary": _proxy("/v16/orbital/registry/summary"),
            "active_nodes": _proxy("/v16/orbital/registry/active"),
            "disconnected_nodes": _proxy("/v16/orbital/registry/disconnected"),
            "orbital_nodes": _proxy("/v16/orbital/registry/orbital"),
            "ground_nodes": _proxy("/v16/orbital/registry/ground"),
        })

    @app.get("/api/v16/orbital/scheduler")
    def orbital_scheduler() -> Any:
        return jsonify({
            "scheduler_metrics": _proxy("/v16/orbital/scheduler/metrics"),
            "recent_decisions": _proxy("/v16/orbital/scheduler/decisions"),
            "recent_events": _proxy("/v16/orbital/scheduler/events"),
        })

    @app.get("/api/v16/orbital/routing")
    def orbital_routing() -> Any:
        return jsonify({
            "routing_metrics": _proxy("/v16/orbital/routing/metrics"),
            "active_routes": _proxy("/v16/orbital/routing/active"),
            "recent_decisions": _proxy("/v16/orbital/routing/decisions"),
        })

    @app.get("/api/v16/orbital/failover")
    def orbital_failover() -> Any:
        return jsonify({
            "failover_metrics": _proxy("/v16/orbital/failover/metrics"),
            "active_failovers": _proxy("/v16/orbital/failover/active"),
            "recent_completed": _proxy("/v16/orbital/failover/completed"),
            "recent_events": _proxy("/v16/orbital/failover/events"),
        })

    @app.get("/api/v16/orbital/delay_tolerant")
    def orbital_delay_tolerant() -> Any:
        return jsonify({
            "queue_metrics": _proxy("/v16/deep_space/queue/metrics"),
            "replication_metrics": _proxy("/v16/deep_space/replication/metrics"),
            "consensus_metrics": _proxy("/v16/deep_space/consensus/metrics"),
            "router_metrics": _proxy("/v16/deep_space/router/metrics"),
        })

    @app.get("/api/v16/orbital/partitions")
    def orbital_partitions() -> Any:
        return jsonify({
            "partition_metrics": _proxy("/v16/resilience/partition/metrics"),
            "active_partitions": _proxy("/v16/resilience/partition/active"),
            "recovery_metrics": _proxy("/v16/resilience/recovery/metrics"),
            "active_recoveries": _proxy("/v16/resilience/recovery/active"),
            "unresolved_conflicts": _proxy("/v16/resilience/recovery/conflicts"),
        })

    @app.get("/api/v16/orbital/disconnected")
    def orbital_disconnected() -> Any:
        return jsonify({
            "operations_state": _proxy("/v16/resilience/disconnected/state"),
            "operations_metrics": _proxy("/v16/resilience/disconnected/metrics"),
            "unsynced_decisions": _proxy("/v16/resilience/disconnected/unsynced"),
            "peer_states": _proxy("/v16/resilience/disconnected/peers"),
        })

    @app.get("/api/v16/orbital/communications")
    def orbital_communications() -> Any:
        return jsonify({
            "mesh_metrics": _proxy("/v16/communications/mesh/metrics"),
            "link_status": _proxy("/v16/communications/mesh/links"),
            "healthy_links": _proxy("/v16/communications/mesh/healthy"),
            "bandwidth_metrics": _proxy("/v16/communications/bandwidth/metrics"),
            "latency_reference": _proxy("/v16/communications/latency/reference"),
        })

    @app.get("/api/v16/orbital/link_health")
    def orbital_link_health() -> Any:
        return jsonify({
            "link_reports": _proxy("/v16/telemetry/link_health/reports"),
            "link_metrics": _proxy("/v16/telemetry/link_health/metrics"),
            "link_events": _proxy("/v16/telemetry/link_health/events"),
        })

    @app.get("/api/v16/orbital/telemetry")
    def orbital_telemetry() -> Any:
        return jsonify({
            "latest_snapshot": _proxy("/v16/telemetry/orbital/latest"),
            "snapshot_history": _proxy("/v16/telemetry/orbital/history"),
            "orbital_metrics": _proxy("/v16/telemetry/orbital/metrics"),
            "failover_history": _proxy("/v16/telemetry/orbital/failovers"),
            "partition_history": _proxy("/v16/telemetry/orbital/partitions"),
        })

    @app.get("/api/v16/orbital/simulation")
    def orbital_simulation() -> Any:
        return jsonify({
            "orbital_sim_summary": _proxy("/v16/simulation/orbital/summary"),
            "orbital_sim_results": _proxy("/v16/simulation/orbital/results"),
            "partition_sim_summary": _proxy("/v16/simulation/partition/summary"),
            "partition_sim_results": _proxy("/v16/simulation/partition/results"),
            "delay_test_summary": _proxy("/v16/simulation/delay/summary"),
            "delay_test_results": _proxy("/v16/simulation/delay/results"),
        })

    @app.get("/api/v16/orbital/resilience")
    def orbital_resilience() -> Any:
        return jsonify({
            "recovery_metrics": _proxy("/v16/resilience/recovery/metrics"),
            "partition_metrics": _proxy("/v16/resilience/partition/metrics"),
            "disconnected_metrics": _proxy("/v16/resilience/disconnected/metrics"),
            "failover_metrics": _proxy("/v16/orbital/failover/metrics"),
            "mesh_metrics": _proxy("/v16/communications/mesh/metrics"),
        })
