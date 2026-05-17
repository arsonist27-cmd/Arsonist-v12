"""Autonomous healing test suite for v14 infrastructure intelligence.

Tests auto-healing detection, recovery, rollback, failure recovery with
retry/escalation, workload rebuilding, and deployment repair.
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from repair.auto_healing import AutoHealingSystem, HealingStatus
from repair.failure_recovery import FailureRecoveryManager, RecoveryStatus
from repair.workload_rebuilder import WorkloadRebuilder, RebuildStatus
from repair.deployment_repair import DeploymentRepairManager, RepairStatus


def _make_telemetry(**overrides):
    base = {
        "deployments": [
            {"deployment_id": "deploy-1", "status": "running", "error_rate": 0.01, "region_id": "us-east"},
            {"deployment_id": "deploy-2", "status": "failed", "error_rate": 0.9, "region_id": "eu-west"},
        ],
        "nodes": [
            {"node_id": "node-1", "status": "healthy", "restart_count_1h": 0, "region_id": "us-east", "cpu_usage": 0.5},
            {"node_id": "node-2", "status": "failed", "restart_count_1h": 10, "region_id": "eu-west", "cpu_usage": 0.3},
            {"node_id": "node-3", "status": "healthy", "restart_count_1h": 0, "region_id": "ap-south", "cpu_usage": 0.98},
        ],
        "services": [
            {"service_id": "svc-1", "status": "running", "region_id": "us-east"},
            {"service_id": "svc-2", "status": "crashed", "region_id": "eu-west"},
        ],
        "replicas": [
            {"replica_id": "replica-1", "status": "healthy", "region_id": "us-east"},
            {"replica_id": "replica-2", "status": "lost", "region_id": "eu-west"},
        ],
        "regions": [
            {"region_id": "us-east", "error_rate": 0.02},
            {"region_id": "eu-west", "error_rate": 0.85},
        ],
    }
    base.update(overrides)
    return base


def test_auto_healing_detect_failures():
    healer = AutoHealingSystem()
    telemetry = _make_telemetry()
    actions = healer.detect_failures(telemetry)
    action_types = [a.action_type.value for a in actions]
    assert "restart_deployment" in action_types, f"Expected restart_deployment, got {action_types}"
    assert "replace_node" in action_types, f"Expected replace_node, got {action_types}"
    assert "rebuild_service" in action_types, f"Expected rebuild_service, got {action_types}"
    assert "restore_replica" in action_types, f"Expected restore_replica, got {action_types}"
    assert "isolate_region" in action_types, f"Expected isolate_region, got {action_types}"
    print("  PASS: test_auto_healing_detect_failures")


def test_auto_healing_execute():
    healer = AutoHealingSystem()
    telemetry = _make_telemetry()
    results = healer.heal(telemetry)
    assert len(results) > 0, "Expected healing actions"
    for r in results:
        assert r.status == HealingStatus.completed, f"Expected completed, got {r.status}"
    m = healer.metrics()
    assert m["total_healed"] > 0, f"Expected healed count > 0, got {m['total_healed']}"
    print("  PASS: test_auto_healing_execute")


def test_auto_healing_rollback():
    healer = AutoHealingSystem()
    telemetry = _make_telemetry()
    results = healer.heal(telemetry)
    action_id = results[0].action_id
    ok = healer.rollback_action(action_id)
    assert ok, "Expected rollback to succeed"
    m = healer.metrics()
    assert m["total_rollbacks"] == 1, f"Expected 1 rollback, got {m['total_rollbacks']}"
    print("  PASS: test_auto_healing_rollback")


def test_failure_recovery_basic():
    mgr = FailureRecoveryManager()
    record = mgr.record_failure("node-x", "node", "crash", region_id="us-east", severity=0.5)
    assert record.failure_id, "Expected failure_id"
    assert record.strategy.value == "rebuild", f"Expected rebuild for crash, got {record.strategy.value}"
    recovered = mgr.recover(record.failure_id)
    assert recovered.status == RecoveryStatus.recovered
    m = mgr.metrics()
    assert m["total_recovered"] == 1
    print("  PASS: test_failure_recovery_basic")


def test_failure_recovery_escalation():
    mgr = FailureRecoveryManager(max_retries=1)
    record = mgr.record_failure("node-y", "node", "unknown", severity=0.3)
    result = mgr.retry(record.failure_id)
    assert result.status == RecoveryStatus.escalated, f"Expected escalated, got {result.status}"
    m = mgr.metrics()
    assert m["total_escalated"] == 1, f"Expected 1 escalated, got {m['total_escalated']}"
    print("  PASS: test_failure_recovery_escalation")


def test_workload_rebuilder():
    rebuilder = WorkloadRebuilder()
    req = rebuilder.request_rebuild("wl-1", region_id="eu-west", target_region="us-east", reason="region failure")
    assert req.status == RebuildStatus.pending
    result = rebuilder.execute_rebuild(req.rebuild_id)
    assert result.status == RebuildStatus.completed
    m = rebuilder.metrics()
    assert m["total_rebuilt"] == 1
    print("  PASS: test_workload_rebuilder")


def test_deployment_repair():
    mgr = DeploymentRepairManager()
    req = mgr.request_repair("deploy-x", region_id="eu-west", failure_reason="config error in manifest")
    assert req.action.value == "config_fix", f"Expected config_fix for config error, got {req.action.value}"
    result = mgr.execute_repair(req.repair_id)
    assert result.status == RepairStatus.repaired
    m = mgr.metrics()
    assert m["total_repaired"] == 1
    print("  PASS: test_deployment_repair")


def test_healing_priority_ordering():
    healer = AutoHealingSystem()
    telemetry = _make_telemetry()
    actions = healer.detect_failures(telemetry)
    priorities = [a.priority.value for a in actions]
    assert priorities[0] == "critical", f"Expected first action to be critical, got {priorities[0]}"
    print("  PASS: test_healing_priority_ordering")


if __name__ == "__main__":
    tests = [
        test_auto_healing_detect_failures,
        test_auto_healing_execute,
        test_auto_healing_rollback,
        test_failure_recovery_basic,
        test_failure_recovery_escalation,
        test_workload_rebuilder,
        test_deployment_repair,
        test_healing_priority_ordering,
    ]
    print(f"Running {len(tests)} autonomous healing tests...")
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {t.__name__}: {e}")
    print(f"\n{passed}/{len(tests)} tests passed")
    sys.exit(0 if passed == len(tests) else 1)
