"""Regional failover test for Arsonist OS v13.

Tests automatic failover, recovery, cascading failures,
and workload migration during regional outages.

Usage:
    PYTHONPATH=$PWD python tests/regional_failover_test.py
"""

from __future__ import annotations

import json
import sys
import time

sys.path.insert(0, ".")

from regions.region_registry import GPUInventory, RegionRecord, RegionRegistry, RegionStatus, RegionType
from regions.region_health import RegionHealthMonitor
from regions.regional_capacity import RegionalCapacityTracker
from routing.smart_failover import SmartFailover, FailoverTrigger
from fabric.compute_fabric import ComputeFabric
from fabric.placement_engine import PlacementRequest
from shared.utils import setup_logging

logger = setup_logging("test.failover")


def create_test_regions() -> RegionRegistry:
    registry = RegionRegistry(db_path=":memory:")
    regions = [
        RegionRecord(
            region_id="primary",
            display_name="Primary Region",
            geographic_location="US East",
            region_type=RegionType.cloud,
            capacity=1.0,
            gpu_inventory=GPUInventory(total_gpus=32, available_gpus=20, total_vram_gb=2560, available_vram_gb=1600),
            workload_saturation=0.40,
            failover_target="secondary",
            failover_priority=0,
        ),
        RegionRecord(
            region_id="secondary",
            display_name="Secondary Region",
            geographic_location="EU West",
            region_type=RegionType.cloud,
            capacity=0.9,
            gpu_inventory=GPUInventory(total_gpus=24, available_gpus=18, total_vram_gb=1920, available_vram_gb=1440),
            workload_saturation=0.30,
            failover_target="tertiary",
            failover_priority=1,
        ),
        RegionRecord(
            region_id="tertiary",
            display_name="Tertiary Region",
            geographic_location="AP East",
            region_type=RegionType.cloud,
            capacity=0.7,
            gpu_inventory=GPUInventory(total_gpus=16, available_gpus=12, total_vram_gb=1280, available_vram_gb=960),
            workload_saturation=0.20,
            failover_priority=2,
        ),
    ]
    for r in regions:
        registry.register(r)
    return registry


def test_basic_failover() -> None:
    logger.info("\n=== TEST: Basic Regional Failover ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    failover = SmartFailover(registry, capacity)

    registry.update_status("primary", RegionStatus.offline)
    logger.info("  Primary region marked OFFLINE")

    events = failover.check_all()
    assert len(events) > 0, "Expected failover event"
    event = events[0]
    assert event.source_region == "primary"
    assert event.target_region == "secondary"
    assert event.trigger == FailoverTrigger.region_outage
    logger.info("  Failover: %s -> %s (%s) ✓", event.source_region, event.target_region, event.trigger.value)

    primary = registry.get("primary")
    assert primary is not None and primary.status == RegionStatus.draining
    logger.info("  Primary status: %s ✓", primary.status.value)
    logger.info("  PASSED")


def test_failover_recovery() -> None:
    logger.info("\n=== TEST: Failover Recovery ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    failover = SmartFailover(registry, capacity)

    registry.update_status("primary", RegionStatus.offline)
    failover.check_all()

    result = failover.recover_region("primary")
    assert result is not None
    assert result.status == RegionStatus.active
    logger.info("  Primary recovered to: %s ✓", result.status.value)
    logger.info("  PASSED")


def test_latency_spike_failover() -> None:
    logger.info("\n=== TEST: Latency Spike Failover ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    failover = SmartFailover(registry, capacity)

    registry.heartbeat("primary", {"avg_latency_ms": 2000.0})
    logger.info("  Primary latency spiked to 2000ms")

    events = failover.check_all()
    assert len(events) > 0, "Expected failover on latency spike"
    assert events[0].trigger == FailoverTrigger.latency_spike
    logger.info("  Failover triggered by latency spike ✓")
    logger.info("  PASSED")


def test_gpu_exhaustion_failover() -> None:
    logger.info("\n=== TEST: GPU Exhaustion Failover ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    failover = SmartFailover(registry, capacity)

    registry.heartbeat("primary", {
        "gpu_inventory": GPUInventory(total_gpus=32, available_gpus=0, total_vram_gb=2560, available_vram_gb=0),
        "workload_saturation": 0.98,
    })
    logger.info("  Primary GPUs exhausted")

    events = failover.check_all()
    assert len(events) > 0, "Expected failover on GPU exhaustion"
    assert events[0].trigger == FailoverTrigger.gpu_exhaustion
    logger.info("  Failover triggered by GPU exhaustion ✓")
    logger.info("  PASSED")


def test_cascading_failover() -> None:
    logger.info("\n=== TEST: Cascading Failover ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    failover = SmartFailover(registry, capacity)

    registry.update_status("primary", RegionStatus.offline)
    events1 = failover.check_all()
    assert len(events1) > 0
    logger.info("  First failover: primary -> %s ✓", events1[0].target_region)

    registry.update_status("secondary", RegionStatus.offline)
    events2 = failover.check_all()
    found_secondary = [e for e in events2 if e.source_region == "secondary"]
    assert len(found_secondary) > 0, "Expected cascading failover from secondary"
    logger.info("  Cascading failover: secondary -> %s ✓", found_secondary[0].target_region)
    logger.info("  PASSED")


def test_workload_migration_on_failover() -> None:
    logger.info("\n=== TEST: Workload Migration on Failover ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    fabric = ComputeFabric(registry, capacity)

    for i in range(5):
        fabric.submit_workload(PlacementRequest(
            workload_id=f"wl-{i}",
            require_gpu=True,
            preferred_region="primary",
        ))

    primary_wl = fabric.workloads_in_region("primary")
    logger.info("  Workloads in primary: %d", len(primary_wl))

    migrated = fabric.drain_region("primary", "secondary")
    logger.info("  Migrated %d workloads to secondary", migrated)

    after_primary = fabric.workloads_in_region("primary")
    after_secondary = fabric.workloads_in_region("secondary")
    logger.info("  Primary remaining: %d, Secondary gained: %d", len(after_primary), len(after_secondary))
    assert len(after_primary) == 0, "Primary should be drained"
    logger.info("  PASSED")


def test_manual_failover() -> None:
    logger.info("\n=== TEST: Manual Failover ===")
    registry = create_test_regions()
    capacity = RegionalCapacityTracker(registry)
    failover = SmartFailover(registry, capacity)

    event = failover.trigger_manual_failover("primary", "tertiary")
    assert event is not None
    assert event.source_region == "primary"
    assert event.target_region == "tertiary"
    assert event.trigger == FailoverTrigger.manual
    logger.info("  Manual failover: primary -> tertiary ✓")

    metrics = failover.metrics()
    assert metrics["total_failovers"] == 1
    logger.info("  Failover metrics: %s ✓", json.dumps(metrics, indent=2))
    logger.info("  PASSED")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Arsonist OS v13 — Regional Failover Tests")
    logger.info("=" * 60)

    tests = [
        test_basic_failover,
        test_failover_recovery,
        test_latency_spike_failover,
        test_gpu_exhaustion_failover,
        test_cascading_failover,
        test_workload_migration_on_failover,
        test_manual_failover,
    ]

    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            logger.error("  FAILED: %s — %s", test_fn.__name__, e)
            failed += 1

    logger.info("\n" + "=" * 60)
    logger.info("Results: %d passed, %d failed out of %d tests", passed, failed, len(tests))
    logger.info("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
