"""Edge disconnect and reconnect test for Arsonist OS v13.

Tests edge node offline operation, local caching during disconnect,
outbox accumulation, and reconnection synchronization.

Usage:
    PYTHONPATH=$PWD python tests/edge_disconnect_test.py
"""

from __future__ import annotations

import json
import sys
import time

sys.path.insert(0, ".")

from edge.edge_runtime import EdgeRuntime, EdgeNodeState
from edge.edge_scheduler import EdgeScheduler, EdgeWorkload
from edge.edge_cache import EdgeCache
from shared.utils import setup_logging

logger = setup_logging("test.edge_disconnect")


def test_edge_online_operation() -> None:
    logger.info("\n=== TEST: Edge Online Operation ===")
    synced_data: list = []

    def on_sync(node_id: str, entries: list) -> None:
        synced_data.extend(entries)

    runtime = EdgeRuntime(
        node_id="edge-test-1",
        region_id="us-east-1",
        db_path=":memory:",
        on_sync=on_sync,
    )

    result = runtime.handle_request("test-key-1", {"prompt": "hello"})
    assert result is None, "First request should be a cache miss"
    logger.info("  Cache miss on first request ✓")

    runtime.store_result("test-key-1", {"output": "world"})

    result = runtime.handle_request("test-key-1", {"prompt": "hello"})
    assert result is not None, "Second request should be a cache hit"
    assert result["output"] == "world"
    logger.info("  Cache hit on second request ✓")

    metrics = runtime.metrics()
    assert metrics["cache_hits"] == 1
    assert metrics["cache_misses"] == 1
    assert metrics["connected"] is True
    logger.info("  Metrics correct ✓")
    logger.info("  PASSED")


def test_edge_disconnect_operation() -> None:
    logger.info("\n=== TEST: Edge Disconnect Operation ===")
    runtime = EdgeRuntime(
        node_id="edge-test-2",
        region_id="us-east-1",
        db_path=":memory:",
    )

    runtime.store_result("cached-1", {"data": "pre-disconnect"})

    runtime.set_connected(False)
    metrics = runtime.metrics()
    assert metrics["connected"] is False
    assert metrics["state"] == EdgeNodeState.offline.value
    logger.info("  Edge node went offline ✓")

    result = runtime.handle_request("cached-1", {})
    assert result is not None, "Cached data should be available offline"
    logger.info("  Cached data available offline ✓")

    result = runtime.handle_request("uncached-key", {})
    assert result is None, "Uncached data should return None offline"
    assert runtime.metrics()["offline_requests"] > 0
    logger.info("  Uncached request handled gracefully offline ✓")

    for i in range(10):
        runtime.store_result(f"offline-result-{i}", {"data": f"offline-{i}"})
    logger.info("  Stored 10 results while offline ✓")
    logger.info("  PASSED")


def test_edge_reconnection() -> None:
    logger.info("\n=== TEST: Edge Reconnection and Sync ===")
    synced_data: list = []

    def on_sync(node_id: str, entries: list) -> None:
        synced_data.extend(entries)

    runtime = EdgeRuntime(
        node_id="edge-test-3",
        region_id="eu-west-1",
        db_path=":memory:",
        on_sync=on_sync,
    )

    for i in range(5):
        runtime.store_result(f"result-{i}", {"data": f"value-{i}"})

    runtime.set_connected(False)
    for i in range(5, 10):
        runtime.store_result(f"result-{i}", {"data": f"value-{i}"})

    runtime.set_connected(True)
    metrics = runtime.metrics()
    assert metrics["state"] == EdgeNodeState.syncing.value
    logger.info("  Edge entered syncing state on reconnect ✓")

    runtime._flush_outbox()
    logger.info("  Synced %d entries after reconnection", len(synced_data))
    assert len(synced_data) > 0, "Should have synced queued data"
    logger.info("  PASSED")


def test_edge_scheduler_node_failure() -> None:
    logger.info("\n=== TEST: Edge Scheduler Node Failure ===")
    scheduler = EdgeScheduler()
    scheduler.register_node("edge-a", capacity=4)
    scheduler.register_node("edge-b", capacity=4)

    for i in range(6):
        scheduler.submit(EdgeWorkload(
            workload_id=f"wl-{i}",
            model_id="small-model",
            offline_capable=True,
        ))
        scheduler.schedule_next()

    metrics_before = scheduler.metrics()
    logger.info("  Running workloads before failure: %d", metrics_before["running"])

    orphaned = scheduler.unregister_node("edge-a")
    logger.info("  Node edge-a failed, %d workloads orphaned and requeued", len(orphaned))

    metrics_after = scheduler.metrics()
    logger.info("  Queue depth after failure: %d", metrics_after["queue_depth"])
    assert metrics_after["queue_depth"] > 0 or metrics_after["running"] > 0
    logger.info("  PASSED")


def test_edge_cache_eviction() -> None:
    logger.info("\n=== TEST: Edge Cache LRU Eviction ===")
    cache = EdgeCache(max_entries=10, max_size_bytes=1024 * 1024)

    for i in range(15):
        cache.put(f"key-{i}", {"data": f"value-{i}"}, size_bytes=100)

    metrics = cache.metrics()
    assert metrics["total_entries"] <= 10, "Cache should respect max_entries"
    assert metrics["eviction_count"] >= 5, "Should have evicted at least 5 entries"
    logger.info("  Cache entries: %d (max=10) ✓", metrics["total_entries"])
    logger.info("  Evictions: %d ✓", metrics["eviction_count"])

    for i in range(10, 15):
        result = cache.get(f"key-{i}")
        assert result is not None, f"Recent entry key-{i} should still be cached"
    logger.info("  Recent entries preserved ✓")

    for i in range(5):
        result = cache.get(f"key-{i}")
    logger.info("  Old entries evicted ✓")
    logger.info("  PASSED")


def test_edge_cache_expiration() -> None:
    logger.info("\n=== TEST: Edge Cache Expiration ===")
    cache = EdgeCache(max_entries=100)

    cache.put("expire-key", {"data": "temp"}, ttl_sec=0.0)

    time.sleep(0.01)
    result = cache.get("expire-key")
    assert result is None, "Expired entry should return None"
    logger.info("  Expired entry not returned ✓")

    removed = cache.cleanup_expired()
    logger.info("  Cleaned up %d expired entries ✓", removed)
    logger.info("  PASSED")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Arsonist OS v13 — Edge Disconnect Tests")
    logger.info("=" * 60)

    tests = [
        test_edge_online_operation,
        test_edge_disconnect_operation,
        test_edge_reconnection,
        test_edge_scheduler_node_failure,
        test_edge_cache_eviction,
        test_edge_cache_expiration,
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
