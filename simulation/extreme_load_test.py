"""v15 Extreme Load Test.

Simulates extreme load conditions including massive concurrent requests,
GPU saturation, memory pressure, and queue overflow scenarios across
planetary infrastructure.
"""
from __future__ import annotations

import random
import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("simulation.extreme_load_test")


class LoadTestResult(BaseModel):
    test_id: str
    test_name: str = ""
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    rejected: int = 0
    avg_latency_ms: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    throughput_rps: float = 0.0
    gpu_utilization_peak: float = 0.0
    queue_depth_peak: int = 0
    regions_saturated: int = 0
    duration_s: float = 0.0
    details: Dict[str, Any] = Field(default_factory=dict)
    ts: float = 0.0


class ExtremeLoadTester:
    """Runs extreme load test simulations against planetary infrastructure
    to measure capacity limits, saturation points, and degradation behavior."""

    def __init__(self, max_history: int = 100) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._results: List[LoadTestResult] = []
        self._total_tests = 0

    def test_massive_concurrency(self, telemetry: Dict[str, Any],
                                 concurrent_requests: int = 1000000) -> LoadTestResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        per_region = concurrent_requests // max(1, len(regions))

        total_successful = 0
        total_failed = 0
        total_rejected = 0
        latencies = []
        saturated = 0

        for r in regions:
            capacity = r.get("capacity", 1.0)
            saturation = r.get("workload_saturation", 0.5)
            available = max(0, (1 - saturation) * capacity * 10000)

            handled = min(per_region, int(available))
            rejected = max(0, per_region - int(available * 1.2))
            failed = per_region - handled - rejected

            total_successful += handled
            total_failed += max(0, failed)
            total_rejected += max(0, rejected)

            base_lat = r.get("avg_latency_ms", 30)
            load_factor = per_region / max(1, available)
            latency = base_lat * (1 + load_factor * 3)
            latencies.append(latency)

            if per_region > available * 0.9:
                saturated += 1

        latencies.sort()
        n = len(latencies)
        avg_lat = sum(latencies) / n if n else 0
        p50 = latencies[int(n * 0.5)] if n else 0
        p95 = latencies[int(n * 0.95)] if n else 0
        p99 = latencies[min(int(n * 0.99), n - 1)] if n else 0
        max_lat = latencies[-1] if n else 0
        duration = now_ts() - start

        result = LoadTestResult(
            test_id=f"load-concurrency-{int(start)}",
            test_name=f"Massive concurrency: {concurrent_requests:,} requests",
            total_requests=concurrent_requests,
            successful=total_successful,
            failed=total_failed,
            rejected=total_rejected,
            avg_latency_ms=round(avg_lat, 1),
            p50_latency_ms=round(p50, 1),
            p95_latency_ms=round(p95, 1),
            p99_latency_ms=round(p99, 1),
            max_latency_ms=round(max_lat, 1),
            throughput_rps=round(total_successful / max(0.001, duration), 0),
            gpu_utilization_peak=min(1.0, 0.5 + concurrent_requests / 2000000),
            queue_depth_peak=max(0, concurrent_requests - total_successful),
            regions_saturated=saturated,
            duration_s=round(duration, 3),
            ts=now_ts(),
        )
        self._store(result)
        return result

    def test_gpu_saturation(self, telemetry: Dict[str, Any]) -> LoadTestResult:
        start = now_ts()
        regions = telemetry.get("regions", [])

        total_gpus = sum(r.get("total_gpus", 0) for r in regions)
        requests = total_gpus * 100
        saturated = 0
        successful = 0
        failed = 0

        for r in regions:
            gpus = r.get("total_gpus", 8)
            util = r.get("gpu_utilization", 0.5)
            available_gpu_slots = int(gpus * (1 - util) * 10)
            region_requests = gpus * 100

            handled = min(region_requests, available_gpu_slots)
            successful += handled
            failed += max(0, region_requests - handled)

            if util > 0.9 or handled < region_requests * 0.5:
                saturated += 1

        duration = now_ts() - start
        result = LoadTestResult(
            test_id=f"load-gpu-sat-{int(start)}",
            test_name=f"GPU saturation: {total_gpus} GPUs under full load",
            total_requests=requests,
            successful=successful,
            failed=failed,
            avg_latency_ms=round(random.uniform(50, 200), 1),
            p95_latency_ms=round(random.uniform(200, 800), 1),
            p99_latency_ms=round(random.uniform(500, 2000), 1),
            max_latency_ms=round(random.uniform(1000, 5000), 1),
            throughput_rps=round(successful / max(0.001, duration), 0),
            gpu_utilization_peak=0.99,
            regions_saturated=saturated,
            duration_s=round(duration, 3),
            details={"total_gpus": total_gpus},
            ts=now_ts(),
        )
        self._store(result)
        return result

    def test_queue_overflow(self, telemetry: Dict[str, Any],
                            queue_depth: int = 5000000) -> LoadTestResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        per_region = queue_depth // max(1, len(regions))

        total_queued = 0
        total_rejected = 0

        for r in regions:
            max_queue = int(r.get("capacity", 1.0) * 100000)
            queued = min(per_region, max_queue)
            rejected = max(0, per_region - max_queue)
            total_queued += queued
            total_rejected += rejected

        duration = now_ts() - start
        result = LoadTestResult(
            test_id=f"load-queue-{int(start)}",
            test_name=f"Queue overflow: {queue_depth:,} depth target",
            total_requests=queue_depth,
            successful=total_queued,
            rejected=total_rejected,
            avg_latency_ms=round(random.uniform(500, 5000), 1),
            p99_latency_ms=round(random.uniform(5000, 30000), 1),
            max_latency_ms=round(random.uniform(10000, 60000), 1),
            queue_depth_peak=total_queued,
            regions_saturated=sum(1 for r in regions if r.get("workload_saturation", 0) > 0.9),
            duration_s=round(duration, 3),
            ts=now_ts(),
        )
        self._store(result)
        return result

    def run_full_suite(self, telemetry: Dict[str, Any]) -> List[LoadTestResult]:
        return [
            self.test_massive_concurrency(telemetry),
            self.test_gpu_saturation(telemetry),
            self.test_queue_overflow(telemetry),
        ]

    def _store(self, result: LoadTestResult) -> None:
        with self._lock:
            self._results.append(result)
            if len(self._results) > self._max_history:
                self._results = self._results[-self._max_history:]
            self._total_tests += 1

    def recent_results(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in reversed(self._results)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_tests": self._total_tests,
            }
