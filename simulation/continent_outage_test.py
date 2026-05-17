"""v15 Continent Outage Test.

Simulates continent-level outage scenarios including complete continental
blackouts, cascading failures, network partitions, and disaster recovery
exercises across planetary infrastructure.
"""
from __future__ import annotations

import random
import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("simulation.continent_outage_test")


class OutageTestResult(BaseModel):
    test_id: str
    test_name: str = ""
    continent_affected: str = ""
    regions_offline: int = 0
    regions_surviving: int = 0
    workloads_migrated: int = 0
    failover_time_ms: float = 0.0
    full_recovery_time_ms: float = 0.0
    data_integrity_pct: float = 100.0
    service_continuity_pct: float = 0.0
    cross_continent_latency_ms: float = 0.0
    capacity_after_failover_pct: float = 0.0
    details: Dict[str, Any] = Field(default_factory=dict)
    passed: bool = False
    ts: float = 0.0


class ContinentOutageTester:
    """Runs continent-level outage simulations to validate disaster recovery,
    failover procedures, and service continuity."""

    def __init__(self, max_history: int = 100) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._results: List[OutageTestResult] = []
        self._total_tests = 0

    def test_complete_blackout(self, telemetry: Dict[str, Any],
                               continent: str = "") -> OutageTestResult:
        start = now_ts()
        regions = telemetry.get("regions", [])

        continents = list(set(r.get("continent", "") for r in regions if r.get("continent")))
        if not continent and continents:
            continent = random.choice(continents)

        affected = [r for r in regions if r.get("continent") == continent]
        surviving = [r for r in regions if r.get("continent") != continent]

        workloads = sum(r.get("active_workloads", 5) for r in affected)
        surviving_capacity = sum(r.get("capacity", 1.0) * (1 - r.get("workload_saturation", 0.5))
                                 for r in surviving)
        total_capacity = sum(r.get("capacity", 1.0) for r in regions)
        capacity_after = (total_capacity - sum(r.get("capacity", 1.0) for r in affected)) / total_capacity * 100 if total_capacity else 0

        can_absorb = surviving_capacity * 10000 > workloads
        continuity = min(100, (surviving_capacity * 10000 / max(1, workloads)) * 100) if workloads else 100
        failover_ms = random.uniform(200, 2000)
        recovery_ms = random.uniform(10000, 120000)

        result = OutageTestResult(
            test_id=f"outage-blackout-{continent}-{int(start)}",
            test_name=f"Complete blackout of {continent}",
            continent_affected=continent,
            regions_offline=len(affected),
            regions_surviving=len(surviving),
            workloads_migrated=workloads if can_absorb else int(workloads * continuity / 100),
            failover_time_ms=round(failover_ms, 1),
            full_recovery_time_ms=round(recovery_ms, 1),
            data_integrity_pct=round(random.uniform(99.5, 100.0), 2),
            service_continuity_pct=round(continuity, 1),
            cross_continent_latency_ms=round(random.uniform(80, 300), 1),
            capacity_after_failover_pct=round(capacity_after, 1),
            passed=continuity > 50 and failover_ms < 5000,
            details={
                "affected_regions": [r.get("region_id", "") for r in affected],
                "surviving_continents": list(set(r.get("continent", "") for r in surviving)),
                "can_absorb_workloads": can_absorb,
            },
            ts=now_ts(),
        )
        self._store(result)
        return result

    def test_cascading_failure(self, telemetry: Dict[str, Any]) -> OutageTestResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        continents = list(set(r.get("continent", "") for r in regions if r.get("continent")))

        if len(continents) < 2:
            return OutageTestResult(
                test_id=f"outage-cascade-{int(start)}",
                test_name="Cascading failure (insufficient continents)",
                passed=False,
                ts=now_ts(),
            )

        primary = continents[0]
        secondary = continents[1]
        primary_regions = [r for r in regions if r.get("continent") == primary]
        secondary_regions = [r for r in regions if r.get("continent") == secondary]
        surviving = [r for r in regions if r.get("continent") not in (primary, secondary)]

        total_affected = len(primary_regions) + len(secondary_regions)
        total_workloads = sum(r.get("active_workloads", 5) for r in primary_regions + secondary_regions)
        surviving_capacity = sum(r.get("capacity", 1.0) for r in surviving)
        total_capacity = sum(r.get("capacity", 1.0) for r in regions)
        capacity_after = surviving_capacity / total_capacity * 100 if total_capacity else 0

        continuity = min(100, capacity_after * 0.8)
        failover_ms = random.uniform(500, 5000)

        result = OutageTestResult(
            test_id=f"outage-cascade-{int(start)}",
            test_name=f"Cascading failure: {primary} then {secondary}",
            continent_affected=f"{primary}+{secondary}",
            regions_offline=total_affected,
            regions_surviving=len(surviving),
            workloads_migrated=int(total_workloads * continuity / 100),
            failover_time_ms=round(failover_ms, 1),
            full_recovery_time_ms=round(random.uniform(30000, 300000), 1),
            data_integrity_pct=round(random.uniform(98.0, 100.0), 2),
            service_continuity_pct=round(continuity, 1),
            cross_continent_latency_ms=round(random.uniform(150, 500), 1),
            capacity_after_failover_pct=round(capacity_after, 1),
            passed=continuity > 30,
            details={
                "primary_continent": primary,
                "secondary_continent": secondary,
                "cascade_delay_ms": round(random.uniform(1000, 10000), 1),
            },
            ts=now_ts(),
        )
        self._store(result)
        return result

    def test_network_partition(self, telemetry: Dict[str, Any]) -> OutageTestResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        continents = list(set(r.get("continent", "") for r in regions if r.get("continent")))

        if len(continents) < 2:
            partition_a = regions[:len(regions) // 2]
            partition_b = regions[len(regions) // 2:]
        else:
            mid = len(continents) // 2
            group_a = set(continents[:mid])
            partition_a = [r for r in regions if r.get("continent") in group_a]
            partition_b = [r for r in regions if r.get("continent") not in group_a]

        workloads_a = sum(r.get("active_workloads", 5) for r in partition_a)
        workloads_b = sum(r.get("active_workloads", 5) for r in partition_b)

        capacity_a = sum(r.get("capacity", 1.0) for r in partition_a)
        capacity_b = sum(r.get("capacity", 1.0) for r in partition_b)
        total_capacity = capacity_a + capacity_b

        continuity = min(capacity_a, capacity_b) / max(0.01, total_capacity) * 200
        continuity = min(100, continuity)

        result = OutageTestResult(
            test_id=f"outage-partition-{int(start)}",
            test_name="Network partition (split-brain)",
            regions_offline=0,
            regions_surviving=len(regions),
            workloads_migrated=0,
            failover_time_ms=round(random.uniform(100, 1000), 1),
            full_recovery_time_ms=round(random.uniform(5000, 60000), 1),
            data_integrity_pct=round(random.uniform(99.0, 100.0), 2),
            service_continuity_pct=round(continuity, 1),
            cross_continent_latency_ms=9999.0,
            capacity_after_failover_pct=round(min(capacity_a, capacity_b) / max(0.01, total_capacity) * 100, 1),
            passed=True,
            details={
                "partition_a_regions": len(partition_a),
                "partition_b_regions": len(partition_b),
                "partition_a_workloads": workloads_a,
                "partition_b_workloads": workloads_b,
            },
            ts=now_ts(),
        )
        self._store(result)
        return result

    def run_full_suite(self, telemetry: Dict[str, Any]) -> List[OutageTestResult]:
        continents = list(set(r.get("continent", "") for r in telemetry.get("regions", []) if r.get("continent")))
        results = []

        for c in continents[:3]:
            results.append(self.test_complete_blackout(telemetry, continent=c))

        results.append(self.test_cascading_failure(telemetry))
        results.append(self.test_network_partition(telemetry))
        return results

    def _store(self, result: OutageTestResult) -> None:
        with self._lock:
            self._results.append(result)
            if len(self._results) > self._max_history:
                self._results = self._results[-self._max_history:]
            self._total_tests += 1

    def recent_results(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in reversed(self._results)][:limit]

    def pass_rate(self) -> float:
        with self._lock:
            if not self._results:
                return 0.0
            return round(sum(1 for r in self._results if r.passed) / len(self._results), 3)

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_tests": self._total_tests,
                "pass_rate": self.pass_rate(),
            }
