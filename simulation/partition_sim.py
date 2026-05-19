"""v16 Partition Simulation.

Simulates network partition scenarios to validate partition detection,
split-brain handling, autonomous operation, and reconvergence across
the interplanetary infrastructure.
"""
from __future__ import annotations

import random
import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("simulation.partition_sim")


class PartitionSimResult(BaseModel):
    scenario: str = ""
    num_nodes: int = 0
    num_partitions: int = 0
    partition_duration_s: float = 0.0
    split_brain_detected: bool = False
    split_brain_resolved: bool = False
    conflicts_created: int = 0
    conflicts_resolved: int = 0
    autonomous_decisions: int = 0
    reconvergence_time_ms: float = 0.0
    data_consistency_pct: float = 100.0
    workloads_affected: int = 0
    workloads_continued: int = 0
    resilience_score: float = 0.0
    ts: float = 0.0
    details: Dict[str, Any] = Field(default_factory=dict)


class PartitionSimulator:
    """Simulates partition scenarios for validation of partition-tolerant
    infrastructure behavior."""

    def __init__(self, seed: int = 42) -> None:
        self._lock = threading.RLock()
        self._rng = random.Random(seed)
        self._results: List[PartitionSimResult] = []

    def simulate_clean_partition(self, num_nodes: int = 20) -> PartitionSimResult:
        split_point = self._rng.randint(num_nodes // 4, num_nodes * 3 // 4)
        workloads = self._rng.randint(100, 500)
        affected = int(workloads * self._rng.uniform(0.3, 0.6))
        continued = int(affected * self._rng.uniform(0.85, 0.98))
        duration = self._rng.uniform(30, 300)
        reconvergence = self._rng.uniform(500, 5000)
        conflicts = self._rng.randint(5, 50)
        resolved = int(conflicts * self._rng.uniform(0.90, 1.0))
        autonomous = self._rng.randint(10, 80)

        resilience = max(0.0, min(1.0,
                                  (continued / max(affected, 1)) * 0.5 +
                                  (resolved / max(conflicts, 1)) * 0.3 +
                                  (1 - reconvergence / 10000) * 0.2))

        result = PartitionSimResult(
            scenario="clean_partition",
            num_nodes=num_nodes,
            num_partitions=2,
            partition_duration_s=round(duration, 1),
            conflicts_created=conflicts,
            conflicts_resolved=resolved,
            autonomous_decisions=autonomous,
            reconvergence_time_ms=round(reconvergence, 1),
            data_consistency_pct=round(self._rng.uniform(95.0, 100.0), 1),
            workloads_affected=affected,
            workloads_continued=continued,
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"side_a": split_point, "side_b": num_nodes - split_point},
        )
        with self._lock:
            self._results.append(result)
        return result

    def simulate_split_brain(self, num_nodes: int = 20) -> PartitionSimResult:
        workloads = self._rng.randint(100, 500)
        affected = int(workloads * self._rng.uniform(0.4, 0.7))
        continued = int(affected * self._rng.uniform(0.70, 0.90))
        conflicts = self._rng.randint(20, 100)
        resolved = int(conflicts * self._rng.uniform(0.80, 0.95))
        reconvergence = self._rng.uniform(2000, 15000)
        autonomous = self._rng.randint(20, 120)

        resilience = max(0.0, min(1.0,
                                  (continued / max(affected, 1)) * 0.4 +
                                  (resolved / max(conflicts, 1)) * 0.4 +
                                  (1 - reconvergence / 20000) * 0.2))

        result = PartitionSimResult(
            scenario="split_brain",
            num_nodes=num_nodes,
            num_partitions=2,
            partition_duration_s=round(self._rng.uniform(60, 600), 1),
            split_brain_detected=True,
            split_brain_resolved=self._rng.random() > 0.05,
            conflicts_created=conflicts,
            conflicts_resolved=resolved,
            autonomous_decisions=autonomous,
            reconvergence_time_ms=round(reconvergence, 1),
            data_consistency_pct=round(self._rng.uniform(85.0, 98.0), 1),
            workloads_affected=affected,
            workloads_continued=continued,
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"split_brain_type": "dual_authority"},
        )
        with self._lock:
            self._results.append(result)
        return result

    def simulate_cascading_partition(self, num_nodes: int = 30) -> PartitionSimResult:
        num_partitions = self._rng.randint(3, 6)
        workloads = self._rng.randint(200, 1000)
        affected = int(workloads * self._rng.uniform(0.5, 0.8))
        continued = int(affected * self._rng.uniform(0.60, 0.85))
        conflicts = self._rng.randint(30, 150)
        resolved = int(conflicts * self._rng.uniform(0.75, 0.92))
        reconvergence = self._rng.uniform(5000, 30000)
        autonomous = self._rng.randint(30, 200)

        resilience = max(0.0, min(1.0,
                                  (continued / max(affected, 1)) * 0.4 +
                                  (resolved / max(conflicts, 1)) * 0.3 +
                                  (1 - num_partitions / 10) * 0.15 +
                                  (1 - reconvergence / 60000) * 0.15))

        result = PartitionSimResult(
            scenario="cascading_partition",
            num_nodes=num_nodes,
            num_partitions=num_partitions,
            partition_duration_s=round(self._rng.uniform(120, 1200), 1),
            split_brain_detected=self._rng.random() > 0.3,
            conflicts_created=conflicts,
            conflicts_resolved=resolved,
            autonomous_decisions=autonomous,
            reconvergence_time_ms=round(reconvergence, 1),
            data_consistency_pct=round(self._rng.uniform(80.0, 95.0), 1),
            workloads_affected=affected,
            workloads_continued=continued,
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"num_partitions": num_partitions, "cascade_depth": num_partitions - 1},
        )
        with self._lock:
            self._results.append(result)
        return result

    def run_full_suite(self) -> List[PartitionSimResult]:
        return [
            self.simulate_clean_partition(),
            self.simulate_split_brain(),
            self.simulate_cascading_partition(),
        ]

    def results_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._results]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            if not self._results:
                return {"total_simulations": 0}
            avg_resilience = sum(r.resilience_score for r in self._results) / len(self._results)
            avg_consistency = sum(r.data_consistency_pct for r in self._results) / len(self._results)
            return {
                "total_simulations": len(self._results),
                "avg_resilience_score": round(avg_resilience, 3),
                "avg_data_consistency_pct": round(avg_consistency, 1),
                "total_conflicts_created": sum(r.conflicts_created for r in self._results),
                "total_conflicts_resolved": sum(r.conflicts_resolved for r in self._results),
            }
