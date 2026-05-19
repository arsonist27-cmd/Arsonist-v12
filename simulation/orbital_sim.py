"""v16 Orbital Simulation.

Simulates communication delay, orbital outages, network partitions,
bandwidth starvation, and isolated cluster scenarios across the
interplanetary infrastructure fabric.
"""
from __future__ import annotations

import random
import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("simulation.orbital_sim")


class OrbitalScenario(str, Enum):
    communication_delay = "communication_delay"
    orbital_outage = "orbital_outage"
    network_partition = "network_partition"
    bandwidth_starvation = "bandwidth_starvation"
    cluster_isolation = "cluster_isolation"
    signal_blackout = "signal_blackout"
    relay_failure = "relay_failure"
    full_stress = "full_stress"


class OrbitalSimResult(BaseModel):
    scenario: str = ""
    duration_s: float = 0.0
    nodes_affected: int = 0
    links_affected: int = 0
    workloads_disrupted: int = 0
    workloads_recovered: int = 0
    messages_delayed: int = 0
    messages_lost: int = 0
    partitions_created: int = 0
    partitions_healed: int = 0
    failovers_triggered: int = 0
    avg_recovery_time_ms: float = 0.0
    max_latency_ms: float = 0.0
    data_preserved_pct: float = 100.0
    autonomous_decisions: int = 0
    resilience_score: float = 0.0
    ts: float = 0.0
    details: Dict[str, Any] = Field(default_factory=dict)


class OrbitalSimulator:
    """Simulates extreme scenarios across interplanetary infrastructure
    to measure resilience, recovery, and autonomous operation capabilities."""

    def __init__(self, seed: int = 42) -> None:
        self._lock = threading.RLock()
        self._rng = random.Random(seed)
        self._results: List[OrbitalSimResult] = []

    def simulate_communication_delay(self, num_nodes: int = 20,
                                     delay_range_ms: tuple = (100, 5000),
                                     duration_s: float = 300.0) -> OrbitalSimResult:
        start = now_ts()
        delayed_nodes = self._rng.randint(num_nodes // 4, num_nodes)
        delays = [self._rng.uniform(delay_range_ms[0], delay_range_ms[1])
                  for _ in range(delayed_nodes)]
        max_delay = max(delays)
        avg_delay = sum(delays) / len(delays)

        workloads = self._rng.randint(50, 500)
        disrupted = int(workloads * (avg_delay / delay_range_ms[1]) * 0.5)
        recovered = int(disrupted * self._rng.uniform(0.85, 0.99))
        messages_delayed = self._rng.randint(100, 2000)

        resilience = max(0.0, 1.0 - (disrupted - recovered) / max(workloads, 1))

        result = OrbitalSimResult(
            scenario=OrbitalScenario.communication_delay.value,
            duration_s=duration_s,
            nodes_affected=delayed_nodes,
            workloads_disrupted=disrupted,
            workloads_recovered=recovered,
            messages_delayed=messages_delayed,
            max_latency_ms=round(max_delay, 1),
            avg_recovery_time_ms=round(avg_delay * 2, 1),
            data_preserved_pct=round(self._rng.uniform(95.0, 100.0), 1),
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"avg_delay_ms": round(avg_delay, 1), "delayed_nodes": delayed_nodes},
        )
        with self._lock:
            self._results.append(result)
        return result

    def simulate_orbital_outage(self, num_nodes: int = 20,
                                outage_fraction: float = 0.3) -> OrbitalSimResult:
        outage_nodes = max(1, int(num_nodes * outage_fraction))
        workloads = self._rng.randint(100, 1000)
        disrupted = int(workloads * outage_fraction * self._rng.uniform(0.6, 0.9))
        failovers = self._rng.randint(outage_nodes, outage_nodes * 3)
        recovered = int(disrupted * self._rng.uniform(0.80, 0.98))
        recovery_ms = self._rng.uniform(500, 5000)

        resilience = max(0.0, 1.0 - (disrupted - recovered) / max(workloads, 1))

        result = OrbitalSimResult(
            scenario=OrbitalScenario.orbital_outage.value,
            duration_s=self._rng.uniform(60, 600),
            nodes_affected=outage_nodes,
            workloads_disrupted=disrupted,
            workloads_recovered=recovered,
            failovers_triggered=failovers,
            avg_recovery_time_ms=round(recovery_ms, 1),
            data_preserved_pct=round(self._rng.uniform(90.0, 99.5), 1),
            autonomous_decisions=self._rng.randint(5, 50),
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"outage_nodes": outage_nodes},
        )
        with self._lock:
            self._results.append(result)
        return result

    def simulate_network_partition(self, num_nodes: int = 20,
                                   partition_sizes: tuple = (0.4, 0.6)) -> OrbitalSimResult:
        side_a = int(num_nodes * partition_sizes[0])
        side_b = num_nodes - side_a
        links_affected = self._rng.randint(side_a, side_a * side_b)
        workloads = self._rng.randint(200, 2000)
        disrupted = int(workloads * self._rng.uniform(0.2, 0.5))
        recovered = int(disrupted * self._rng.uniform(0.75, 0.95))
        messages_lost = self._rng.randint(10, 200)
        autonomous = self._rng.randint(10, 100)

        resilience = max(0.0, 1.0 - (disrupted - recovered) / max(workloads, 1))

        result = OrbitalSimResult(
            scenario=OrbitalScenario.network_partition.value,
            duration_s=self._rng.uniform(30, 300),
            nodes_affected=num_nodes,
            links_affected=links_affected,
            workloads_disrupted=disrupted,
            workloads_recovered=recovered,
            messages_lost=messages_lost,
            partitions_created=1,
            partitions_healed=1 if self._rng.random() > 0.1 else 0,
            autonomous_decisions=autonomous,
            data_preserved_pct=round(self._rng.uniform(85.0, 99.0), 1),
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"side_a": side_a, "side_b": side_b},
        )
        with self._lock:
            self._results.append(result)
        return result

    def simulate_bandwidth_starvation(self, num_links: int = 15,
                                      starvation_pct: float = 0.9) -> OrbitalSimResult:
        starved_links = max(1, int(num_links * starvation_pct))
        workloads = self._rng.randint(100, 500)
        disrupted = int(workloads * starvation_pct * self._rng.uniform(0.3, 0.6))
        recovered = int(disrupted * self._rng.uniform(0.7, 0.95))
        messages_delayed = self._rng.randint(500, 5000)

        resilience = max(0.0, 1.0 - (disrupted - recovered) / max(workloads, 1))

        result = OrbitalSimResult(
            scenario=OrbitalScenario.bandwidth_starvation.value,
            duration_s=self._rng.uniform(60, 600),
            links_affected=starved_links,
            workloads_disrupted=disrupted,
            workloads_recovered=recovered,
            messages_delayed=messages_delayed,
            avg_recovery_time_ms=round(self._rng.uniform(1000, 10000), 1),
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"starved_links": starved_links, "starvation_pct": starvation_pct},
        )
        with self._lock:
            self._results.append(result)
        return result

    def simulate_cluster_isolation(self, num_clusters: int = 5,
                                   isolated_clusters: int = 2) -> OrbitalSimResult:
        workloads_per_cluster = self._rng.randint(50, 200)
        total_workloads = workloads_per_cluster * num_clusters
        disrupted = workloads_per_cluster * isolated_clusters
        autonomous = self._rng.randint(isolated_clusters * 10, isolated_clusters * 50)
        recovered = int(disrupted * self._rng.uniform(0.90, 0.99))

        resilience = max(0.0, 1.0 - (disrupted - recovered) / max(total_workloads, 1))

        result = OrbitalSimResult(
            scenario=OrbitalScenario.cluster_isolation.value,
            duration_s=self._rng.uniform(120, 1800),
            nodes_affected=isolated_clusters * self._rng.randint(3, 10),
            workloads_disrupted=disrupted,
            workloads_recovered=recovered,
            autonomous_decisions=autonomous,
            data_preserved_pct=round(self._rng.uniform(92.0, 100.0), 1),
            resilience_score=round(resilience, 3),
            ts=now_ts(),
            details={"num_clusters": num_clusters, "isolated": isolated_clusters},
        )
        with self._lock:
            self._results.append(result)
        return result

    def run_full_stress_test(self) -> List[OrbitalSimResult]:
        results = [
            self.simulate_communication_delay(),
            self.simulate_orbital_outage(),
            self.simulate_network_partition(),
            self.simulate_bandwidth_starvation(),
            self.simulate_cluster_isolation(),
        ]
        return results

    def results_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._results]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            if not self._results:
                return {"total_simulations": 0}
            avg_resilience = sum(r.resilience_score for r in self._results) / len(self._results)
            total_disrupted = sum(r.workloads_disrupted for r in self._results)
            total_recovered = sum(r.workloads_recovered for r in self._results)
            return {
                "total_simulations": len(self._results),
                "avg_resilience_score": round(avg_resilience, 3),
                "total_workloads_disrupted": total_disrupted,
                "total_workloads_recovered": total_recovered,
                "recovery_rate": round(total_recovered / max(total_disrupted, 1), 3),
            }
