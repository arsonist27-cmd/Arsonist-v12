"""v15 Extreme Simulation Layer.

Simulates massive traffic spikes, regional blackouts, transcontinental
failovers, global inference surges, and edge isolation events. Outputs
resilience metrics, failover timing, routing efficiency, and global
latency maps.
"""
from __future__ import annotations

import random
import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("simulation.planetary_simulation")


class ScenarioType(str, Enum):
    traffic_spike = "traffic_spike"
    regional_blackout = "regional_blackout"
    continental_failover = "continental_failover"
    inference_surge = "inference_surge"
    edge_isolation = "edge_isolation"
    cascade_failure = "cascade_failure"
    global_stress = "global_stress"


class SimulationResult(BaseModel):
    scenario_id: str
    scenario_type: ScenarioType = ScenarioType.global_stress
    description: str = ""
    duration_s: float = 0.0
    regions_affected: int = 0
    workloads_rerouted: int = 0
    failover_time_ms: float = 0.0
    recovery_time_ms: float = 0.0
    avg_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    routing_efficiency: float = 0.0
    resilience_score: float = 0.0
    throughput_maintained_pct: float = 0.0
    data_loss_pct: float = 0.0
    latency_map: Dict[str, float] = Field(default_factory=dict)
    details: Dict[str, Any] = Field(default_factory=dict)
    ts: float = 0.0


class PlanetarySimulator:
    """Simulates extreme scenarios across planetary infrastructure to
    measure resilience, failover timing, and routing efficiency."""

    def __init__(self, max_history: int = 100) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._results: List[SimulationResult] = []
        self._total_simulations = 0

    def simulate_traffic_spike(self, telemetry: Dict[str, Any],
                               spike_multiplier: float = 10.0) -> SimulationResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        scenario_id = f"sim-spike-{int(start)}"

        affected = 0
        rerouted = 0
        latencies = {}
        for r in regions:
            rid = r.get("region_id", "")
            base_sat = r.get("workload_saturation", 0.5)
            spiked_sat = min(1.0, base_sat * spike_multiplier)
            base_latency = r.get("avg_latency_ms", 30)

            if spiked_sat > 0.95:
                affected += 1
                latency = base_latency * (1 + (spiked_sat - 0.5) * 4)
                rerouted += int(r.get("active_workloads", 10) * 0.3)
            else:
                latency = base_latency * (1 + spiked_sat * 0.5)
            latencies[rid] = round(latency, 1)

        avg_lat = sum(latencies.values()) / len(latencies) if latencies else 0
        max_lat = max(latencies.values()) if latencies else 0
        resilience = max(0, 1.0 - affected / len(regions)) if regions else 0
        throughput = max(0, 100 * (1 - affected * 0.1))

        result = SimulationResult(
            scenario_id=scenario_id,
            scenario_type=ScenarioType.traffic_spike,
            description=f"{spike_multiplier}x traffic spike across {len(regions)} regions",
            duration_s=round(now_ts() - start, 3),
            regions_affected=affected,
            workloads_rerouted=rerouted,
            failover_time_ms=round(random.uniform(50, 200), 1),
            recovery_time_ms=round(random.uniform(500, 3000), 1),
            avg_latency_ms=round(avg_lat, 1),
            max_latency_ms=round(max_lat, 1),
            routing_efficiency=round(max(0.5, 1.0 - affected * 0.05), 3),
            resilience_score=round(resilience, 3),
            throughput_maintained_pct=round(throughput, 1),
            data_loss_pct=0.0,
            latency_map=latencies,
            ts=now_ts(),
        )
        self._store(result)
        return result

    def simulate_regional_blackout(self, telemetry: Dict[str, Any],
                                   target_continent: str = "") -> SimulationResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        scenario_id = f"sim-blackout-{int(start)}"

        if not target_continent:
            continents = set(r.get("continent", "") for r in regions)
            target_continent = random.choice(list(continents)) if continents else "NA"

        blacked_out = [r for r in regions if r.get("continent") == target_continent]
        surviving = [r for r in regions if r.get("continent") != target_continent]

        rerouted = sum(r.get("active_workloads", 5) for r in blacked_out)
        latencies = {}
        for r in surviving:
            rid = r.get("region_id", "")
            base = r.get("avg_latency_ms", 30)
            latencies[rid] = round(base * 1.5, 1)
        for r in blacked_out:
            latencies[r.get("region_id", "")] = 9999.0

        avg_lat = sum(v for v in latencies.values() if v < 9000) / max(1, len(surviving))
        resilience = len(surviving) / len(regions) if regions else 0

        result = SimulationResult(
            scenario_id=scenario_id,
            scenario_type=ScenarioType.regional_blackout,
            description=f"Complete blackout of {target_continent} ({len(blacked_out)} regions)",
            duration_s=round(now_ts() - start, 3),
            regions_affected=len(blacked_out),
            workloads_rerouted=rerouted,
            failover_time_ms=round(random.uniform(100, 500), 1),
            recovery_time_ms=round(random.uniform(5000, 30000), 1),
            avg_latency_ms=round(avg_lat, 1),
            max_latency_ms=max(v for v in latencies.values() if v < 9000) if surviving else 9999,
            routing_efficiency=round(max(0.3, resilience * 0.9), 3),
            resilience_score=round(resilience, 3),
            throughput_maintained_pct=round(resilience * 100, 1),
            data_loss_pct=round(random.uniform(0, 0.5), 2),
            latency_map=latencies,
            details={"blacked_out_continent": target_continent,
                     "regions_offline": len(blacked_out),
                     "regions_surviving": len(surviving)},
            ts=now_ts(),
        )
        self._store(result)
        return result

    def simulate_continental_failover(self, telemetry: Dict[str, Any],
                                      source: str = "", target: str = "") -> SimulationResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        scenario_id = f"sim-failover-{int(start)}"

        continents = list(set(r.get("continent", "") for r in regions if r.get("continent")))
        if not source and continents:
            source = continents[0]
        if not target and len(continents) > 1:
            target = continents[1]

        source_regions = [r for r in regions if r.get("continent") == source]
        target_regions = [r for r in regions if r.get("continent") == target]
        rerouted = sum(r.get("active_workloads", 5) for r in source_regions)

        latencies = {}
        for r in target_regions:
            rid = r.get("region_id", "")
            base = r.get("avg_latency_ms", 30)
            additional = rerouted * 0.5
            latencies[rid] = round(base + additional, 1)

        avg_lat = sum(latencies.values()) / len(latencies) if latencies else 0
        efficiency = max(0.5, 1.0 - (rerouted / 100) * 0.1) if rerouted else 1.0

        result = SimulationResult(
            scenario_id=scenario_id,
            scenario_type=ScenarioType.continental_failover,
            description=f"Failover from {source} to {target} ({rerouted} workloads)",
            duration_s=round(now_ts() - start, 3),
            regions_affected=len(source_regions),
            workloads_rerouted=rerouted,
            failover_time_ms=round(random.uniform(200, 1000), 1),
            recovery_time_ms=round(random.uniform(2000, 15000), 1),
            avg_latency_ms=round(avg_lat, 1),
            max_latency_ms=round(max(latencies.values()) if latencies else 0, 1),
            routing_efficiency=round(efficiency, 3),
            resilience_score=round(min(1.0, len(target_regions) / max(1, len(source_regions))), 3),
            throughput_maintained_pct=round(min(100, efficiency * 100), 1),
            data_loss_pct=0.0,
            latency_map=latencies,
            details={"source_continent": source, "target_continent": target},
            ts=now_ts(),
        )
        self._store(result)
        return result

    def simulate_inference_surge(self, telemetry: Dict[str, Any],
                                 surge_factor: float = 50.0) -> SimulationResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        scenario_id = f"sim-surge-{int(start)}"

        affected = 0
        rerouted = 0
        latencies = {}
        for r in regions:
            rid = r.get("region_id", "")
            gpu_util = r.get("gpu_utilization", 0.5)
            surged = min(1.0, gpu_util * surge_factor / 10)
            base_latency = r.get("avg_latency_ms", 30)

            if surged > 0.95:
                affected += 1
                latency = base_latency * 5
                rerouted += int(r.get("active_workloads", 10) * 0.5)
            elif surged > 0.8:
                latency = base_latency * 2
            else:
                latency = base_latency * 1.2
            latencies[rid] = round(latency, 1)

        avg_lat = sum(latencies.values()) / len(latencies) if latencies else 0

        result = SimulationResult(
            scenario_id=scenario_id,
            scenario_type=ScenarioType.inference_surge,
            description=f"{surge_factor}x inference surge across {len(regions)} regions",
            duration_s=round(now_ts() - start, 3),
            regions_affected=affected,
            workloads_rerouted=rerouted,
            failover_time_ms=round(random.uniform(100, 400), 1),
            recovery_time_ms=round(random.uniform(1000, 8000), 1),
            avg_latency_ms=round(avg_lat, 1),
            max_latency_ms=round(max(latencies.values()) if latencies else 0, 1),
            routing_efficiency=round(max(0.4, 1.0 - affected * 0.08), 3),
            resilience_score=round(max(0, 1.0 - affected / len(regions)), 3) if regions else 0,
            throughput_maintained_pct=round(max(0, 100 - affected * 8), 1),
            data_loss_pct=0.0,
            latency_map=latencies,
            ts=now_ts(),
        )
        self._store(result)
        return result

    def simulate_edge_isolation(self, telemetry: Dict[str, Any],
                                isolation_pct: float = 0.3) -> SimulationResult:
        start = now_ts()
        regions = telemetry.get("regions", [])
        scenario_id = f"sim-edge-iso-{int(start)}"

        edge_regions = [r for r in regions if r.get("is_edge") or r.get("node_type") == "edge"]
        if not edge_regions:
            edge_regions = regions[:max(1, int(len(regions) * 0.3))]

        isolated_count = max(1, int(len(edge_regions) * isolation_pct))
        isolated = edge_regions[:isolated_count]
        rerouted = sum(r.get("active_workloads", 3) for r in isolated)

        latencies = {}
        for r in regions:
            rid = r.get("region_id", "")
            if r in isolated:
                latencies[rid] = 9999.0
            else:
                latencies[rid] = round(r.get("avg_latency_ms", 30) * 1.1, 1)

        result = SimulationResult(
            scenario_id=scenario_id,
            scenario_type=ScenarioType.edge_isolation,
            description=f"Edge isolation: {isolated_count}/{len(edge_regions)} edge nodes disconnected",
            duration_s=round(now_ts() - start, 3),
            regions_affected=isolated_count,
            workloads_rerouted=rerouted,
            failover_time_ms=round(random.uniform(50, 300), 1),
            recovery_time_ms=round(random.uniform(1000, 10000), 1),
            avg_latency_ms=round(sum(v for v in latencies.values() if v < 9000) / max(1, len(regions) - isolated_count), 1),
            max_latency_ms=max(v for v in latencies.values() if v < 9000) if any(v < 9000 for v in latencies.values()) else 0,
            routing_efficiency=round(1.0 - isolated_count * 0.05, 3),
            resilience_score=round(1.0 - isolated_count / max(1, len(regions)), 3),
            throughput_maintained_pct=round(100 * (1 - isolated_count / max(1, len(regions))), 1),
            data_loss_pct=round(random.uniform(0, 1.0), 2),
            latency_map=latencies,
            ts=now_ts(),
        )
        self._store(result)
        return result

    def run_full_stress_test(self, telemetry: Dict[str, Any]) -> List[SimulationResult]:
        results = [
            self.simulate_traffic_spike(telemetry, spike_multiplier=10.0),
            self.simulate_regional_blackout(telemetry),
            self.simulate_continental_failover(telemetry),
            self.simulate_inference_surge(telemetry, surge_factor=50.0),
            self.simulate_edge_isolation(telemetry, isolation_pct=0.5),
        ]
        return results

    def _store(self, result: SimulationResult) -> None:
        with self._lock:
            self._results.append(result)
            if len(self._results) > self._max_history:
                self._results = self._results[-self._max_history:]
            self._total_simulations += 1

    def recent_results(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in reversed(self._results)][:limit]

    def results_by_type(self, scenario_type: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._results
                    if r.scenario_type.value == scenario_type]

    def resilience_summary(self) -> Dict[str, Any]:
        with self._lock:
            if not self._results:
                return {"ts": now_ts(), "simulations": 0}
            avg_resilience = sum(r.resilience_score for r in self._results) / len(self._results)
            avg_failover = sum(r.failover_time_ms for r in self._results) / len(self._results)
            avg_recovery = sum(r.recovery_time_ms for r in self._results) / len(self._results)
            by_type = {}
            for r in self._results:
                by_type[r.scenario_type.value] = by_type.get(r.scenario_type.value, 0) + 1
            return {
                "ts": now_ts(),
                "simulations": len(self._results),
                "avg_resilience_score": round(avg_resilience, 3),
                "avg_failover_time_ms": round(avg_failover, 1),
                "avg_recovery_time_ms": round(avg_recovery, 1),
                "by_type": by_type,
            }

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_simulations": self._total_simulations,
            }
