from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("intelligence.optimization")


class OptimizationCategory(str, Enum):
    placement = "placement"
    latency = "latency"
    gpu_utilization = "gpu_utilization"
    congestion = "congestion"
    replication = "replication"
    cost = "cost"
    thermal = "thermal"
    energy = "energy"


class Inefficiency(BaseModel):
    inefficiency_id: str
    category: OptimizationCategory
    region_id: str = ""
    severity: float = 0.0
    description: str = ""
    recommendation: str = ""
    potential_improvement_pct: float = 0.0
    detected_at: float = 0.0


class OptimizationAction(BaseModel):
    action_id: str
    category: OptimizationCategory
    target_region: str = ""
    target_workload: str = ""
    action_type: str = ""
    parameters: Dict[str, Any] = Field(default_factory=dict)
    expected_improvement_pct: float = 0.0
    executed: bool = False
    executed_at: float = 0.0
    result: str = ""


class OptimizationEngine:
    """Continuously analyzes telemetry to identify inefficiencies and optimize
    placement, latency, GPU utilization, and cluster congestion."""

    def __init__(
        self,
        optimization_interval_s: float = 5.0,
        max_history: int = 1000,
    ) -> None:
        self._lock = threading.RLock()
        self._interval = optimization_interval_s
        self._inefficiencies: List[Inefficiency] = []
        self._actions: List[OptimizationAction] = []
        self._max_history = max_history
        self._total_optimizations = 0
        self._total_improvements = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._telemetry_sources: Dict[str, Any] = {}
        self._events: List[Dict[str, Any]] = []

    def register_telemetry_source(self, name: str, source: Any) -> None:
        with self._lock:
            self._telemetry_sources[name] = source
            logger.info("registered telemetry source: %s", name)

    def analyze_telemetry(self, telemetry: Dict[str, Any]) -> List[Inefficiency]:
        found: List[Inefficiency] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            saturation = r.get("workload_saturation", 0.0)
            if saturation > 0.85:
                found.append(Inefficiency(
                    inefficiency_id=f"congestion-{region_id}-{int(ts)}",
                    category=OptimizationCategory.congestion,
                    region_id=region_id,
                    severity=min(saturation, 1.0),
                    description=f"Region {region_id} saturation at {saturation:.0%}",
                    recommendation=f"Migrate workloads from {region_id} to less loaded regions",
                    potential_improvement_pct=round((saturation - 0.7) * 100, 1),
                    detected_at=ts,
                ))

            avg_latency = r.get("avg_latency_ms", 0.0)
            if avg_latency > 200:
                found.append(Inefficiency(
                    inefficiency_id=f"latency-{region_id}-{int(ts)}",
                    category=OptimizationCategory.latency,
                    region_id=region_id,
                    severity=min(avg_latency / 500.0, 1.0),
                    description=f"Region {region_id} avg latency {avg_latency:.0f}ms",
                    recommendation=f"Reroute traffic away from {region_id} or add capacity",
                    potential_improvement_pct=round((avg_latency - 100) / avg_latency * 100, 1),
                    detected_at=ts,
                ))

            gpu_util = r.get("gpu_utilization", 0.0)
            if gpu_util < 0.3 and r.get("total_gpus", 0) > 0:
                found.append(Inefficiency(
                    inefficiency_id=f"gpu-underutil-{region_id}-{int(ts)}",
                    category=OptimizationCategory.gpu_utilization,
                    region_id=region_id,
                    severity=1.0 - gpu_util,
                    description=f"Region {region_id} GPU utilization only {gpu_util:.0%}",
                    recommendation=f"Consolidate workloads into {region_id} or power down idle GPUs",
                    potential_improvement_pct=round((0.7 - gpu_util) * 100, 1),
                    detected_at=ts,
                ))
            elif gpu_util > 0.95:
                found.append(Inefficiency(
                    inefficiency_id=f"gpu-exhaust-{region_id}-{int(ts)}",
                    category=OptimizationCategory.gpu_utilization,
                    region_id=region_id,
                    severity=gpu_util,
                    description=f"Region {region_id} GPU exhaustion at {gpu_util:.0%}",
                    recommendation=f"Offload workloads from {region_id} to available regions",
                    potential_improvement_pct=round((gpu_util - 0.8) * 100, 1),
                    detected_at=ts,
                ))

            thermal = r.get("thermal_pressure", 0.0)
            if thermal > 0.8:
                found.append(Inefficiency(
                    inefficiency_id=f"thermal-{region_id}-{int(ts)}",
                    category=OptimizationCategory.thermal,
                    region_id=region_id,
                    severity=thermal,
                    description=f"Region {region_id} thermal pressure {thermal:.0%}",
                    recommendation=f"Reduce load on {region_id} to lower GPU temperatures",
                    potential_improvement_pct=round((thermal - 0.6) * 100, 1),
                    detected_at=ts,
                ))

        with self._lock:
            self._inefficiencies.extend(found)
            if len(self._inefficiencies) > self._max_history:
                self._inefficiencies = self._inefficiencies[-self._max_history:]

        return found

    def generate_actions(self, inefficiencies: List[Inefficiency]) -> List[OptimizationAction]:
        actions: List[OptimizationAction] = []
        ts = now_ts()

        for ineff in inefficiencies:
            if ineff.category == OptimizationCategory.congestion:
                actions.append(OptimizationAction(
                    action_id=f"migrate-{ineff.region_id}-{int(ts)}",
                    category=ineff.category,
                    target_region=ineff.region_id,
                    action_type="workload_migration",
                    parameters={"direction": "outbound", "max_migrate_pct": 0.2},
                    expected_improvement_pct=ineff.potential_improvement_pct,
                ))
            elif ineff.category == OptimizationCategory.latency:
                actions.append(OptimizationAction(
                    action_id=f"reroute-{ineff.region_id}-{int(ts)}",
                    category=ineff.category,
                    target_region=ineff.region_id,
                    action_type="traffic_reroute",
                    parameters={"strategy": "lowest_latency"},
                    expected_improvement_pct=ineff.potential_improvement_pct,
                ))
            elif ineff.category == OptimizationCategory.gpu_utilization:
                action_type = "consolidate" if ineff.severity > 0.5 else "offload"
                actions.append(OptimizationAction(
                    action_id=f"gpu-opt-{ineff.region_id}-{int(ts)}",
                    category=ineff.category,
                    target_region=ineff.region_id,
                    action_type=action_type,
                    parameters={"target_utilization": 0.7},
                    expected_improvement_pct=ineff.potential_improvement_pct,
                ))
            elif ineff.category == OptimizationCategory.thermal:
                actions.append(OptimizationAction(
                    action_id=f"thermal-{ineff.region_id}-{int(ts)}",
                    category=ineff.category,
                    target_region=ineff.region_id,
                    action_type="thermal_rebalance",
                    parameters={"target_thermal": 0.6},
                    expected_improvement_pct=ineff.potential_improvement_pct,
                ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]
            self._total_optimizations += len(actions)

        return actions

    def execute_action(self, action: OptimizationAction) -> OptimizationAction:
        action.executed = True
        action.executed_at = now_ts()
        action.result = "applied"
        with self._lock:
            self._total_improvements += 1
            self._events.append({
                "type": "optimization_executed",
                "action_id": action.action_id,
                "action_type": action.action_type,
                "region": action.target_region,
                "ts": action.executed_at,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        logger.info("executed optimization %s on %s", action.action_type, action.target_region)
        return action

    def run_optimization_loop(self, telemetry: Dict[str, Any]) -> List[OptimizationAction]:
        start = now_ts()
        inefficiencies = self.analyze_telemetry(telemetry)
        actions = self.generate_actions(inefficiencies)
        for action in actions:
            self.execute_action(action)
        elapsed = (now_ts() - start) * 1000
        logger.info("optimization loop completed in %.1fms, %d actions", elapsed, len(actions))
        return actions

    def recent_inefficiencies(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [i.model_dump(mode="json") for i in reversed(self._inefficiencies)][:limit]

    def recent_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in reversed(self._actions)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_optimizations": self._total_optimizations,
                "total_improvements": self._total_improvements,
                "pending_inefficiencies": len(self._inefficiencies),
                "pending_actions": len(self._actions),
            }
