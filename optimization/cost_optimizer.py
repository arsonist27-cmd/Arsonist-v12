from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("optimization.cost")


class CostCategory(str, Enum):
    gpu_compute = "gpu_compute"
    bandwidth = "bandwidth"
    energy = "energy"
    idle_waste = "idle_waste"
    replication = "replication"
    storage = "storage"


class CostAction(BaseModel):
    action_id: str
    category: CostCategory
    region_id: str = ""
    action_type: str = ""
    current_cost_per_hour: float = 0.0
    projected_cost_per_hour: float = 0.0
    savings_pct: float = 0.0
    description: str = ""
    executed: bool = False
    created_at: float = 0.0


class RegionCostProfile(BaseModel):
    region_id: str
    gpu_cost_per_hour: float = 0.0
    bandwidth_cost_per_gb: float = 0.0
    energy_cost_per_kwh: float = 0.0
    idle_waste_pct: float = 0.0
    total_cost_per_hour: float = 0.0
    efficiency_score: float = 0.0
    ts: float = 0.0


class CostOptimizer:
    """Optimizes global infrastructure costs by analyzing GPU efficiency,
    bandwidth costs, energy usage, idle waste, and replication overhead.
    Supports spot workload routing and regional cost-aware scheduling."""

    def __init__(
        self,
        idle_waste_threshold: float = 0.20,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._idle_waste_threshold = idle_waste_threshold
        self._max_history = max_history
        self._profiles: Dict[str, RegionCostProfile] = {}
        self._actions: List[CostAction] = []
        self._total_optimizations = 0
        self._total_savings_pct = 0.0
        self._events: List[Dict[str, Any]] = []

    def analyze(self, telemetry: Dict[str, Any]) -> List[CostAction]:
        actions: List[CostAction] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            gpu_cost = r.get("gpu_cost_per_hour", 0.0)
            bw_cost = r.get("bandwidth_cost_per_gb", 0.0)
            energy_cost = r.get("energy_cost_per_kwh", 0.0)
            gpu_util = r.get("gpu_utilization", 0.0)
            total_cost = gpu_cost + bw_cost * r.get("bandwidth_usage_gb", 0.0) + energy_cost * r.get("energy_kwh", 0.0)

            idle_waste = max(0.0, 1.0 - gpu_util) if gpu_util < 0.5 else 0.0
            efficiency = min(1.0, gpu_util * 0.6 + (1.0 - idle_waste) * 0.4)

            profile = RegionCostProfile(
                region_id=region_id,
                gpu_cost_per_hour=round(gpu_cost, 4),
                bandwidth_cost_per_gb=round(bw_cost, 4),
                energy_cost_per_kwh=round(energy_cost, 4),
                idle_waste_pct=round(idle_waste, 3),
                total_cost_per_hour=round(total_cost, 4),
                efficiency_score=round(efficiency, 3),
                ts=ts,
            )
            with self._lock:
                self._profiles[region_id] = profile

            if idle_waste > self._idle_waste_threshold:
                savings = round(idle_waste * gpu_cost, 4)
                actions.append(CostAction(
                    action_id=f"cost-idle-{region_id}-{int(ts)}",
                    category=CostCategory.idle_waste,
                    region_id=region_id,
                    action_type="consolidate_idle",
                    current_cost_per_hour=total_cost,
                    projected_cost_per_hour=round(total_cost - savings, 4),
                    savings_pct=round(idle_waste * 100, 1),
                    description=f"Consolidate idle GPUs in {region_id} ({idle_waste:.0%} waste)",
                    created_at=ts,
                ))

            if gpu_cost > 0 and gpu_util > 0.9:
                actions.append(CostAction(
                    action_id=f"cost-spot-{region_id}-{int(ts)}",
                    category=CostCategory.gpu_compute,
                    region_id=region_id,
                    action_type="spot_routing",
                    current_cost_per_hour=total_cost,
                    projected_cost_per_hour=round(total_cost * 0.7, 4),
                    savings_pct=30.0,
                    description=f"Route low-priority workloads to spot instances from {region_id}",
                    created_at=ts,
                ))

            repl_overhead = r.get("replication_overhead_pct", 0.0)
            if repl_overhead > 0.15:
                actions.append(CostAction(
                    action_id=f"cost-repl-{region_id}-{int(ts)}",
                    category=CostCategory.replication,
                    region_id=region_id,
                    action_type="reduce_replication",
                    current_cost_per_hour=total_cost,
                    projected_cost_per_hour=round(total_cost * (1 - repl_overhead * 0.5), 4),
                    savings_pct=round(repl_overhead * 50, 1),
                    description=f"Reduce replication overhead in {region_id} ({repl_overhead:.0%})",
                    created_at=ts,
                ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute(self, action: CostAction) -> CostAction:
        action.executed = True
        with self._lock:
            self._total_optimizations += 1
            self._total_savings_pct += action.savings_pct
            self._events.append({
                "type": "cost_optimization_executed",
                "action_id": action.action_id,
                "category": action.category.value,
                "region": action.region_id,
                "savings_pct": action.savings_pct,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        logger.info("cost optimization %s: %s (%.1f%% savings)", action.action_type, action.region_id, action.savings_pct)
        return action

    def optimize(self, telemetry: Dict[str, Any]) -> List[CostAction]:
        actions = self.analyze(telemetry)
        for a in actions:
            self.execute(a)
        return actions

    def cost_map(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {rid: p.model_dump(mode="json") for rid, p in self._profiles.items()}

    def cheapest_regions(self, limit: int = 5) -> List[Dict[str, Any]]:
        with self._lock:
            sorted_profiles = sorted(self._profiles.values(), key=lambda p: p.total_cost_per_hour)
            return [p.model_dump(mode="json") for p in sorted_profiles[:limit]]

    def recent_actions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in reversed(self._actions)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            profiles = list(self._profiles.values())
            total_cost = sum(p.total_cost_per_hour for p in profiles)
            avg_efficiency = sum(p.efficiency_score for p in profiles) / len(profiles) if profiles else 0.0
            return {
                "ts": now_ts(),
                "total_optimizations": self._total_optimizations,
                "total_cost_per_hour": round(total_cost, 4),
                "avg_efficiency": round(avg_efficiency, 3),
                "monitored_regions": len(profiles),
            }
