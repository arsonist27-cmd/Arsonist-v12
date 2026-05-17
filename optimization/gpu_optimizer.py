from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("optimization.gpu")


class GPUOptimizationAction(BaseModel):
    action_id: str
    region_id: str = ""
    gpu_id: str = ""
    action_type: str = ""
    current_utilization: float = 0.0
    target_utilization: float = 0.7
    workloads_affected: int = 0
    expected_improvement_pct: float = 0.0
    executed: bool = False
    created_at: float = 0.0


class GPUOptimizer:
    """Optimizes GPU utilization across regions by consolidating underutilized
    GPUs, offloading overloaded ones, and balancing workloads for optimal
    throughput and lifespan."""

    def __init__(
        self,
        target_utilization: float = 0.7,
        underutil_threshold: float = 0.3,
        overutil_threshold: float = 0.9,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._target = target_utilization
        self._underutil = underutil_threshold
        self._overutil = overutil_threshold
        self._max_history = max_history
        self._actions: List[GPUOptimizationAction] = []
        self._total_optimizations = 0
        self._events: List[Dict[str, Any]] = []

    def analyze(self, telemetry: Dict[str, Any]) -> List[GPUOptimizationAction]:
        actions: List[GPUOptimizationAction] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")
            gpu_util = r.get("gpu_utilization", 0.0)
            total_gpus = r.get("total_gpus", 0)

            if gpu_util < self._underutil and total_gpus > 0:
                actions.append(GPUOptimizationAction(
                    action_id=f"gpu-consolidate-{region_id}-{int(ts)}",
                    region_id=region_id,
                    action_type="consolidate",
                    current_utilization=round(gpu_util, 3),
                    target_utilization=self._target,
                    workloads_affected=max(1, int(total_gpus * (1 - gpu_util))),
                    expected_improvement_pct=round((self._target - gpu_util) * 100, 1),
                    created_at=ts,
                ))
            elif gpu_util > self._overutil:
                actions.append(GPUOptimizationAction(
                    action_id=f"gpu-offload-{region_id}-{int(ts)}",
                    region_id=region_id,
                    action_type="offload",
                    current_utilization=round(gpu_util, 3),
                    target_utilization=self._target,
                    workloads_affected=max(1, int(total_gpus * (gpu_util - self._target))),
                    expected_improvement_pct=round((gpu_util - self._target) * 100, 1),
                    created_at=ts,
                ))

            gpus = r.get("gpus", [])
            for gpu in gpus:
                gpu_id = gpu.get("gpu_id", "")
                util = gpu.get("utilization", 0.0)
                if util > self._overutil:
                    actions.append(GPUOptimizationAction(
                        action_id=f"gpu-rebalance-{gpu_id}-{int(ts)}",
                        region_id=region_id,
                        gpu_id=gpu_id,
                        action_type="rebalance",
                        current_utilization=round(util, 3),
                        target_utilization=self._target,
                        expected_improvement_pct=round((util - self._target) * 100, 1),
                        created_at=ts,
                    ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute(self, action: GPUOptimizationAction) -> GPUOptimizationAction:
        action.executed = True
        with self._lock:
            self._total_optimizations += 1
            self._events.append({
                "type": "gpu_optimization_executed",
                "action_id": action.action_id,
                "action_type": action.action_type,
                "region": action.region_id,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        logger.info("GPU optimization %s on %s: %s", action.action_type, action.region_id, action.action_id)
        return action

    def optimize(self, telemetry: Dict[str, Any]) -> List[GPUOptimizationAction]:
        actions = self.analyze(telemetry)
        for a in actions:
            self.execute(a)
        return actions

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
                "pending_actions": sum(1 for a in self._actions if not a.executed),
            }
