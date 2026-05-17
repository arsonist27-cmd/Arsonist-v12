from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("adaptation.adaptive_scaling")


class ScaleDirection(str, Enum):
    up = "up"
    down = "down"
    none = "none"


class ScaleAction(BaseModel):
    action_id: str
    region_id: str = ""
    resource_type: str = ""
    direction: ScaleDirection = ScaleDirection.none
    current_count: int = 0
    target_count: int = 0
    delta: int = 0
    trigger: str = ""
    confidence: float = 0.0
    cooldown_s: float = 60.0
    executed: bool = False
    created_at: float = 0.0
    executed_at: float = 0.0


class AdaptiveScaler:
    """Adaptive autoscaler that adjusts resources based on real-time demand
    and predicted future load, with cooldown periods to prevent flapping."""

    def __init__(
        self,
        scale_up_threshold: float = 0.80,
        scale_down_threshold: float = 0.30,
        cooldown_s: float = 60.0,
        max_scale_step: int = 5,
        max_history: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self._up_threshold = scale_up_threshold
        self._down_threshold = scale_down_threshold
        self._cooldown = cooldown_s
        self._max_step = max_scale_step
        self._max_history = max_history
        self._actions: List[ScaleAction] = []
        self._last_scale: Dict[str, float] = {}
        self._total_scale_ups = 0
        self._total_scale_downs = 0
        self._events: List[Dict[str, Any]] = []

    def _in_cooldown(self, key: str) -> bool:
        last = self._last_scale.get(key, 0.0)
        return (now_ts() - last) < self._cooldown

    def evaluate(self, telemetry: Dict[str, Any]) -> List[ScaleAction]:
        actions: List[ScaleAction] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")

            for resource, usage_key, count_key in [
                ("gpu", "gpu_utilization", "total_gpus"),
                ("workers", "cpu_utilization", "worker_count"),
                ("replicas", "workload_saturation", "replica_count"),
            ]:
                usage = r.get(usage_key, 0.0)
                current = r.get(count_key, 0)
                if current == 0 and resource != "gpu":
                    continue

                key = f"{resource}:{region_id}"
                if self._in_cooldown(key):
                    continue

                if usage > self._up_threshold:
                    delta = min(self._max_step, max(1, int(current * (usage - self._up_threshold))))
                    actions.append(ScaleAction(
                        action_id=f"scale-up-{resource}-{region_id}-{int(ts)}",
                        region_id=region_id,
                        resource_type=resource,
                        direction=ScaleDirection.up,
                        current_count=current,
                        target_count=current + delta,
                        delta=delta,
                        trigger=f"{usage_key}={usage:.2f} > {self._up_threshold}",
                        confidence=min(0.95, usage),
                        cooldown_s=self._cooldown,
                        created_at=ts,
                    ))
                elif usage < self._down_threshold and current > 1:
                    delta = min(self._max_step, max(1, int(current * (self._down_threshold - usage) * 0.5)))
                    delta = min(delta, current - 1)
                    if delta > 0:
                        actions.append(ScaleAction(
                            action_id=f"scale-down-{resource}-{region_id}-{int(ts)}",
                            region_id=region_id,
                            resource_type=resource,
                            direction=ScaleDirection.down,
                            current_count=current,
                            target_count=current - delta,
                            delta=-delta,
                            trigger=f"{usage_key}={usage:.2f} < {self._down_threshold}",
                            confidence=min(0.95, 1.0 - usage),
                            cooldown_s=self._cooldown,
                            created_at=ts,
                        ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute(self, action: ScaleAction) -> ScaleAction:
        key = f"{action.resource_type}:{action.region_id}"
        action.executed = True
        action.executed_at = now_ts()
        with self._lock:
            self._last_scale[key] = action.executed_at
            if action.direction == ScaleDirection.up:
                self._total_scale_ups += 1
            else:
                self._total_scale_downs += 1
            self._events.append({
                "type": "scale_executed",
                "action_id": action.action_id,
                "direction": action.direction.value,
                "resource": action.resource_type,
                "region": action.region_id,
                "delta": action.delta,
                "ts": action.executed_at,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
        logger.info("scaled %s %s in %s by %d", action.resource_type, action.direction.value,
                     action.region_id, action.delta)
        return action

    def scale(self, telemetry: Dict[str, Any]) -> List[ScaleAction]:
        actions = self.evaluate(telemetry)
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
                "total_scale_ups": self._total_scale_ups,
                "total_scale_downs": self._total_scale_downs,
                "active_cooldowns": sum(1 for k, v in self._last_scale.items()
                                        if (now_ts() - v) < self._cooldown),
            }
