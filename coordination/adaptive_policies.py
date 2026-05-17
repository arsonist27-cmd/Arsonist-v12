"""v15 Adaptive Global Policies.

Policies that dynamically adapt based on traffic patterns, failures,
latency, energy cost, and regional instability. Supports automatic
policy adjustment and threshold tuning.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("coordination.adaptive_policies")


class PolicyType(str, Enum):
    scaling = "scaling"
    routing = "routing"
    failover = "failover"
    energy = "energy"
    carbon = "carbon"
    thermal = "thermal"
    cost = "cost"
    security = "security"


class PolicyMode(str, Enum):
    conservative = "conservative"
    balanced = "balanced"
    aggressive = "aggressive"
    emergency = "emergency"


class AdaptivePolicy(BaseModel):
    policy_id: str
    policy_type: PolicyType = PolicyType.scaling
    mode: PolicyMode = PolicyMode.balanced
    enabled: bool = True
    thresholds: Dict[str, float] = Field(default_factory=dict)
    weights: Dict[str, float] = Field(default_factory=dict)
    conditions: Dict[str, Any] = Field(default_factory=dict)
    actions: List[str] = Field(default_factory=list)
    triggers_count: int = 0
    last_triggered: float = 0.0
    last_adapted: float = 0.0
    created_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PolicyEvaluation(BaseModel):
    policy_id: str
    triggered: bool = False
    mode_before: str = ""
    mode_after: str = ""
    adjustments: Dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    ts: float = 0.0


class AdaptivePolicyManager:
    """Manages adaptive global policies that dynamically adjust based
    on real-time infrastructure conditions."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._policies: Dict[str, AdaptivePolicy] = {}
        self._evaluations: List[PolicyEvaluation] = []
        self._total_evaluations = 0
        self._total_adaptations = 0
        self._events: List[Dict[str, Any]] = []
        self._init_default_policies()

    def _init_default_policies(self) -> None:
        defaults = [
            AdaptivePolicy(
                policy_id="scaling-auto",
                policy_type=PolicyType.scaling,
                thresholds={"scale_up": 0.8, "scale_down": 0.3, "cooldown_s": 60},
                actions=["scale_up", "scale_down", "rebalance"],
            ),
            AdaptivePolicy(
                policy_id="routing-latency",
                policy_type=PolicyType.routing,
                thresholds={"max_latency_ms": 200, "reroute_threshold_ms": 400, "failover_threshold_ms": 800},
                actions=["reroute", "failover", "degrade"],
            ),
            AdaptivePolicy(
                policy_id="failover-auto",
                policy_type=PolicyType.failover,
                thresholds={"health_threshold": 0.5, "region_offline_pct": 0.5, "recovery_timeout_s": 120},
                actions=["isolate", "reroute", "recover"],
            ),
            AdaptivePolicy(
                policy_id="energy-green",
                policy_type=PolicyType.energy,
                thresholds={"max_carbon_intensity": 0.6, "min_renewable_pct": 0.3, "cost_ceiling": 100},
                actions=["shift_to_green", "throttle_brown", "schedule_off_peak"],
            ),
            AdaptivePolicy(
                policy_id="thermal-safety",
                policy_type=PolicyType.thermal,
                thresholds={"warning_temp_c": 75, "critical_temp_c": 85, "throttle_pressure": 0.8},
                actions=["throttle", "migrate", "shutdown"],
            ),
            AdaptivePolicy(
                policy_id="cost-optimize",
                policy_type=PolicyType.cost,
                thresholds={"max_cost_per_hour": 150, "spot_threshold": 0.5, "idle_timeout_s": 300},
                actions=["use_spot", "consolidate", "terminate_idle"],
            ),
        ]
        for p in defaults:
            p.created_at = now_ts()
            self._policies[p.policy_id] = p

    def register_policy(self, policy: AdaptivePolicy) -> None:
        with self._lock:
            policy.created_at = now_ts()
            self._policies[policy.policy_id] = policy

    def remove_policy(self, policy_id: str) -> bool:
        with self._lock:
            return self._policies.pop(policy_id, None) is not None

    def evaluate_all(self, telemetry: Dict[str, Any]) -> List[PolicyEvaluation]:
        results = []
        with self._lock:
            policies = list(self._policies.values())

        for policy in policies:
            if not policy.enabled:
                continue
            evaluation = self._evaluate_policy(policy, telemetry)
            if evaluation:
                results.append(evaluation)

        with self._lock:
            self._evaluations.extend(results)
            if len(self._evaluations) > self._max_history:
                self._evaluations = self._evaluations[-self._max_history:]
            self._total_evaluations += len(results)

        return results

    def _evaluate_policy(self, policy: AdaptivePolicy, telemetry: Dict[str, Any]) -> Optional[PolicyEvaluation]:
        regions = telemetry.get("regions", [])
        if not regions:
            return None

        triggered = False
        reason = ""
        adjustments: Dict[str, Any] = {}
        old_mode = policy.mode.value

        if policy.policy_type == PolicyType.scaling:
            avg_sat = sum(r.get("workload_saturation", 0) for r in regions) / len(regions)
            scale_up = policy.thresholds.get("scale_up", 0.8)
            scale_down = policy.thresholds.get("scale_down", 0.3)
            if avg_sat > scale_up:
                triggered = True
                reason = f"Avg saturation {avg_sat:.0%} > {scale_up:.0%}"
                adjustments["action"] = "scale_up"
                if avg_sat > 0.95:
                    policy.mode = PolicyMode.emergency
                elif avg_sat > scale_up:
                    policy.mode = PolicyMode.aggressive
            elif avg_sat < scale_down:
                triggered = True
                reason = f"Avg saturation {avg_sat:.0%} < {scale_down:.0%}"
                adjustments["action"] = "scale_down"
                policy.mode = PolicyMode.conservative

        elif policy.policy_type == PolicyType.routing:
            avg_lat = sum(r.get("avg_latency_ms", 0) for r in regions) / len(regions)
            max_lat = policy.thresholds.get("max_latency_ms", 200)
            if avg_lat > max_lat:
                triggered = True
                reason = f"Avg latency {avg_lat:.0f}ms > {max_lat:.0f}ms"
                adjustments["action"] = "reroute"

        elif policy.policy_type == PolicyType.failover:
            offline = sum(1 for r in regions if r.get("status") == "offline")
            threshold = policy.thresholds.get("region_offline_pct", 0.5)
            if offline / len(regions) > threshold:
                triggered = True
                reason = f"{offline}/{len(regions)} regions offline"
                adjustments["action"] = "failover"
                policy.mode = PolicyMode.emergency

        elif policy.policy_type == PolicyType.energy:
            avg_carbon = sum(r.get("carbon_intensity", 0.5) for r in regions) / len(regions)
            max_carbon = policy.thresholds.get("max_carbon_intensity", 0.6)
            if avg_carbon > max_carbon:
                triggered = True
                reason = f"Avg carbon intensity {avg_carbon:.2f} > {max_carbon:.2f}"
                adjustments["action"] = "shift_to_green"

        elif policy.policy_type == PolicyType.thermal:
            high_thermal = [r for r in regions if r.get("thermal_pressure", 0) > policy.thresholds.get("throttle_pressure", 0.8)]
            if high_thermal:
                triggered = True
                reason = f"{len(high_thermal)} regions above thermal threshold"
                adjustments["action"] = "throttle"
                adjustments["affected_regions"] = [r.get("region_id", "") for r in high_thermal]

        elif policy.policy_type == PolicyType.cost:
            avg_cost = sum(r.get("cost_per_hour", 0) for r in regions) / len(regions)
            max_cost = policy.thresholds.get("max_cost_per_hour", 150)
            if avg_cost > max_cost:
                triggered = True
                reason = f"Avg cost ${avg_cost:.0f}/hr > ${max_cost:.0f}/hr"
                adjustments["action"] = "consolidate"

        if not triggered:
            return None

        with self._lock:
            policy.triggers_count += 1
            policy.last_triggered = now_ts()
            if old_mode != policy.mode.value:
                policy.last_adapted = now_ts()
                self._total_adaptations += 1

        return PolicyEvaluation(
            policy_id=policy.policy_id,
            triggered=True,
            mode_before=old_mode,
            mode_after=policy.mode.value,
            adjustments=adjustments,
            reason=reason,
            ts=now_ts(),
        )

    def get_policy(self, policy_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            p = self._policies.get(policy_id)
            return p.model_dump(mode="json") if p else None

    def all_policies(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._policies.values()]

    def recent_evaluations(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.model_dump(mode="json") for e in reversed(self._evaluations)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_policies": len(self._policies),
                "enabled_policies": sum(1 for p in self._policies.values() if p.enabled),
                "total_evaluations": self._total_evaluations,
                "total_adaptations": self._total_adaptations,
            }
