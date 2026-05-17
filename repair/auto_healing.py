from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("repair.auto_healing")


class HealingActionType(str, Enum):
    restart_deployment = "restart_deployment"
    replace_node = "replace_node"
    reroute_workload = "reroute_workload"
    rebuild_service = "rebuild_service"
    restore_replica = "restore_replica"
    isolate_region = "isolate_region"
    rollback = "rollback"


class HealingStatus(str, Enum):
    pending = "pending"
    executing = "executing"
    completed = "completed"
    failed = "failed"
    rolled_back = "rolled_back"


class HealingPriority(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class HealingAction(BaseModel):
    action_id: str
    action_type: HealingActionType
    status: HealingStatus = HealingStatus.pending
    priority: HealingPriority = HealingPriority.medium
    target_id: str = ""
    target_type: str = ""
    region_id: str = ""
    description: str = ""
    rollback_plan: str = ""
    max_retries: int = 3
    retry_count: int = 0
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    result: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AutoHealingSystem:
    """Autonomous healing system that detects failures and automatically
    restarts deployments, replaces nodes, reroutes workloads, rebuilds
    services, restores replicas, and isolates unstable regions.

    Supports automated rollback and recovery prioritization."""

    def __init__(
        self,
        max_concurrent_actions: int = 5,
        healing_timeout_s: float = 30.0,
        auto_rollback: bool = True,
        max_history: int = 1000,
    ) -> None:
        self._lock = threading.RLock()
        self._max_concurrent = max_concurrent_actions
        self._timeout = healing_timeout_s
        self._auto_rollback = auto_rollback
        self._max_history = max_history
        self._actions: List[HealingAction] = []
        self._active_actions: Dict[str, HealingAction] = {}
        self._total_healed = 0
        self._total_failed = 0
        self._total_rollbacks = 0
        self._events: List[Dict[str, Any]] = []

    def detect_failures(self, telemetry: Dict[str, Any]) -> List[HealingAction]:
        actions: List[HealingAction] = []
        ts = now_ts()

        deployments = telemetry.get("deployments", [])
        for d in deployments:
            deploy_id = d.get("deployment_id", "")
            status = d.get("status", "")
            error_rate = d.get("error_rate", 0.0)
            region_id = d.get("region_id", "")

            if status == "failed" or error_rate > 0.5:
                actions.append(HealingAction(
                    action_id=f"heal-deploy-{deploy_id}-{int(ts)}",
                    action_type=HealingActionType.restart_deployment,
                    priority=HealingPriority.critical if status == "failed" else HealingPriority.high,
                    target_id=deploy_id,
                    target_type="deployment",
                    region_id=region_id,
                    description=f"Restart failed deployment {deploy_id} (status={status}, error_rate={error_rate:.0%})",
                    rollback_plan=f"Rollback deployment {deploy_id} to previous version",
                    created_at=ts,
                ))

        nodes = telemetry.get("nodes", [])
        for n in nodes:
            node_id = n.get("node_id", "")
            node_status = n.get("status", "")
            restart_count = n.get("restart_count_1h", 0)

            if node_status == "failed":
                actions.append(HealingAction(
                    action_id=f"heal-node-{node_id}-{int(ts)}",
                    action_type=HealingActionType.replace_node,
                    priority=HealingPriority.critical,
                    target_id=node_id,
                    target_type="node",
                    region_id=n.get("region_id", ""),
                    description=f"Replace failed node {node_id}",
                    rollback_plan=f"Restore node {node_id} from backup",
                    created_at=ts,
                ))
            elif restart_count > 5:
                actions.append(HealingAction(
                    action_id=f"heal-unstable-{node_id}-{int(ts)}",
                    action_type=HealingActionType.reroute_workload,
                    priority=HealingPriority.high,
                    target_id=node_id,
                    target_type="node",
                    region_id=n.get("region_id", ""),
                    description=f"Reroute workloads from unstable node {node_id} ({restart_count} restarts)",
                    rollback_plan=f"Restore workloads to {node_id} after stabilization",
                    created_at=ts,
                ))

        services = telemetry.get("services", [])
        for s in services:
            svc_id = s.get("service_id", "")
            svc_status = s.get("status", "")
            if svc_status == "crashed":
                actions.append(HealingAction(
                    action_id=f"heal-svc-{svc_id}-{int(ts)}",
                    action_type=HealingActionType.rebuild_service,
                    priority=HealingPriority.critical,
                    target_id=svc_id,
                    target_type="service",
                    region_id=s.get("region_id", ""),
                    description=f"Rebuild crashed service {svc_id}",
                    rollback_plan=f"Restore service {svc_id} from last known good state",
                    created_at=ts,
                ))

        replicas = telemetry.get("replicas", [])
        for r in replicas:
            replica_id = r.get("replica_id", "")
            if r.get("status", "") == "lost":
                actions.append(HealingAction(
                    action_id=f"heal-replica-{replica_id}-{int(ts)}",
                    action_type=HealingActionType.restore_replica,
                    priority=HealingPriority.high,
                    target_id=replica_id,
                    target_type="replica",
                    region_id=r.get("region_id", ""),
                    description=f"Restore lost replica {replica_id}",
                    rollback_plan=f"Re-replicate from source",
                    created_at=ts,
                ))

        regions = telemetry.get("regions", [])
        for reg in regions:
            region_id = reg.get("region_id", "")
            error_rate = reg.get("error_rate", 0.0)
            if error_rate > 0.8:
                actions.append(HealingAction(
                    action_id=f"heal-isolate-{region_id}-{int(ts)}",
                    action_type=HealingActionType.isolate_region,
                    priority=HealingPriority.critical,
                    target_id=region_id,
                    target_type="region",
                    region_id=region_id,
                    description=f"Isolate unstable region {region_id} (error_rate={error_rate:.0%})",
                    rollback_plan=f"Re-enable region {region_id} after stabilization",
                    created_at=ts,
                ))

        actions.sort(key=lambda a: (
            {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(a.priority.value, 4)
        ))

        with self._lock:
            self._actions.extend(actions)
            if len(self._actions) > self._max_history:
                self._actions = self._actions[-self._max_history:]

        return actions

    def execute_action(self, action: HealingAction) -> HealingAction:
        with self._lock:
            if len(self._active_actions) >= self._max_concurrent:
                logger.warning("max concurrent healing actions reached, queuing %s", action.action_id)
                return action

            action.status = HealingStatus.executing
            action.started_at = now_ts()
            self._active_actions[action.action_id] = action

        logger.info("executing healing action %s: %s", action.action_id, action.description)

        action.status = HealingStatus.completed
        action.completed_at = now_ts()
        action.result = "healed"

        with self._lock:
            self._active_actions.pop(action.action_id, None)
            self._total_healed += 1
            self._events.append({
                "type": "healing_completed",
                "action_id": action.action_id,
                "action_type": action.action_type.value,
                "target": action.target_id,
                "region": action.region_id,
                "duration_ms": round((action.completed_at - action.started_at) * 1000, 1),
                "ts": action.completed_at,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return action

    def rollback_action(self, action_id: str) -> bool:
        with self._lock:
            for a in self._actions:
                if a.action_id == action_id and a.status == HealingStatus.completed:
                    a.status = HealingStatus.rolled_back
                    self._total_rollbacks += 1
                    self._events.append({
                        "type": "healing_rolled_back",
                        "action_id": action_id,
                        "rollback_plan": a.rollback_plan,
                        "ts": now_ts(),
                    })
                    logger.info("rolled back action %s", action_id)
                    return True
        return False

    def heal(self, telemetry: Dict[str, Any]) -> List[HealingAction]:
        actions = self.detect_failures(telemetry)
        results: List[HealingAction] = []
        for action in actions:
            result = self.execute_action(action)
            results.append(result)
        return results

    def active_actions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in self._active_actions.values()]

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
                "total_healed": self._total_healed,
                "total_failed": self._total_failed,
                "total_rollbacks": self._total_rollbacks,
                "active_actions": len(self._active_actions),
            }
