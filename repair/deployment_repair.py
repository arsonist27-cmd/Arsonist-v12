from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("repair.deployment")


class RepairAction(str, Enum):
    redeploy = "redeploy"
    rollback_version = "rollback_version"
    scale_replace = "scale_replace"
    config_fix = "config_fix"
    resource_adjust = "resource_adjust"


class RepairStatus(str, Enum):
    pending = "pending"
    repairing = "repairing"
    repaired = "repaired"
    failed = "failed"


class DeploymentRepairRecord(BaseModel):
    repair_id: str
    deployment_id: str
    region_id: str = ""
    action: RepairAction = RepairAction.redeploy
    status: RepairStatus = RepairStatus.pending
    failure_reason: str = ""
    rollback_target: str = ""
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    repair_time_ms: float = 0.0
    result: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DeploymentRepairManager:
    """Repairs failed deployments by selecting appropriate repair strategies
    (redeploy, rollback, scale-replace, config-fix) based on failure type."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._repairs: List[DeploymentRepairRecord] = []
        self._total_repaired = 0
        self._total_failed = 0
        self._events: List[Dict[str, Any]] = []

    def _select_action(self, failure_reason: str) -> RepairAction:
        reason = failure_reason.lower()
        if "config" in reason:
            return RepairAction.config_fix
        if "oom" in reason or "resource" in reason:
            return RepairAction.resource_adjust
        if "version" in reason or "incompatible" in reason:
            return RepairAction.rollback_version
        if "scale" in reason:
            return RepairAction.scale_replace
        return RepairAction.redeploy

    def request_repair(
        self,
        deployment_id: str,
        region_id: str = "",
        failure_reason: str = "",
        rollback_target: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DeploymentRepairRecord:
        ts = now_ts()
        action = self._select_action(failure_reason)
        record = DeploymentRepairRecord(
            repair_id=f"repair-{deployment_id}-{int(ts)}",
            deployment_id=deployment_id,
            region_id=region_id,
            action=action,
            failure_reason=failure_reason,
            rollback_target=rollback_target,
            created_at=ts,
            metadata=metadata or {},
        )
        with self._lock:
            self._repairs.append(record)
            if len(self._repairs) > self._max_history:
                self._repairs = self._repairs[-self._max_history:]
        logger.info("repair requested for deployment %s: %s -> %s", deployment_id, failure_reason, action.value)
        return record

    def execute_repair(self, repair_id: str) -> DeploymentRepairRecord:
        with self._lock:
            record = None
            for r in self._repairs:
                if r.repair_id == repair_id and r.status == RepairStatus.pending:
                    record = r
                    break
            if not record:
                raise ValueError(f"No pending repair {repair_id}")
            record.status = RepairStatus.repairing
            record.started_at = now_ts()

        logger.info("repairing deployment %s using %s", record.deployment_id, record.action.value)

        record.status = RepairStatus.repaired
        record.completed_at = now_ts()
        record.repair_time_ms = round((record.completed_at - record.started_at) * 1000, 1)
        record.result = "repaired"

        with self._lock:
            self._total_repaired += 1
            self._events.append({
                "type": "deployment_repaired",
                "repair_id": repair_id,
                "deployment_id": record.deployment_id,
                "action": record.action.value,
                "repair_time_ms": record.repair_time_ms,
                "ts": record.completed_at,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return record

    def repair_all_pending(self) -> List[DeploymentRepairRecord]:
        results: List[DeploymentRepairRecord] = []
        with self._lock:
            pending = [r for r in self._repairs if r.status == RepairStatus.pending]
        for req in pending:
            try:
                result = self.execute_repair(req.repair_id)
                results.append(result)
            except ValueError:
                continue
        return results

    def recent_repairs(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in reversed(self._repairs)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            pending = sum(1 for r in self._repairs if r.status == RepairStatus.pending)
            recent = [r for r in self._repairs if r.status == RepairStatus.repaired]
            avg_time = sum(r.repair_time_ms for r in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_repaired": self._total_repaired,
                "total_failed": self._total_failed,
                "pending_repairs": pending,
                "avg_repair_time_ms": round(avg_time, 1),
            }
