from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("repair.failure_recovery")


class RecoveryStrategy(str, Enum):
    restart = "restart"
    failover = "failover"
    rebuild = "rebuild"
    rollback = "rollback"
    escalate = "escalate"


class RecoveryStatus(str, Enum):
    detected = "detected"
    recovering = "recovering"
    recovered = "recovered"
    failed = "failed"
    escalated = "escalated"


class FailureRecord(BaseModel):
    failure_id: str
    component_id: str = ""
    component_type: str = ""
    region_id: str = ""
    failure_type: str = ""
    severity: float = 0.0
    strategy: RecoveryStrategy = RecoveryStrategy.restart
    status: RecoveryStatus = RecoveryStatus.detected
    retry_count: int = 0
    max_retries: int = 3
    detected_at: float = 0.0
    recovery_started_at: float = 0.0
    recovered_at: float = 0.0
    recovery_time_ms: float = 0.0
    description: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FailureRecoveryManager:
    """Manages failure detection and recovery with strategy selection,
    retry logic, and escalation for unrecoverable failures."""

    def __init__(self, max_retries: int = 3, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_retries = max_retries
        self._max_history = max_history
        self._failures: List[FailureRecord] = []
        self._active: Dict[str, FailureRecord] = {}
        self._total_recovered = 0
        self._total_escalated = 0
        self._events: List[Dict[str, Any]] = []

    def _select_strategy(self, failure_type: str, severity: float, retry_count: int) -> RecoveryStrategy:
        if retry_count >= self._max_retries:
            return RecoveryStrategy.escalate
        if severity > 0.9:
            return RecoveryStrategy.failover
        if failure_type in ("crash", "oom", "segfault"):
            return RecoveryStrategy.rebuild
        if failure_type in ("config_error", "bad_deploy"):
            return RecoveryStrategy.rollback
        return RecoveryStrategy.restart

    def record_failure(
        self,
        component_id: str,
        component_type: str,
        failure_type: str,
        region_id: str = "",
        severity: float = 0.5,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FailureRecord:
        ts = now_ts()
        strategy = self._select_strategy(failure_type, severity, 0)
        record = FailureRecord(
            failure_id=f"fail-{component_id}-{int(ts)}",
            component_id=component_id,
            component_type=component_type,
            region_id=region_id,
            failure_type=failure_type,
            severity=severity,
            strategy=strategy,
            detected_at=ts,
            description=f"{component_type} {component_id} failed: {failure_type}",
            metadata=metadata or {},
        )
        with self._lock:
            self._failures.append(record)
            self._active[record.failure_id] = record
            if len(self._failures) > self._max_history:
                self._failures = self._failures[-self._max_history:]
            self._events.append({
                "type": "failure_detected",
                "failure_id": record.failure_id,
                "component": component_id,
                "failure_type": failure_type,
                "strategy": strategy.value,
                "ts": ts,
            })
        logger.info("failure detected: %s (%s), strategy: %s", component_id, failure_type, strategy.value)
        return record

    def recover(self, failure_id: str) -> FailureRecord:
        with self._lock:
            record = self._active.get(failure_id)
            if not record:
                raise ValueError(f"No active failure {failure_id}")

            record.status = RecoveryStatus.recovering
            record.recovery_started_at = now_ts()

        logger.info("recovering %s using strategy %s", failure_id, record.strategy.value)

        record.status = RecoveryStatus.recovered
        record.recovered_at = now_ts()
        record.recovery_time_ms = round((record.recovered_at - record.recovery_started_at) * 1000, 1)

        with self._lock:
            self._active.pop(failure_id, None)
            self._total_recovered += 1
            self._events.append({
                "type": "failure_recovered",
                "failure_id": failure_id,
                "strategy": record.strategy.value,
                "recovery_time_ms": record.recovery_time_ms,
                "ts": record.recovered_at,
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return record

    def escalate(self, failure_id: str) -> FailureRecord:
        with self._lock:
            record = self._active.get(failure_id)
            if not record:
                raise ValueError(f"No active failure {failure_id}")
            record.status = RecoveryStatus.escalated
            self._active.pop(failure_id, None)
            self._total_escalated += 1
            self._events.append({
                "type": "failure_escalated",
                "failure_id": failure_id,
                "component": record.component_id,
                "ts": now_ts(),
            })
        logger.warning("escalated failure %s for %s", failure_id, record.component_id)
        return record

    def retry(self, failure_id: str) -> FailureRecord:
        should_escalate = False
        with self._lock:
            record = self._active.get(failure_id)
            if not record:
                raise ValueError(f"No active failure {failure_id}")
            record.retry_count += 1
            record.strategy = self._select_strategy(
                record.failure_type, record.severity, record.retry_count
            )
            if record.strategy == RecoveryStrategy.escalate:
                should_escalate = True
        if should_escalate:
            return self.escalate(failure_id)
        return self.recover(failure_id)

    def active_failures(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in self._active.values()]

    def recent_failures(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.model_dump(mode="json") for f in reversed(self._failures)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = [f for f in self._failures if f.status == RecoveryStatus.recovered]
            avg_recovery = (
                sum(f.recovery_time_ms for f in recent) / len(recent) if recent else 0.0
            )
            return {
                "ts": now_ts(),
                "total_recovered": self._total_recovered,
                "total_escalated": self._total_escalated,
                "active_failures": len(self._active),
                "avg_recovery_time_ms": round(avg_recovery, 1),
            }
