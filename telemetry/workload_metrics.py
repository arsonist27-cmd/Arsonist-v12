from __future__ import annotations

import threading
from typing import Any, Dict

from shared.utils import now_ts


class WorkloadMetrics:
    """Counters for orchestration workloads (deployments, rollouts)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.deployments_created = 0
        self.rollouts_started = 0
        self.failures = 0

    def inc_deployment(self) -> None:
        with self._lock:
            self.deployments_created += 1

    def inc_rollout(self) -> None:
        with self._lock:
            self.rollouts_started += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "deployments_created": self.deployments_created,
                "rollouts_started": self.rollouts_started,
                "failures": self.failures,
            }
