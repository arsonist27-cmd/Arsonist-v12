from __future__ import annotations

import time
from typing import Any, Dict

from orchestrator.runtime_manager import DeploymentState, RuntimeManager


class RolloutManager:
    """Rolling updates, canary fraction, rollback hooks."""

    def __init__(self, runtime: RuntimeManager) -> None:
        self.runtime = runtime

    def start_rollout(self, deployment_id: str, new_image: str, canary_fraction: float = 0.1) -> Dict[str, Any]:
        self.runtime.set_state(deployment_id, DeploymentState.deploying)
        return {"deployment_id": deployment_id, "new_image": new_image, "canary_fraction": canary_fraction, "ts": time.time()}

    def validate_health(self, deployment_id: str, healthy_replicas: int, desired: int) -> DeploymentState:
        if healthy_replicas >= desired:
            st = DeploymentState.healthy
        elif healthy_replicas > 0:
            st = DeploymentState.degraded
        else:
            st = DeploymentState.failed
        self.runtime.set_state(deployment_id, st)
        return st

    def rollback(self, deployment_id: str) -> None:
        self.runtime.graceful_shutdown_marker(deployment_id)
        self.runtime.set_state(deployment_id, DeploymentState.rolling_back)
