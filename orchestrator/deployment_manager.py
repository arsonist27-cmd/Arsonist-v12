from __future__ import annotations

from typing import Any, Dict, List

from orchestrator.runtime_manager import DeploymentState, RuntimeManager


class DeploymentManager:
    """High-level deployment CRUD on top of RuntimeManager."""

    def __init__(self, runtime: RuntimeManager) -> None:
        self.runtime = runtime

    def create(self, name: str, image: str, replicas: int = 1) -> str:
        return self.runtime.create_deployment(name, image, desired_replicas=replicas)

    def mark_healthy(self, deployment_id: str, healthy: int) -> None:
        self.runtime.set_state(deployment_id, DeploymentState.healthy if healthy > 0 else DeploymentState.degraded)

    def list(self) -> List[Dict[str, Any]]:
        return self.runtime.list_deployments()
