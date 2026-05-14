from __future__ import annotations

import os
from typing import Any, Dict, Optional

from orchestrator.container_runtime import ContainerRuntime
from orchestrator.runtime_manager import RuntimeManager
from shared.ai_workloads import AIWorkloadKind, WorkloadSpec
from containers.sandbox_profiles import profile_for_workload


class WorkloadController:
    """Maps AI workload specs to container runs (non-destructive to v8 job path)."""

    def __init__(self, runtime_mgr: RuntimeManager) -> None:
        self.runtime_mgr = runtime_mgr
        self.container = runtime_mgr.runtime

    def execute_container_phase(self, spec: WorkloadSpec, extra_env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        profile = profile_for_workload(spec)
        image = spec.container_image or os.getenv("ARSONIST_DEFAULT_WORKLOAD_IMAGE", "python:3.11-slim")
        cmd = spec.entrypoint or ["python", "-c", "print('noop workload')"]
        env = {"ARSONIST_WORKLOAD": spec.kind.value, **(extra_env or {})}
        return self.container.run_workload(image=image, command=list(cmd), env=env, profile=profile, labels={"workload": spec.kind.value}, timeout_sec=spec.timeout_sec)
