from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from shared.ai_workloads import WorkloadSpec


class NetworkPolicy(str, Enum):
    default = "default"
    none = "none"


@dataclass
class SandboxProfile:
    read_only_root: bool = False
    network: NetworkPolicy = NetworkPolicy.default
    memory_mb: Optional[int] = None
    cpu_quota_cpus: Optional[float] = None
    gpu_device_requests: List[str] = field(default_factory=list)


def profile_for_workload(spec: WorkloadSpec) -> SandboxProfile:
    mem = min(max(spec.vram_mb_estimate + 2048, 512), 131072)
    return SandboxProfile(
        read_only_root=True,
        network=NetworkPolicy.none if spec.kind.value in ("batch_generation", "fine_tuning") else NetworkPolicy.default,
        memory_mb=mem,
        cpu_quota_cpus=2.0 if spec.gpu_required else 1.0,
        gpu_device_requests=spec.metadata.get("gpu_devices") or [],
    )
