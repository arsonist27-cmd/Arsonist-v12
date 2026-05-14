from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.ai_workloads import WorkloadSpec


@dataclass
class ResourceAllocation:
    cpu_cores: float
    memory_mb: int
    gpu_devices: List[str]
    vram_mb: int


class ResourceAllocator:
    """Static + env-driven allocation hints; integrates with GPU scheduler externally."""

    def __init__(self) -> None:
        self.cluster_cpu = float(os.getenv("ARSONIST_CLUSTER_CPU", "16"))
        self.cluster_mem_mb = int(os.getenv("ARSONIST_CLUSTER_MEM_MB", "65536"))

    def allocate(self, spec: WorkloadSpec, available_gpus: int) -> ResourceAllocation:
        cpu = 1.0 if spec.gpu_required else 0.5
        mem = min(spec.vram_mb_estimate + 1024, self.cluster_mem_mb // 4)
        gpus: List[str] = []
        if spec.gpu_required and available_gpus > 0:
            gpus = ["0"]
        return ResourceAllocation(cpu_cores=cpu, memory_mb=mem, gpu_devices=gpus, vram_mb=spec.vram_mb_estimate)
