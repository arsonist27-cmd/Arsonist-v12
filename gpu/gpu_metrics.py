from __future__ import annotations

import os
from typing import Any, Dict

from gpu.gpu_discovery import GpuDiscovery
from shared.utils import now_ts


class GpuMetrics:
    """Aggregated GPU telemetry for dashboards and autoscaling."""

    def __init__(self, discovery: GpuDiscovery) -> None:
        self.discovery = discovery

    def snapshot(self) -> Dict[str, Any]:
        gpus = self.discovery.discover()
        return {
            "ts": now_ts(),
            "gpus": [g.__dict__ for g in gpus],
            "throughput_tokens_per_s": float(os.getenv("ARSONIST_GPU_TOKENS_PER_S", "0")),
        }
