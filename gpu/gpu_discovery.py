from __future__ import annotations

import os
import subprocess
import threading
from dataclasses import dataclass
from typing import List

from shared.utils import now_ts


@dataclass
class GpuDevice:
    index: int
    name: str
    total_vram_mb: int
    free_vram_mb: int
    temperature_c: float
    utilization_pct: float
    compute_capability: str = "unknown"


class GpuDiscovery:
    """Best-effort nvidia-smi discovery; falls back to env overrides for CI."""

    def discover(self) -> List[GpuDevice]:
        if os.getenv("ARSONIST_GPU_MOCK_COUNT"):
            n = int(os.getenv("ARSONIST_GPU_MOCK_COUNT", "0"))
            return [
                GpuDevice(
                    index=i,
                    name="mock-gpu",
                    total_vram_mb=24_576,
                    free_vram_mb=20_000,
                    temperature_c=55.0,
                    utilization_pct=10.0,
                    compute_capability="8.9",
                )
                for i in range(n)
            ]
        try:
            fmt = (
                "gpu_name,memory.total,memory.free,temperature.gpu,utilization.gpu,"
                "compute_cap.major,compute_cap.minor"
            )
            proc = subprocess.run(
                ["nvidia-smi", "--query-gpu=" + fmt, "--format=csv,noheader,nounits"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if proc.returncode != 0:
                return []
            devices: List[GpuDevice] = []
            for idx, line in enumerate(proc.stdout.strip().splitlines()):
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 5:
                    continue
                name, total_mib, free_mib, temp, util = parts[0], parts[1], parts[2], parts[3], parts[4]
                major = parts[5] if len(parts) > 5 else "0"
                minor = parts[6] if len(parts) > 6 else "0"
                devices.append(
                    GpuDevice(
                        index=idx,
                        name=name,
                        total_vram_mb=int(float(total_mib)),
                        free_vram_mb=int(float(free_mib)),
                        temperature_c=float(temp),
                        utilization_pct=float(util),
                        compute_capability=f"{major}.{minor}",
                    )
                )
            return devices
        except FileNotFoundError:
            return []
