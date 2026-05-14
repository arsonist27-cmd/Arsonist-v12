from __future__ import annotations

import os

from gpu.gpu_discovery import GpuDiscovery
from gpu.gpu_scheduler import GpuScheduler
from gpu.vram_manager import VramManager


def main() -> None:
    os.environ["ARSONIST_GPU_MOCK_COUNT"] = "2"
    disc = GpuDiscovery()
    vram = VramManager()
    sched = GpuScheduler(disc, vram)
    idx = sched.pick_gpu(required_vram_mb=8000, tensor_tokens=1024, prefer_quantization="none")
    print({"picked": idx, "devices": [d.__dict__ for d in disc.discover()]})


if __name__ == "__main__":
    main()
