from __future__ import annotations

from typing import Optional

from gpu.gpu_discovery import GpuDiscovery
from gpu.vram_manager import VramManager


class GpuScheduler:
    """Scores GPUs for a workload using VRAM, utilization, temperature, and hints."""

    def __init__(self, discovery: GpuDiscovery, vram: VramManager) -> None:
        self.discovery = discovery
        self.vram = vram

    def pick_gpu(self, required_vram_mb: int, tensor_tokens: int, prefer_quantization: str) -> Optional[int]:
        devices = self.discovery.discover()
        best: tuple[float, int] | None = None
        for g in devices:
            effective_free = g.free_vram_mb - self.vram.reserved(g.index)
            if effective_free < required_vram_mb:
                continue
            temp_penalty = max(0.0, g.temperature_c - 80.0) * 5.0
            util_penalty = g.utilization_pct * 2.0
            tok_penalty = min(tensor_tokens / 100_000.0, 50.0)
            score = float(effective_free) - temp_penalty - util_penalty - tok_penalty
            if prefer_quantization != "none":
                score += 128.0
            if best is None or score > best[0]:
                best = (score, g.index)
        return best[1] if best else None
