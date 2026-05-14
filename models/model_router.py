from __future__ import annotations

from typing import Any, Dict

from gpu.gpu_scheduler import GpuScheduler
from models.registry import ModelRegistry


class ModelRouter:
    """Routes inference to best local/registered model placement using GPU pressure hints."""

    def __init__(self, registry: ModelRegistry, gpu_scheduler: GpuScheduler) -> None:
        self.registry = registry
        self.gpu_scheduler = gpu_scheduler

    def route(self, model_name: str, required_vram_mb: int, tensor_tokens: int, quantization: str) -> Dict[str, Any]:
        matches = self.registry.search(model_name, limit=8)
        best = None
        best_score = -1e9
        for m in matches:
            gpu_idx = self.gpu_scheduler.pick_gpu(max(required_vram_mb, m.required_vram_mb), tensor_tokens, quantization)
            score = float(m.required_vram_mb) * -0.01 + (5000 if gpu_idx is not None else 0)
            if score > best_score:
                best_score = score
                best = (m, gpu_idx)
        if not best:
            return {"model_id": None, "gpu_index": None, "reason": "no_model_match"}
        rec, gpu = best
        return {"model_id": rec.model_id, "gpu_index": gpu, "record": rec.model_dump(), "score": best_score}
