from __future__ import annotations

from typing import List

from models.registry import ModelRecord


def compatible_gpus(rec: ModelRecord, offered: List[str]) -> List[str]:
    if not rec.supported_gpu_types:
        return offered
    return [g for g in offered if g in rec.supported_gpu_types]
