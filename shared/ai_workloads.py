from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AIWorkloadKind(str, Enum):
    inference = "inference"
    embeddings = "embeddings"
    reranking = "reranking"
    fine_tuning = "fine_tuning"
    batch_generation = "batch_generation"
    streaming_inference = "streaming_inference"


class WorkloadSpec(BaseModel):
    kind: AIWorkloadKind
    model_id: str = ""
    gpu_required: bool = False
    vram_mb_estimate: int = 4096
    latency_target_ms: int = 5000
    scaling_policy: str = "queue_depth"  # queue_depth | rps | latency
    max_concurrency: int = Field(default=4, ge=1, le=256)
    tensor_hint_tokens: int = 2048
    quantization: str = "none"  # none | int8 | int4 | gguf
    container_image: Optional[str] = None
    entrypoint: Optional[List[str]] = None
    timeout_sec: int = Field(default=300, ge=5, le=3600)
    metadata: Dict[str, Any] = Field(default_factory=dict)
