from __future__ import annotations

from enum import Enum


class QuantizationKind(str, Enum):
    none = "none"
    int8 = "int8"
    int4 = "int4"
    gguf = "gguf"


def vram_multiplier(q: QuantizationKind | str) -> float:
    qv = q.value if isinstance(q, QuantizationKind) else str(q)
    return {"none": 1.0, "int8": 0.55, "int4": 0.35, "gguf": 0.4}.get(qv, 1.0)
