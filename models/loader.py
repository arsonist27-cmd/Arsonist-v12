from __future__ import annotations

from pathlib import Path
from typing import Optional

from models.registry import ModelRecord


def resolve_model_path(cache_root: Path, rec: ModelRecord) -> Optional[Path]:
    """Resolve on-disk path for a model record (HuggingFace-style or flat)."""
    safe = rec.model_name.replace("/", "_")
    candidate = cache_root / safe / rec.version
    if candidate.exists():
        return candidate
    flat = cache_root / f"{safe}-{rec.version}.bin"
    return flat if flat.exists() else None
