from __future__ import annotations

import threading
from typing import Dict


class VramManager:
    """Tracks logical VRAM reservations per GPU index."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reserved: Dict[int, int] = {}

    def reserve(self, gpu_index: int, mb: int) -> bool:
        with self._lock:
            cur = self._reserved.get(gpu_index, 0)
            self._reserved[gpu_index] = cur + mb
            return True

    def release(self, gpu_index: int, mb: int) -> None:
        with self._lock:
            cur = self._reserved.get(gpu_index, 0)
            self._reserved[gpu_index] = max(0, cur - mb)

    def reserved(self, gpu_index: int) -> int:
        with self._lock:
            return self._reserved.get(gpu_index, 0)
