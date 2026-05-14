from __future__ import annotations

import threading
from collections import deque
from typing import Deque, Optional


class TokenizerPool:
    """Lightweight pool placeholder for tokenizer handles (real binding would use HF tokenizers)."""

    def __init__(self, size: int = 4) -> None:
        self._lock = threading.Lock()
        self._pool: Deque[str] = deque([f"tok-{i}" for i in range(size)])

    def acquire(self, timeout_sec: float = 5.0) -> Optional[str]:
        with self._lock:
            return self._pool.popleft() if self._pool else None

    def release(self, handle: str) -> None:
        with self._lock:
            self._pool.append(handle)
