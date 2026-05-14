from __future__ import annotations

import json
import threading
import zlib
from typing import Any, Dict, List, Tuple


class OfflineSyncBuffer:
    """Bandwidth-aware batching of deferred updates (metrics, small payloads)."""

    def __init__(self, max_bytes: int = 48_000) -> None:
        self._lock = threading.Lock()
        self._chunks: List[bytes] = []
        self.max_bytes = max_bytes

    def enqueue(self, payload: Dict[str, Any]) -> None:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        with self._lock:
            self._chunks.append(zlib.compress(raw, level=6))

    def drain(self) -> Tuple[bytes, str]:
        with self._lock:
            merged = b"".join(self._chunks)
            self._chunks.clear()
        if not merged:
            return b"", "identity"
        if len(merged) > self.max_bytes:
            merged = merged[: self.max_bytes]
        return merged, "zlib"
