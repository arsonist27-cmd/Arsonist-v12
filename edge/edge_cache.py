from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("edge.cache")


class EdgeCacheEntry:
    def __init__(self, key: str, value: Any, size_bytes: int = 0, ttl_sec: float = 3600.0) -> None:
        self.key = key
        self.value = value
        self.size_bytes = size_bytes
        self.ttl_sec = ttl_sec
        self.created_at = now_ts()
        self.accessed_at = self.created_at
        self.access_count = 0

    def is_expired(self) -> bool:
        return now_ts() - self.created_at > self.ttl_sec


class EdgeCache:
    """Local inference cache for edge nodes with LRU eviction and size limits."""

    def __init__(self, max_entries: int = 5000, max_size_bytes: int = 1024 * 1024 * 1024) -> None:
        self._lock = threading.Lock()
        self._entries: Dict[str, EdgeCacheEntry] = {}
        self.max_entries = max_entries
        self.max_size_bytes = max_size_bytes
        self._current_size = 0
        self._hit_count = 0
        self._miss_count = 0
        self._eviction_count = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                self._miss_count += 1
                return None
            if entry.is_expired():
                self._remove_entry(key)
                self._miss_count += 1
                return None
            entry.accessed_at = now_ts()
            entry.access_count += 1
            self._hit_count += 1
            return entry.value

    def put(self, key: str, value: Any, size_bytes: int = 0, ttl_sec: float = 3600.0) -> None:
        with self._lock:
            if key in self._entries:
                self._remove_entry(key)
            while len(self._entries) >= self.max_entries or (
                self._current_size + size_bytes > self.max_size_bytes and self._entries
            ):
                self._evict_lru()
            entry = EdgeCacheEntry(key=key, value=value, size_bytes=size_bytes, ttl_sec=ttl_sec)
            self._entries[key] = entry
            self._current_size += size_bytes

    def delete(self, key: str) -> bool:
        with self._lock:
            if key not in self._entries:
                return False
            self._remove_entry(key)
            return True

    def clear(self) -> int:
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            self._current_size = 0
            return count

    def cleanup_expired(self) -> int:
        with self._lock:
            expired = [k for k, v in self._entries.items() if v.is_expired()]
            for k in expired:
                self._remove_entry(k)
            return len(expired)

    def _remove_entry(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry:
            self._current_size -= entry.size_bytes

    def _evict_lru(self) -> None:
        if not self._entries:
            return
        lru_key = min(self._entries, key=lambda k: self._entries[k].accessed_at)
        self._remove_entry(lru_key)
        self._eviction_count += 1

    def hot_keys(self, limit: int = 20) -> List[str]:
        with self._lock:
            sorted_entries = sorted(
                self._entries.items(),
                key=lambda x: x[1].access_count,
                reverse=True,
            )
            return [k for k, _ in sorted_entries[:limit]]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hit_count + self._miss_count
            return {
                "ts": now_ts(),
                "total_entries": len(self._entries),
                "current_size_bytes": self._current_size,
                "max_entries": self.max_entries,
                "max_size_bytes": self.max_size_bytes,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": round(self._hit_count / total, 4) if total else 0.0,
                "eviction_count": self._eviction_count,
            }
