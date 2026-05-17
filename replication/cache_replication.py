from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from shared.utils import now_ts, setup_logging

logger = setup_logging("replication.cache")


class CacheEntryType(str, Enum):
    model = "model"
    tokenizer = "tokenizer"
    embedding = "embedding"
    inference_output = "inference_output"


class CacheEntry:
    def __init__(
        self,
        key: str,
        entry_type: CacheEntryType,
        region_id: str,
        size_bytes: int = 0,
        ttl_sec: float = 3600.0,
    ) -> None:
        self.key = key
        self.entry_type = entry_type
        self.region_id = region_id
        self.size_bytes = size_bytes
        self.ttl_sec = ttl_sec
        self.created_at = now_ts()
        self.last_accessed = self.created_at
        self.access_count = 0
        self.invalidated = False

    def is_expired(self) -> bool:
        return now_ts() - self.created_at > self.ttl_sec

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "entry_type": self.entry_type.value,
            "region_id": self.region_id,
            "size_bytes": self.size_bytes,
            "ttl_sec": self.ttl_sec,
            "created_at": self.created_at,
            "last_accessed": self.last_accessed,
            "access_count": self.access_count,
            "invalidated": self.invalidated,
        }


class DistributedCacheFabric:
    """Distributed cache with regional invalidation, synchronization, and warming."""

    def __init__(
        self,
        local_region: str,
        max_size_bytes: int = 10 * 1024 * 1024 * 1024,
        warming_threshold: int = 5,
        on_invalidate: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.local_region = local_region
        self.max_size_bytes = max_size_bytes
        self.warming_threshold = warming_threshold
        self._on_invalidate = on_invalidate
        self._lock = threading.RLock()
        self._entries: Dict[str, CacheEntry] = {}
        self._current_size = 0
        self._hit_count = 0
        self._miss_count = 0
        self._eviction_count = 0
        self._invalidation_count = 0
        self._warm_candidates: Set[str] = set()

    def put(self, key: str, entry_type: CacheEntryType, size_bytes: int = 0, ttl_sec: float = 3600.0) -> CacheEntry:
        entry = CacheEntry(
            key=key,
            entry_type=entry_type,
            region_id=self.local_region,
            size_bytes=size_bytes,
            ttl_sec=ttl_sec,
        )
        with self._lock:
            while self._current_size + size_bytes > self.max_size_bytes and self._entries:
                self._evict_lru()
            old = self._entries.get(key)
            if old:
                self._current_size -= old.size_bytes
            self._entries[key] = entry
            self._current_size += size_bytes
        return entry

    def get(self, key: str) -> Optional[CacheEntry]:
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                self._miss_count += 1
                return None
            if entry.is_expired() or entry.invalidated:
                del self._entries[key]
                self._current_size -= entry.size_bytes
                self._miss_count += 1
                return None
            entry.last_accessed = now_ts()
            entry.access_count += 1
            self._hit_count += 1
            if entry.access_count >= self.warming_threshold:
                self._warm_candidates.add(key)
            return entry

    def invalidate(self, key: str, propagate: bool = True) -> bool:
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return False
            entry.invalidated = True
            self._invalidation_count += 1
        if propagate and self._on_invalidate:
            try:
                self._on_invalidate(key, self.local_region)
            except Exception:
                logger.exception("Invalidation callback error for key %s", key)
        return True

    def invalidate_by_type(self, entry_type: CacheEntryType, propagate: bool = True) -> int:
        count = 0
        with self._lock:
            for entry in self._entries.values():
                if entry.entry_type == entry_type and not entry.invalidated:
                    entry.invalidated = True
                    count += 1
                    self._invalidation_count += 1
        if propagate and self._on_invalidate and count > 0:
            try:
                self._on_invalidate(f"type:{entry_type.value}", self.local_region)
            except Exception:
                logger.exception("Bulk invalidation callback error")
        return count

    def receive_invalidation(self, key: str, source_region: str) -> None:
        if source_region == self.local_region:
            return
        self.invalidate(key, propagate=False)

    def get_warm_candidates(self) -> List[str]:
        with self._lock:
            candidates = list(self._warm_candidates)
            self._warm_candidates.clear()
            return candidates

    def cleanup_expired(self) -> int:
        removed = 0
        with self._lock:
            expired_keys = [k for k, v in self._entries.items() if v.is_expired() or v.invalidated]
            for k in expired_keys:
                entry = self._entries.pop(k)
                self._current_size -= entry.size_bytes
                removed += 1
                self._eviction_count += 1
        return removed

    def _evict_lru(self) -> None:
        if not self._entries:
            return
        lru_key = min(self._entries, key=lambda k: self._entries[k].last_accessed)
        entry = self._entries.pop(lru_key)
        self._current_size -= entry.size_bytes
        self._eviction_count += 1

    def entries_by_type(self) -> Dict[str, int]:
        with self._lock:
            counts: Dict[str, int] = {}
            for entry in self._entries.values():
                counts[entry.entry_type.value] = counts.get(entry.entry_type.value, 0) + 1
            return counts

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hit_count + self._miss_count
            return {
                "ts": now_ts(),
                "region": self.local_region,
                "total_entries": len(self._entries),
                "current_size_bytes": self._current_size,
                "max_size_bytes": self.max_size_bytes,
                "utilization": round(self._current_size / self.max_size_bytes, 4) if self.max_size_bytes else 0,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": round(self._hit_count / total, 4) if total else 0.0,
                "eviction_count": self._eviction_count,
                "invalidation_count": self._invalidation_count,
                "warm_candidates": len(self._warm_candidates),
                "by_type": self.entries_by_type(),
            }
