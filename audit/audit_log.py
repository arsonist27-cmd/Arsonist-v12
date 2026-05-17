from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_redis = None


def _redis_client():
    global _redis
    url = os.getenv("ARSONIST_REDIS_URL", "").strip()
    if not url:
        return None
    if _redis is False:
        return None
    if _redis is None:
        try:
            import redis

            r = redis.Redis.from_url(url, decode_responses=True)
            r.ping()
            _redis = r
        except Exception:
            _redis = False
            return None
    return _redis


class AuditLog:
    def __init__(self, max_memory: int = 10_000) -> None:
        self._lock = threading.RLock()
        self._rows: List[Dict[str, Any]] = []
        self._max = max_memory

    def append(self, event: Dict[str, Any]) -> None:
        row = {
            "ts": datetime.now(timezone.utc).isoformat(),
            **event,
        }
        r = _redis_client()
        if r is not None:
            try:
                r.lpush("v12:audit", json.dumps(row, default=str))
                r.ltrim("v12:audit", 0, 99_999)
            except Exception:
                pass
        with self._lock:
            self._rows.append(row)
            if len(self._rows) > self._max:
                self._rows = self._rows[-self._max :]

    def query(self, org_id: str | None = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self._rows)
        if org_id:
            rows = [x for x in rows if x.get("org_id") == org_id]
        return rows[-limit:]


AUDIT = AuditLog()


def append_audit(**kwargs: Any) -> None:
    AUDIT.append(kwargs)
