from __future__ import annotations

import os
import threading
from collections import defaultdict

from billing.usage_tracking import TRACKER

_redis: object | None = None
_redis_lock = threading.Lock()
_mem_meter: dict[tuple[str, str], float] = defaultdict(float)
_meter_lock = threading.Lock()


def get_redis_client():
    """Shared Redis sync client for metering / rate limits (optional)."""
    global _redis
    url = os.getenv("ARSONIST_REDIS_URL", "").strip()
    if not url:
        return None
    with _redis_lock:
        if _redis is False:
            return None
        if _redis is None:
            try:
                import redis

                client = redis.Redis.from_url(url, decode_responses=True)
                client.ping()
                _redis = client
            except Exception:
                _redis = False
                return None
        return _redis if _redis is not False else None


def incr_meter(org_id: str, key: str, amount: float = 1.0, ttl_sec: int = 120) -> float:
    """High-volume counters: Redis INCRBYFLOAT, else in-process aggregate + usage_tracking trail."""
    r = get_redis_client()
    if r is None:
        with _meter_lock:
            _mem_meter[(org_id, key)] += amount
            new_val = _mem_meter[(org_id, key)]
        TRACKER.record(org_id, metric=key, amount=amount, unit="count")
        return new_val
    rk = f"meter:{org_id}:{key}"
    try:
        pipe = r.pipeline()
        pipe.incrbyfloat(rk, amount)
        pipe.expire(rk, ttl_sec)
        new_val, _ = pipe.execute()
        return float(new_val)
    except Exception:
        with _meter_lock:
            _mem_meter[(org_id, key)] += amount
            new_val = _mem_meter[(org_id, key)]
        TRACKER.record(org_id, metric=key, amount=amount, unit="count")
        return new_val


def get_meter(org_id: str, key: str) -> float:
    r = get_redis_client()
    if r is None:
        with _meter_lock:
            return float(_mem_meter.get((org_id, key), 0.0))
    rk = f"meter:{org_id}:{key}"
    try:
        v = r.get(rk)
        return float(v or 0.0)
    except Exception:
        with _meter_lock:
            return float(_mem_meter.get((org_id, key), 0.0))
