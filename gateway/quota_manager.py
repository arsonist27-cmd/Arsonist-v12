from __future__ import annotations

import time
from typing import Tuple

from billing.metering import get_meter, get_redis_client, incr_meter
from billing.subscription_manager import SUBSCRIPTIONS


def _limits(org_id: str) -> dict:
    return SUBSCRIPTIONS.get_subscription(org_id)["limits"]


def consume_tokens(org_id: str, n: float) -> Tuple[bool, str]:
    lim = _limits(org_id)
    tpm = float(lim.get("tokens_per_minute", 1e9))
    minute_key = f"tokens:{int(time.time() // 60)}"
    used = get_meter(org_id, minute_key)
    if used + n > tpm:
        return False, "tokens_per_minute"
    incr_meter(org_id, minute_key, n, ttl_sec=120)
    return True, ""


def check_gpu_budget(org_id: str, seconds: float) -> Tuple[bool, str]:
    lim = _limits(org_id)
    cap = float(lim.get("gpu_seconds_per_day", 1e12))
    day_key = f"gpu_sec:{int(time.time() // 86400)}"
    used = get_meter(org_id, day_key)
    if used + seconds > cap:
        return False, "gpu_seconds_per_day"
    incr_meter(org_id, day_key, seconds, ttl_sec=90000)
    return True, ""


def incr_concurrency(org_id: str) -> None:
    r = get_redis_client()
    if r is None:
        return
    key = f"inflight:{org_id}"
    try:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, 300)
        pipe.execute()
    except Exception:
        pass


def decr_concurrency(org_id: str) -> None:
    r = get_redis_client()
    if r is None:
        return
    key = f"inflight:{org_id}"
    try:
        n = int(r.get(key) or 0)
        if n > 0:
            r.decr(key)
    except Exception:
        pass


def check_concurrency(org_id: str) -> Tuple[bool, str]:
    lim = _limits(org_id)
    max_c = int(lim.get("max_concurrency", 64))
    r = get_redis_client()
    if r is None:
        return True, ""
    key = f"inflight:{org_id}"
    try:
        cur = int(r.get(key) or 0)
        if cur >= max_c:
            return False, "max_concurrency"
        return True, ""
    except Exception:
        return True, ""
