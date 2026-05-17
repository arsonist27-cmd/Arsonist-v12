from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Tuple

from billing.metering import get_redis_client

_mem_rl: Dict[str, Deque[float]] = defaultdict(deque)
_rl_lock = threading.Lock()


def allow_request(org_id: str, requests_per_sec: float) -> Tuple[bool, float]:
    """
    Per-second request cap. Redis: fixed-window INCR. No Redis: in-process deque (~single worker).
    """
    r = get_redis_client()
    if r is None:
        now = time.monotonic()
        with _rl_lock:
            q = _mem_rl[org_id]
            while q and now - q[0] > 1.0:
                q.popleft()
            n = float(len(q))
            if n >= requests_per_sec:
                return False, n
            q.append(now)
            return True, n + 1.0

    sec = int(time.time())
    key = f"rl:rps:{org_id}:{sec}"
    try:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, 3)
        n, _ = pipe.execute()
        n = float(n)
        if n > requests_per_sec:
            return False, n
        return True, n
    except Exception:
        return True, 0.0
