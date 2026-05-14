from __future__ import annotations

import heapq
import threading
import time
from typing import List, Tuple


class DelayedJobQueue:
    """In-memory min-heap of jobs keyed by deliver time (edge / offline replay)."""

    def __init__(self) -> None:
        self._heap: List[Tuple[float, str]] = []
        self._lock = threading.Lock()

    def push(self, job_id: str, delay_sec: float) -> None:
        deliver = time.time() + max(0.0, delay_sec)
        with self._lock:
            heapq.heappush(self._heap, (deliver, job_id))

    def pop_ready(self) -> List[str]:
        now = time.time()
        ready: List[str] = []
        with self._lock:
            while self._heap and self._heap[0][0] <= now:
                _, jid = heapq.heappop(self._heap)
                ready.append(jid)
        return ready
