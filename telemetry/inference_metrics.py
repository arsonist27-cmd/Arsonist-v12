from __future__ import annotations

import threading
from collections import deque
from typing import Any, Deque, Dict, List

from shared.utils import now_ts


class InferenceMetrics:
    """In-memory rolling metrics for inference + GPU pressure hooks."""

    def __init__(self, window: int = 500) -> None:
        self._lock = threading.Lock()
        self._latencies_ms: Deque[float] = deque(maxlen=window)
        self._tokens_out: Deque[int] = deque(maxlen=window)
        self.requests_total = 0
        self.failures = 0
        self._model_load_ms: Dict[str, float] = {}

    def record_chat(self, model: str, latency_ms: float, tokens_out: int) -> None:
        with self._lock:
            self.requests_total += 1
            self._latencies_ms.append(latency_ms)
            self._tokens_out.append(tokens_out)

    def record_embedding(self, model: str, latency_ms: float, dim: int) -> None:
        with self._lock:
            self.requests_total += 1
            self._latencies_ms.append(latency_ms)

    def record_generate(self, model: str, latency_ms: float, tokens_out: int) -> None:
        with self._lock:
            self.requests_total += 1
            self._latencies_ms.append(latency_ms)
            self._tokens_out.append(tokens_out)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latencies_ms)
            toks = list(self._tokens_out)
        p50 = _pctl(lats, 50)
        p95 = _pctl(lats, 95)
        tps = (sum(toks) / max(1e-6, (p95 or 1) / 1000.0)) if toks else 0.0
        return {
            "ts": now_ts(),
            "requests_total": self.requests_total,
            "failures": self.failures,
            "latency_p50_ms": p50,
            "latency_p95_ms": p95,
            "throughput_tokens_per_s_est": round(tps, 3),
            "model_load_times_ms": dict(self._model_load_ms),
        }


def _pctl(values: List[float], p: int) -> float | None:
    if not values:
        return None
    s = sorted(values)
    idx = int(round((p / 100.0) * (len(s) - 1)))
    return float(s[idx])
