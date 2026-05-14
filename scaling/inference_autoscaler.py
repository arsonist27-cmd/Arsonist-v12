from __future__ import annotations

import os
import threading
import time
from typing import Any, Callable, Optional

from telemetry.inference_metrics import InferenceMetrics


class InferenceAutoscaler:
    """Heuristic autoscaler hooks driven by latency, queue depth, and GPU saturation."""

    def __init__(self, metrics: InferenceMetrics, scale_fn: Optional[Callable[[str, int], None]] = None) -> None:
        self.metrics = metrics
        self.scale_fn = scale_fn
        self._stop = threading.Event()
        self.interval = float(os.getenv("ARSONIST_INFERENCE_AUTOSCALE_INTERVAL_SEC", "8"))

    def _tick(self) -> None:
        snap = self.metrics.snapshot()
        p95 = snap.get("latency_p95_ms") or 0.0
        if p95 > float(os.getenv("ARSONIST_AUTOSCALE_LATENCY_P95_MS", "800")) and self.scale_fn:
            self.scale_fn("latency", 1)

    def run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:
                pass
            time.sleep(self.interval)

    def start_background(self) -> threading.Thread:
        t = threading.Thread(target=self.run_loop, daemon=True, name="inference-autoscaler")
        t.start()
        return t

    def stop(self) -> None:
        self._stop.set()
