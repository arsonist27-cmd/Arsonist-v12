from __future__ import annotations

import os
import threading
import time
from typing import Callable, Optional

from gpu.gpu_metrics import GpuMetrics


class GpuScaler:
    def __init__(self, metrics: GpuMetrics, fn: Optional[Callable[[], None]] = None) -> None:
        self.metrics = metrics
        self.fn = fn
        self._stop = threading.Event()

    def run_loop(self) -> None:
        while not self._stop.is_set():
            snap = self.metrics.snapshot()
            util = max((g.get("utilization_pct", 0) for g in snap.get("gpus", [])), default=0.0)
            if util > float(os.getenv("ARSONIST_GPU_SCALE_UTIL_THRESHOLD", "92")) and self.fn:
                self.fn()
            time.sleep(float(os.getenv("ARSONIST_GPU_SCALE_INTERVAL_SEC", "10")))

    def start_background(self) -> threading.Thread:
        t = threading.Thread(target=self.run_loop, daemon=True, name="gpu-scaler")
        t.start()
        return t

    def stop(self) -> None:
        self._stop.set()
