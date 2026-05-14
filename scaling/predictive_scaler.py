from __future__ import annotations

import statistics
import time
from collections import deque
from typing import Deque


class PredictiveScaler:
    """Very small moving-average predictor for burst smoothing (hook only)."""

    def __init__(self, window: int = 20) -> None:
        self._hist: Deque[float] = deque(maxlen=window)

    def observe_rps(self, rps: float) -> None:
        self._hist.append(rps)

    def predict_next(self) -> float:
        if len(self._hist) < 3:
            return self._hist[-1] if self._hist else 0.0
        return float(statistics.mean(list(self._hist)[-5:]))
