from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional, Tuple

from shared.utils import now_ts, setup_logging

logger = setup_logging("regions.latency_map")


class LatencyMap:
    """Tracks inter-region and client-to-region latency measurements."""

    def __init__(self, ema_alpha: float = 0.3) -> None:
        self._lock = threading.Lock()
        self._inter_region: Dict[Tuple[str, str], float] = {}
        self._client_region: Dict[Tuple[str, str], float] = {}
        self._history: List[Dict[str, Any]] = []
        self._alpha = ema_alpha

    def record_inter_region(self, from_region: str, to_region: str, latency_ms: float) -> None:
        key = (from_region, to_region)
        with self._lock:
            prev = self._inter_region.get(key)
            if prev is not None:
                self._inter_region[key] = prev * (1 - self._alpha) + latency_ms * self._alpha
            else:
                self._inter_region[key] = latency_ms
            self._history.append({
                "ts": now_ts(),
                "type": "inter_region",
                "from": from_region,
                "to": to_region,
                "latency_ms": latency_ms,
            })
            if len(self._history) > 1000:
                self._history = self._history[-1000:]

    def record_client_region(self, client_id: str, region_id: str, latency_ms: float) -> None:
        key = (client_id, region_id)
        with self._lock:
            prev = self._client_region.get(key)
            if prev is not None:
                self._client_region[key] = prev * (1 - self._alpha) + latency_ms * self._alpha
            else:
                self._client_region[key] = latency_ms

    def get_inter_region(self, from_region: str, to_region: str) -> Optional[float]:
        with self._lock:
            return self._inter_region.get((from_region, to_region))

    def get_client_region(self, client_id: str, region_id: str) -> Optional[float]:
        with self._lock:
            return self._client_region.get((client_id, region_id))

    def best_region_for_client(self, client_id: str, candidate_regions: List[str]) -> Optional[str]:
        with self._lock:
            scored = []
            for rid in candidate_regions:
                lat = self._client_region.get((client_id, rid))
                if lat is not None:
                    scored.append((lat, rid))
        if not scored:
            return None
        scored.sort(key=lambda x: x[0])
        return scored[0][1]

    def inter_region_matrix(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            matrix: Dict[str, Dict[str, float]] = {}
            for (src, dst), lat in self._inter_region.items():
                if src not in matrix:
                    matrix[src] = {}
                matrix[src][dst] = round(lat, 2)
            return matrix

    def recent_measurements(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._history))[:limit]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            vals = list(self._inter_region.values())
            return {
                "ts": now_ts(),
                "inter_region_pairs": len(self._inter_region),
                "client_region_pairs": len(self._client_region),
                "avg_inter_region_ms": round(sum(vals) / len(vals), 2) if vals else 0.0,
                "min_inter_region_ms": round(min(vals), 2) if vals else 0.0,
                "max_inter_region_ms": round(max(vals), 2) if vals else 0.0,
            }
