from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional, Tuple

from shared.utils import now_ts, setup_logging

logger = setup_logging("networking.bandwidth")


class BandwidthMeasurement:
    def __init__(self, source: str, target: str, bandwidth_mbps: float) -> None:
        self.source = source
        self.target = target
        self.bandwidth_mbps = bandwidth_mbps
        self.ts = now_ts()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "bandwidth_mbps": self.bandwidth_mbps,
            "ts": self.ts,
        }


class BandwidthOptimizer:
    """Bandwidth-aware routing and transfer optimization for inter-region data flows."""

    def __init__(self, ema_alpha: float = 0.3) -> None:
        self._lock = threading.Lock()
        self._alpha = ema_alpha
        self._measurements: Dict[Tuple[str, str], float] = {}
        self._history: List[BandwidthMeasurement] = []
        self._transfer_queue: List[Dict[str, Any]] = []
        self._total_transfers = 0
        self._total_bytes_transferred = 0

    def record_bandwidth(self, source: str, target: str, bandwidth_mbps: float) -> None:
        key = (source, target)
        with self._lock:
            prev = self._measurements.get(key)
            if prev is not None:
                self._measurements[key] = prev * (1 - self._alpha) + bandwidth_mbps * self._alpha
            else:
                self._measurements[key] = bandwidth_mbps
            measurement = BandwidthMeasurement(source, target, bandwidth_mbps)
            self._history.append(measurement)
            if len(self._history) > 1000:
                self._history = self._history[-1000:]

    def get_bandwidth(self, source: str, target: str) -> Optional[float]:
        with self._lock:
            return self._measurements.get((source, target))

    def best_path(self, source: str, target: str, candidates: List[str]) -> Optional[str]:
        best: Optional[str] = None
        best_bw = 0.0
        for mid in candidates:
            bw1 = self.get_bandwidth(source, mid)
            bw2 = self.get_bandwidth(mid, target)
            if bw1 is not None and bw2 is not None:
                effective = min(bw1, bw2)
                if effective > best_bw:
                    best_bw = effective
                    best = mid
        direct = self.get_bandwidth(source, target)
        if direct is not None and direct >= best_bw:
            return None
        return best

    def schedule_transfer(
        self,
        source: str,
        target: str,
        size_bytes: int,
        priority: int = 0,
    ) -> Dict[str, Any]:
        transfer = {
            "source": source,
            "target": target,
            "size_bytes": size_bytes,
            "priority": priority,
            "scheduled_at": now_ts(),
            "status": "queued",
        }
        with self._lock:
            self._transfer_queue.append(transfer)
            self._transfer_queue.sort(key=lambda t: t["priority"], reverse=True)
            self._total_transfers += 1
            self._total_bytes_transferred += size_bytes
        return transfer

    def bandwidth_matrix(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            matrix: Dict[str, Dict[str, float]] = {}
            for (src, dst), bw in self._measurements.items():
                if src not in matrix:
                    matrix[src] = {}
                matrix[src][dst] = round(bw, 2)
            return matrix

    def recent_measurements(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return [m.to_dict() for m in reversed(self._history)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            bws = list(self._measurements.values())
            return {
                "ts": now_ts(),
                "measured_links": len(self._measurements),
                "avg_bandwidth_mbps": round(sum(bws) / len(bws), 2) if bws else 0.0,
                "min_bandwidth_mbps": round(min(bws), 2) if bws else 0.0,
                "max_bandwidth_mbps": round(max(bws), 2) if bws else 0.0,
                "pending_transfers": len(self._transfer_queue),
                "total_transfers": self._total_transfers,
                "total_bytes_transferred": self._total_bytes_transferred,
            }
