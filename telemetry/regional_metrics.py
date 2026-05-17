from __future__ import annotations

import threading
from typing import Any, Dict, List

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.regional_metrics")


class RegionalMetrics:
    """Per-region metrics collection and aggregation."""

    def __init__(self, region_id: str) -> None:
        self.region_id = region_id
        self._lock = threading.Lock()
        self._requests_total = 0
        self._requests_success = 0
        self._requests_failed = 0
        self._inference_count = 0
        self._avg_latency_ms = 0.0
        self._gpu_utilization = 0.0
        self._cpu_utilization = 0.0
        self._memory_utilization = 0.0
        self._active_models: Dict[str, int] = {}
        self._edge_nodes_online = 0
        self._edge_nodes_total = 0
        self._snapshots: List[Dict[str, Any]] = []

    def record_request(self, success: bool, latency_ms: float, model_id: str = "") -> None:
        with self._lock:
            self._requests_total += 1
            if success:
                self._requests_success += 1
            else:
                self._requests_failed += 1
            alpha = 0.1
            self._avg_latency_ms = self._avg_latency_ms * (1 - alpha) + latency_ms * alpha
            if model_id:
                self._active_models[model_id] = self._active_models.get(model_id, 0) + 1
                self._inference_count += 1

    def update_utilization(
        self,
        gpu: float | None = None,
        cpu: float | None = None,
        memory: float | None = None,
    ) -> None:
        with self._lock:
            if gpu is not None:
                self._gpu_utilization = gpu
            if cpu is not None:
                self._cpu_utilization = cpu
            if memory is not None:
                self._memory_utilization = memory

    def update_edge_status(self, online: int, total: int) -> None:
        with self._lock:
            self._edge_nodes_online = online
            self._edge_nodes_total = total

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            snap = {
                "ts": now_ts(),
                "region_id": self.region_id,
                "requests_total": self._requests_total,
                "requests_success": self._requests_success,
                "requests_failed": self._requests_failed,
                "inference_count": self._inference_count,
                "avg_latency_ms": round(self._avg_latency_ms, 2),
                "gpu_utilization": round(self._gpu_utilization, 4),
                "cpu_utilization": round(self._cpu_utilization, 4),
                "memory_utilization": round(self._memory_utilization, 4),
                "active_models": dict(self._active_models),
                "edge_nodes_online": self._edge_nodes_online,
                "edge_nodes_total": self._edge_nodes_total,
                "error_rate": round(
                    self._requests_failed / self._requests_total, 4
                ) if self._requests_total else 0.0,
            }
            self._snapshots.append(snap)
            if len(self._snapshots) > 200:
                self._snapshots = self._snapshots[-200:]
            return snap

    def history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._snapshots))[:limit]
