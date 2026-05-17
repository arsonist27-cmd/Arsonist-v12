from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("intelligence.workload_analyzer")


class WorkloadProfile(BaseModel):
    workload_id: str
    region_id: str = ""
    workload_type: str = ""
    avg_latency_ms: float = 0.0
    avg_gpu_usage: float = 0.0
    avg_memory_mb: float = 0.0
    avg_cpu_usage: float = 0.0
    request_rate: float = 0.0
    error_rate: float = 0.0
    cost_per_hour: float = 0.0
    efficiency_score: float = 0.0
    placement_quality: float = 0.0
    samples: int = 0
    last_updated: float = 0.0


class WorkloadAnalyzer:
    """Analyzes workload characteristics to profile resource usage patterns,
    identify inefficient placements, and recommend migrations."""

    def __init__(self, max_profiles: int = 500) -> None:
        self._lock = threading.RLock()
        self._profiles: Dict[str, WorkloadProfile] = {}
        self._max_profiles = max_profiles
        self._total_analyzed = 0
        self._events: List[Dict[str, Any]] = []

    def analyze(self, workloads: List[Dict[str, Any]]) -> List[WorkloadProfile]:
        results: List[WorkloadProfile] = []
        ts = now_ts()

        for w in workloads:
            wid = w.get("workload_id", "")
            if not wid:
                continue

            gpu_usage = w.get("gpu_usage", 0.0)
            cpu_usage = w.get("cpu_usage", 0.0)
            latency = w.get("latency_ms", 0.0)
            error_rate = w.get("error_rate", 0.0)

            efficiency = self._compute_efficiency(gpu_usage, cpu_usage, latency, error_rate)
            placement_q = self._compute_placement_quality(w)

            profile = WorkloadProfile(
                workload_id=wid,
                region_id=w.get("region_id", ""),
                workload_type=w.get("workload_type", "inference"),
                avg_latency_ms=round(latency, 2),
                avg_gpu_usage=round(gpu_usage, 3),
                avg_memory_mb=w.get("memory_mb", 0.0),
                avg_cpu_usage=round(cpu_usage, 3),
                request_rate=w.get("request_rate", 0.0),
                error_rate=round(error_rate, 4),
                cost_per_hour=w.get("cost_per_hour", 0.0),
                efficiency_score=round(efficiency, 3),
                placement_quality=round(placement_q, 3),
                samples=w.get("samples", 1),
                last_updated=ts,
            )

            with self._lock:
                self._profiles[wid] = profile
                self._total_analyzed += 1

            results.append(profile)

        if len(self._profiles) > self._max_profiles:
            with self._lock:
                sorted_keys = sorted(self._profiles, key=lambda k: self._profiles[k].last_updated)
                for k in sorted_keys[:len(self._profiles) - self._max_profiles]:
                    del self._profiles[k]

        return results

    def _compute_efficiency(self, gpu: float, cpu: float, latency: float, error_rate: float) -> float:
        resource_score = 1.0 - abs(gpu - 0.7) - abs(cpu - 0.6) * 0.5
        latency_score = max(0.0, 1.0 - latency / 500.0)
        error_score = max(0.0, 1.0 - error_rate * 10)
        return max(0.0, min(1.0, resource_score * 0.4 + latency_score * 0.4 + error_score * 0.2))

    def _compute_placement_quality(self, w: Dict[str, Any]) -> float:
        latency_ok = 1.0 if w.get("latency_ms", 0) < 200 else 0.5
        gpu_ok = 1.0 if 0.3 < w.get("gpu_usage", 0) < 0.9 else 0.5
        error_ok = 1.0 if w.get("error_rate", 0) < 0.01 else 0.3
        return (latency_ok + gpu_ok + error_ok) / 3.0

    def inefficient_workloads(self, threshold: float = 0.5) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._profiles.values()
                    if p.efficiency_score < threshold]

    def migration_candidates(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._profiles.values()
                    if p.placement_quality < 0.6 or p.efficiency_score < 0.4]

    def get_profile(self, workload_id: str) -> Dict[str, Any]:
        with self._lock:
            p = self._profiles.get(workload_id)
            return p.model_dump(mode="json") if p else {}

    def all_profiles(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._profiles.values()]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            profiles = list(self._profiles.values())
            avg_eff = sum(p.efficiency_score for p in profiles) / len(profiles) if profiles else 0.0
            return {
                "ts": now_ts(),
                "total_analyzed": self._total_analyzed,
                "active_profiles": len(self._profiles),
                "avg_efficiency": round(avg_eff, 3),
                "migration_candidates": sum(1 for p in profiles if p.placement_quality < 0.6),
            }
