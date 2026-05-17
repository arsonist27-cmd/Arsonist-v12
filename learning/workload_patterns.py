from __future__ import annotations

import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("learning.workload_patterns")


class WorkloadPattern(BaseModel):
    pattern_id: str
    workload_type: str = ""
    region_id: str = ""
    avg_duration_ms: float = 0.0
    avg_gpu_usage: float = 0.0
    avg_memory_mb: float = 0.0
    peak_hour: int = -1
    request_frequency: float = 0.0
    success_rate: float = 1.0
    preferred_gpu_type: str = ""
    typical_batch_size: int = 1
    sample_count: int = 0
    last_updated: float = 0.0


class WorkloadPatternAnalyzer:
    """Learns workload execution patterns to predict resource requirements,
    optimal scheduling windows, and preferred GPU configurations."""

    def __init__(self, min_samples: int = 5, max_patterns: int = 500) -> None:
        self._lock = threading.RLock()
        self._min_samples = min_samples
        self._max_patterns = max_patterns
        self._raw: Dict[str, List[Dict[str, Any]]] = {}
        self._patterns: Dict[str, WorkloadPattern] = {}
        self._total_analyzed = 0

    def record(self, workload_type: str, observation: Dict[str, Any]) -> None:
        key = f"{workload_type}:{observation.get('region_id', 'global')}"
        with self._lock:
            if key not in self._raw:
                self._raw[key] = []
            self._raw[key].append(observation)
            if len(self._raw[key]) > 200:
                self._raw[key] = self._raw[key][-200:]

    def analyze(self, workloads: List[Dict[str, Any]]) -> List[WorkloadPattern]:
        ts = now_ts()
        for w in workloads:
            wtype = w.get("workload_type", "inference")
            self.record(wtype, w)

        results: List[WorkloadPattern] = []
        with self._lock:
            for key, observations in self._raw.items():
                if len(observations) < self._min_samples:
                    continue
                parts = key.split(":", 1)
                wtype = parts[0]
                region = parts[1] if len(parts) > 1 else "global"

                durations = [o.get("duration_ms", 0) for o in observations if o.get("duration_ms")]
                gpu_usages = [o.get("gpu_usage", 0) for o in observations if "gpu_usage" in o]
                memory = [o.get("memory_mb", 0) for o in observations if o.get("memory_mb")]
                successes = [o.get("success", True) for o in observations]

                avg_dur = sum(durations) / len(durations) if durations else 0.0
                avg_gpu = sum(gpu_usages) / len(gpu_usages) if gpu_usages else 0.0
                avg_mem = sum(memory) / len(memory) if memory else 0.0
                success_rate = sum(1 for s in successes if s) / len(successes) if successes else 1.0

                gpu_types = [o.get("gpu_type", "") for o in observations if o.get("gpu_type")]
                preferred_gpu = max(set(gpu_types), key=gpu_types.count) if gpu_types else ""

                pattern = WorkloadPattern(
                    pattern_id=f"wp-{key}",
                    workload_type=wtype,
                    region_id=region,
                    avg_duration_ms=round(avg_dur, 1),
                    avg_gpu_usage=round(avg_gpu, 3),
                    avg_memory_mb=round(avg_mem, 1),
                    request_frequency=round(len(observations) / max(1, (ts - observations[0].get("ts", ts)) / 3600), 2),
                    success_rate=round(success_rate, 3),
                    preferred_gpu_type=preferred_gpu,
                    sample_count=len(observations),
                    last_updated=ts,
                )
                self._patterns[key] = pattern
                results.append(pattern)
                self._total_analyzed += 1

            if len(self._patterns) > self._max_patterns:
                sorted_keys = sorted(self._patterns, key=lambda k: self._patterns[k].last_updated)
                for k in sorted_keys[:len(self._patterns) - self._max_patterns]:
                    del self._patterns[k]

        return results

    def get_pattern(self, workload_type: str, region_id: str = "global") -> Dict[str, Any]:
        key = f"{workload_type}:{region_id}"
        with self._lock:
            p = self._patterns.get(key)
            return p.model_dump(mode="json") if p else {}

    def all_patterns(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._patterns.values()]

    def low_success_patterns(self, threshold: float = 0.9) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._patterns.values()
                    if p.success_rate < threshold]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_analyzed": self._total_analyzed,
                "active_patterns": len(self._patterns),
                "tracked_types": len(self._raw),
            }
