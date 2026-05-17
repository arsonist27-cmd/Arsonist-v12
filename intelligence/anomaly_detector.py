from __future__ import annotations

import math
import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("intelligence.anomaly")


class AnomalyType(str, Enum):
    latency_spike = "latency_spike"
    node_instability = "node_instability"
    suspicious_traffic = "suspicious_traffic"
    memory_leak = "memory_leak"
    runaway_workload = "runaway_workload"
    gpu_degradation = "gpu_degradation"
    replication_anomaly = "replication_anomaly"
    throughput_drop = "throughput_drop"


class AnomalySeverity(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class Anomaly(BaseModel):
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity = AnomalySeverity.medium
    score: float = 0.0
    region_id: str = ""
    node_id: str = ""
    description: str = ""
    metric_name: str = ""
    metric_value: float = 0.0
    threshold: float = 0.0
    remediation: str = ""
    detected_at: float = 0.0
    resolved: bool = False
    resolved_at: float = 0.0


class AnomalyDetector:
    """Near real-time anomaly detection for infrastructure metrics.

    Detects abnormal inference latency, node instability, suspicious traffic,
    memory leaks, runaway workloads, GPU degradation, and replication anomalies.
    Uses statistical deviation and threshold-based detection.
    """

    def __init__(
        self,
        latency_threshold_ms: float = 500.0,
        gpu_temp_threshold_c: float = 85.0,
        memory_growth_threshold_pct: float = 0.15,
        replication_lag_threshold_s: float = 30.0,
        traffic_spike_factor: float = 3.0,
        max_history: int = 1000,
    ) -> None:
        self._lock = threading.RLock()
        self._latency_threshold = latency_threshold_ms
        self._gpu_temp_threshold = gpu_temp_threshold_c
        self._memory_growth_threshold = memory_growth_threshold_pct
        self._replication_lag_threshold = replication_lag_threshold_s
        self._traffic_spike_factor = traffic_spike_factor
        self._max_history = max_history
        self._anomalies: List[Anomaly] = []
        self._metric_history: Dict[str, List[float]] = {}
        self._total_detected = 0
        self._total_resolved = 0
        self._events: List[Dict[str, Any]] = []

    def _record_metric(self, key: str, value: float) -> None:
        if key not in self._metric_history:
            self._metric_history[key] = []
        self._metric_history[key].append(value)
        if len(self._metric_history[key]) > 200:
            self._metric_history[key] = self._metric_history[key][-200:]

    def _stddev(self, key: str) -> tuple[float, float]:
        values = self._metric_history.get(key, [])
        if len(values) < 5:
            return 0.0, 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return mean, math.sqrt(variance)

    def _is_anomalous(self, key: str, value: float, factor: float = 2.5) -> bool:
        mean, sd = self._stddev(key)
        if sd == 0:
            return False
        return abs(value - mean) > factor * sd

    def detect(self, telemetry: Dict[str, Any]) -> List[Anomaly]:
        found: List[Anomaly] = []
        ts = now_ts()

        regions = telemetry.get("regions", [])
        for r in regions:
            region_id = r.get("region_id", "unknown")

            latency = r.get("avg_latency_ms", 0.0)
            metric_key = f"latency:{region_id}"
            self._record_metric(metric_key, latency)
            if latency > self._latency_threshold or self._is_anomalous(metric_key, latency):
                severity = AnomalySeverity.critical if latency > self._latency_threshold * 2 else AnomalySeverity.high
                found.append(Anomaly(
                    anomaly_id=f"latency-{region_id}-{int(ts)}",
                    anomaly_type=AnomalyType.latency_spike,
                    severity=severity,
                    score=min(latency / self._latency_threshold, 2.0),
                    region_id=region_id,
                    description=f"Latency spike in {region_id}: {latency:.0f}ms",
                    metric_name="avg_latency_ms",
                    metric_value=latency,
                    threshold=self._latency_threshold,
                    remediation=f"Reroute traffic from {region_id} or scale capacity",
                    detected_at=ts,
                ))

            gpu_temp = r.get("gpu_temp_c", 0.0)
            if gpu_temp > self._gpu_temp_threshold:
                severity = AnomalySeverity.critical if gpu_temp > 95 else AnomalySeverity.high
                found.append(Anomaly(
                    anomaly_id=f"gpu-temp-{region_id}-{int(ts)}",
                    anomaly_type=AnomalyType.gpu_degradation,
                    severity=severity,
                    score=min(gpu_temp / 100.0, 1.5),
                    region_id=region_id,
                    description=f"GPU overheating in {region_id}: {gpu_temp:.0f}C",
                    metric_name="gpu_temp_c",
                    metric_value=gpu_temp,
                    threshold=self._gpu_temp_threshold,
                    remediation=f"Throttle workloads in {region_id} and redistribute",
                    detected_at=ts,
                ))

            mem_growth = r.get("memory_growth_rate", 0.0)
            if mem_growth > self._memory_growth_threshold:
                found.append(Anomaly(
                    anomaly_id=f"memleak-{region_id}-{int(ts)}",
                    anomaly_type=AnomalyType.memory_leak,
                    severity=AnomalySeverity.high,
                    score=min(mem_growth / self._memory_growth_threshold, 2.0),
                    region_id=region_id,
                    description=f"Possible memory leak in {region_id}: {mem_growth:.0%} growth",
                    metric_name="memory_growth_rate",
                    metric_value=mem_growth,
                    threshold=self._memory_growth_threshold,
                    remediation=f"Investigate and restart leaking services in {region_id}",
                    detected_at=ts,
                ))

            rps = r.get("requests_per_second", 0.0)
            rps_key = f"rps:{region_id}"
            self._record_metric(rps_key, rps)
            if self._is_anomalous(rps_key, rps, self._traffic_spike_factor):
                found.append(Anomaly(
                    anomaly_id=f"traffic-{region_id}-{int(ts)}",
                    anomaly_type=AnomalyType.suspicious_traffic,
                    severity=AnomalySeverity.medium,
                    score=min(rps / 1000.0, 1.5),
                    region_id=region_id,
                    description=f"Traffic anomaly in {region_id}: {rps:.0f} rps",
                    metric_name="requests_per_second",
                    metric_value=rps,
                    remediation=f"Rate limit or investigate traffic to {region_id}",
                    detected_at=ts,
                ))

            repl_lag = r.get("replication_lag_s", 0.0)
            if repl_lag > self._replication_lag_threshold:
                found.append(Anomaly(
                    anomaly_id=f"repl-lag-{region_id}-{int(ts)}",
                    anomaly_type=AnomalyType.replication_anomaly,
                    severity=AnomalySeverity.high,
                    score=min(repl_lag / self._replication_lag_threshold, 2.0),
                    region_id=region_id,
                    description=f"Replication lag in {region_id}: {repl_lag:.1f}s",
                    metric_name="replication_lag_s",
                    metric_value=repl_lag,
                    threshold=self._replication_lag_threshold,
                    remediation=f"Check replication health for {region_id}",
                    detected_at=ts,
                ))

        nodes = telemetry.get("nodes", [])
        for n in nodes:
            node_id = n.get("node_id", "unknown")
            restart_count = n.get("restart_count_1h", 0)
            if restart_count > 3:
                found.append(Anomaly(
                    anomaly_id=f"unstable-{node_id}-{int(ts)}",
                    anomaly_type=AnomalyType.node_instability,
                    severity=AnomalySeverity.high,
                    score=min(restart_count / 5.0, 2.0),
                    node_id=node_id,
                    description=f"Node {node_id} restarted {restart_count} times in 1h",
                    metric_name="restart_count_1h",
                    metric_value=float(restart_count),
                    threshold=3.0,
                    remediation=f"Investigate and potentially replace node {node_id}",
                    detected_at=ts,
                ))

            cpu_usage = n.get("cpu_usage", 0.0)
            if cpu_usage > 0.95:
                found.append(Anomaly(
                    anomaly_id=f"runaway-{node_id}-{int(ts)}",
                    anomaly_type=AnomalyType.runaway_workload,
                    severity=AnomalySeverity.high,
                    score=cpu_usage,
                    node_id=node_id,
                    description=f"Runaway workload on {node_id}: CPU at {cpu_usage:.0%}",
                    metric_name="cpu_usage",
                    metric_value=cpu_usage,
                    threshold=0.95,
                    remediation=f"Kill or throttle runaway process on {node_id}",
                    detected_at=ts,
                ))

        with self._lock:
            self._anomalies.extend(found)
            if len(self._anomalies) > self._max_history:
                self._anomalies = self._anomalies[-self._max_history:]
            self._total_detected += len(found)
            for a in found:
                self._events.append({
                    "type": "anomaly_detected",
                    "anomaly_id": a.anomaly_id,
                    "anomaly_type": a.anomaly_type.value,
                    "severity": a.severity.value,
                    "region_id": a.region_id,
                    "ts": a.detected_at,
                })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

        return found

    def resolve(self, anomaly_id: str) -> bool:
        with self._lock:
            for a in self._anomalies:
                if a.anomaly_id == anomaly_id and not a.resolved:
                    a.resolved = True
                    a.resolved_at = now_ts()
                    self._total_resolved += 1
                    self._events.append({
                        "type": "anomaly_resolved",
                        "anomaly_id": anomaly_id,
                        "ts": a.resolved_at,
                    })
                    return True
        return False

    def active_anomalies(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in self._anomalies if not a.resolved]

    def recent_anomalies(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in reversed(self._anomalies)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            active = sum(1 for a in self._anomalies if not a.resolved)
            by_type: Dict[str, int] = {}
            for a in self._anomalies:
                if not a.resolved:
                    by_type[a.anomaly_type.value] = by_type.get(a.anomaly_type.value, 0) + 1
            return {
                "ts": now_ts(),
                "total_detected": self._total_detected,
                "total_resolved": self._total_resolved,
                "active_anomalies": active,
                "by_type": by_type,
            }
