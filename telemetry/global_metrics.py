from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.global_metrics")


class GlobalMetricsCollector:
    """Collects and exposes global fabric metrics across all regions.

    Tracks: global request flow, cross-region latency, replication lag,
    edge node health, model replication status, bandwidth usage,
    failover events, regional saturation.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
        self._events: List[Dict[str, Any]] = []
        self._regional_data: Dict[str, Dict[str, Any]] = {}

    def inc_counter(self, name: str, delta: int = 1) -> int:
        with self._lock:
            val = self._counters.get(name, 0) + delta
            self._counters[name] = val
            return val

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def record_histogram(self, name: str, value: float) -> None:
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = []
            hist = self._histograms[name]
            hist.append(value)
            if len(hist) > 1000:
                self._histograms[name] = hist[-1000:]

    def record_event(self, event_type: str, details: Dict[str, Any]) -> None:
        entry = {"ts": now_ts(), "event": event_type, "details": details}
        with self._lock:
            self._events.append(entry)
            if len(self._events) > 500:
                self._events = self._events[-500:]

    def update_regional_data(self, region_id: str, data: Dict[str, Any]) -> None:
        with self._lock:
            self._regional_data[region_id] = {**data, "updated_at": now_ts()}

    def record_request_flow(
        self,
        request_id: str,
        source_region: str,
        target_region: str,
        latency_ms: float,
        success: bool = True,
    ) -> None:
        self.inc_counter("global_requests_total")
        if success:
            self.inc_counter("global_requests_success")
        else:
            self.inc_counter("global_requests_failed")
        self.record_histogram("request_latency_ms", latency_ms)
        self.record_histogram(f"request_latency_ms:{target_region}", latency_ms)
        self.record_event("request_flow", {
            "request_id": request_id,
            "source": source_region,
            "target": target_region,
            "latency_ms": latency_ms,
            "success": success,
        })

    def record_cross_region_latency(self, from_region: str, to_region: str, latency_ms: float) -> None:
        self.record_histogram("cross_region_latency_ms", latency_ms)
        self.set_gauge(f"latency:{from_region}:{to_region}", latency_ms)

    def record_replication_lag(self, region_id: str, lag_ms: float) -> None:
        self.set_gauge(f"replication_lag_ms:{region_id}", lag_ms)
        self.record_histogram("replication_lag_ms", lag_ms)

    def record_edge_health(self, node_id: str, healthy: bool, details: Dict[str, Any] | None = None) -> None:
        self.set_gauge(f"edge_health:{node_id}", 1.0 if healthy else 0.0)
        if not healthy:
            self.inc_counter("edge_unhealthy_events")
            self.record_event("edge_unhealthy", {"node_id": node_id, **(details or {})})

    def record_failover(self, source_region: str, target_region: str, trigger: str) -> None:
        self.inc_counter("failover_events_total")
        self.record_event("failover", {
            "source": source_region,
            "target": target_region,
            "trigger": trigger,
        })

    def record_bandwidth(self, source: str, target: str, mbps: float) -> None:
        self.set_gauge(f"bandwidth_mbps:{source}:{target}", mbps)
        self.record_histogram("bandwidth_mbps", mbps)

    def record_saturation(self, region_id: str, saturation: float) -> None:
        self.set_gauge(f"saturation:{region_id}", saturation)

    def _histogram_stats(self, name: str) -> Dict[str, float]:
        with self._lock:
            vals = self._histograms.get(name, [])
            if not vals:
                return {"count": 0, "avg": 0.0, "min": 0.0, "max": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
            sorted_vals = sorted(vals)
            n = len(sorted_vals)
            return {
                "count": n,
                "avg": round(sum(sorted_vals) / n, 2),
                "min": round(sorted_vals[0], 2),
                "max": round(sorted_vals[-1], 2),
                "p50": round(sorted_vals[int(n * 0.5)], 2),
                "p95": round(sorted_vals[min(int(n * 0.95), n - 1)], 2),
                "p99": round(sorted_vals[min(int(n * 0.99), n - 1)], 2),
            }

    def global_metrics(self) -> Dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
        return {
            "ts": now_ts(),
            "counters": counters,
            "gauges": gauges,
            "request_latency": self._histogram_stats("request_latency_ms"),
            "cross_region_latency": self._histogram_stats("cross_region_latency_ms"),
            "replication_lag": self._histogram_stats("replication_lag_ms"),
            "bandwidth": self._histogram_stats("bandwidth_mbps"),
        }

    def region_metrics(self, region_id: str) -> Dict[str, Any]:
        with self._lock:
            data = self._regional_data.get(region_id, {})
            gauges = {k: v for k, v in self._gauges.items() if region_id in k}
        return {
            "ts": now_ts(),
            "region_id": region_id,
            "data": data,
            "gauges": gauges,
            "request_latency": self._histogram_stats(f"request_latency_ms:{region_id}"),
        }

    def recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]
