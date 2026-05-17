from __future__ import annotations

import threading
from typing import Any, Dict, List

from shared.utils import now_ts, setup_logging

logger = setup_logging("telemetry.routing_metrics")


class RoutingMetrics:
    """Metrics for global routing decisions, failovers, and reroutes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total_routes = 0
        self._total_reroutes = 0
        self._total_failovers = 0
        self._by_region: Dict[str, int] = {}
        self._by_strategy: Dict[str, int] = {}
        self._latency_samples: List[float] = []
        self._failover_events: List[Dict[str, Any]] = []
        self._reroute_events: List[Dict[str, Any]] = []

    def record_route(self, target_region: str, strategy: str, decision_time_ms: float) -> None:
        with self._lock:
            self._total_routes += 1
            self._by_region[target_region] = self._by_region.get(target_region, 0) + 1
            self._by_strategy[strategy] = self._by_strategy.get(strategy, 0) + 1
            self._latency_samples.append(decision_time_ms)
            if len(self._latency_samples) > 1000:
                self._latency_samples = self._latency_samples[-1000:]

    def record_reroute(self, from_region: str, to_region: str, reason: str) -> None:
        with self._lock:
            self._total_reroutes += 1
            self._reroute_events.append({
                "ts": now_ts(),
                "from": from_region,
                "to": to_region,
                "reason": reason,
            })
            if len(self._reroute_events) > 200:
                self._reroute_events = self._reroute_events[-200:]

    def record_failover(self, source: str, target: str, trigger: str) -> None:
        with self._lock:
            self._total_failovers += 1
            self._failover_events.append({
                "ts": now_ts(),
                "source": source,
                "target": target,
                "trigger": trigger,
            })
            if len(self._failover_events) > 200:
                self._failover_events = self._failover_events[-200:]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            samples = self._latency_samples
            avg_latency = round(sum(samples) / len(samples), 2) if samples else 0.0
            sorted_samples = sorted(samples) if samples else []
            n = len(sorted_samples)
            return {
                "ts": now_ts(),
                "total_routes": self._total_routes,
                "total_reroutes": self._total_reroutes,
                "total_failovers": self._total_failovers,
                "by_region": dict(self._by_region),
                "by_strategy": dict(self._by_strategy),
                "decision_latency": {
                    "avg_ms": avg_latency,
                    "p50_ms": round(sorted_samples[int(n * 0.5)], 2) if n else 0.0,
                    "p95_ms": round(sorted_samples[min(int(n * 0.95), n - 1)], 2) if n else 0.0,
                    "p99_ms": round(sorted_samples[min(int(n * 0.99), n - 1)], 2) if n else 0.0,
                },
                "recent_failovers": list(reversed(self._failover_events))[:10],
                "recent_reroutes": list(reversed(self._reroute_events))[:10],
            }
