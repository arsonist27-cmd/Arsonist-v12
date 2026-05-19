"""v16 Delay Test.

Tests delay-tolerant infrastructure behavior under various latency
conditions including high-latency links, burst synchronization,
store-and-forward operations, and disconnected queue handling.
"""
from __future__ import annotations

import random
import threading
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("simulation.delay_test")


class DelayTestResult(BaseModel):
    scenario: str = ""
    messages_sent: int = 0
    messages_delivered: int = 0
    messages_lost: int = 0
    messages_expired: int = 0
    avg_delivery_delay_ms: float = 0.0
    max_delivery_delay_ms: float = 0.0
    burst_syncs_performed: int = 0
    store_forward_operations: int = 0
    queue_overflow_events: int = 0
    throughput_msg_per_s: float = 0.0
    delivery_rate_pct: float = 100.0
    resilience_score: float = 0.0
    ts: float = 0.0
    details: Dict[str, Any] = Field(default_factory=dict)


class DelayTester:
    """Tests delay-tolerant infrastructure under various latency and
    disconnection scenarios."""

    def __init__(self, seed: int = 42) -> None:
        self._lock = threading.RLock()
        self._rng = random.Random(seed)
        self._results: List[DelayTestResult] = []

    def test_high_latency(self, num_messages: int = 1000,
                          latency_range_ms: tuple = (500, 10000)) -> DelayTestResult:
        delivered = 0
        lost = 0
        expired = 0
        delays = []

        for _ in range(num_messages):
            delay = self._rng.uniform(latency_range_ms[0], latency_range_ms[1])
            roll = self._rng.random()
            if roll > 0.02:
                delivered += 1
                delays.append(delay)
            elif roll > 0.01:
                expired += 1
            else:
                lost += 1

        avg_delay = sum(delays) / len(delays) if delays else 0.0
        max_delay = max(delays) if delays else 0.0
        delivery_rate = delivered / max(num_messages, 1) * 100

        result = DelayTestResult(
            scenario="high_latency",
            messages_sent=num_messages,
            messages_delivered=delivered,
            messages_lost=lost,
            messages_expired=expired,
            avg_delivery_delay_ms=round(avg_delay, 1),
            max_delivery_delay_ms=round(max_delay, 1),
            delivery_rate_pct=round(delivery_rate, 2),
            resilience_score=round(delivery_rate / 100, 3),
            ts=now_ts(),
            details={"latency_range": latency_range_ms},
        )
        with self._lock:
            self._results.append(result)
        return result

    def test_burst_sync(self, num_bursts: int = 10,
                        messages_per_burst: int = 500,
                        inter_burst_delay_ms: float = 30000) -> DelayTestResult:
        total_sent = num_bursts * messages_per_burst
        total_delivered = 0
        total_lost = 0
        all_delays = []

        for burst in range(num_bursts):
            burst_success_rate = self._rng.uniform(0.90, 0.99)
            burst_delivered = int(messages_per_burst * burst_success_rate)
            total_delivered += burst_delivered
            total_lost += messages_per_burst - burst_delivered
            for _ in range(burst_delivered):
                all_delays.append(inter_burst_delay_ms + self._rng.uniform(10, 500))

        avg_delay = sum(all_delays) / len(all_delays) if all_delays else 0.0
        max_delay = max(all_delays) if all_delays else 0.0
        delivery_rate = total_delivered / max(total_sent, 1) * 100
        throughput = total_delivered / (num_bursts * inter_burst_delay_ms / 1000) if inter_burst_delay_ms > 0 else 0

        result = DelayTestResult(
            scenario="burst_sync",
            messages_sent=total_sent,
            messages_delivered=total_delivered,
            messages_lost=total_lost,
            avg_delivery_delay_ms=round(avg_delay, 1),
            max_delivery_delay_ms=round(max_delay, 1),
            burst_syncs_performed=num_bursts,
            throughput_msg_per_s=round(throughput, 1),
            delivery_rate_pct=round(delivery_rate, 2),
            resilience_score=round(delivery_rate / 100, 3),
            ts=now_ts(),
            details={"num_bursts": num_bursts, "per_burst": messages_per_burst},
        )
        with self._lock:
            self._results.append(result)
        return result

    def test_store_forward(self, num_messages: int = 2000,
                           disconnect_periods: int = 5) -> DelayTestResult:
        delivered = 0
        lost = 0
        expired = 0
        store_ops = 0
        overflow_events = 0
        delays = []

        messages_per_period = num_messages // disconnect_periods
        for period in range(disconnect_periods):
            stored = self._rng.randint(messages_per_period // 2, messages_per_period)
            store_ops += stored
            reconnect_delay = self._rng.uniform(5000, 60000)

            if stored > 10000:
                overflow_events += 1
                overflow_lost = stored - 10000
                stored = 10000
                lost += overflow_lost

            forward_rate = self._rng.uniform(0.92, 0.99)
            forwarded = int(stored * forward_rate)
            delivered += forwarded
            expired += stored - forwarded
            for _ in range(forwarded):
                delays.append(reconnect_delay + self._rng.uniform(100, 2000))

        avg_delay = sum(delays) / len(delays) if delays else 0.0
        max_delay = max(delays) if delays else 0.0
        delivery_rate = delivered / max(num_messages, 1) * 100

        result = DelayTestResult(
            scenario="store_forward",
            messages_sent=num_messages,
            messages_delivered=delivered,
            messages_lost=lost,
            messages_expired=expired,
            avg_delivery_delay_ms=round(avg_delay, 1),
            max_delivery_delay_ms=round(max_delay, 1),
            store_forward_operations=store_ops,
            queue_overflow_events=overflow_events,
            delivery_rate_pct=round(delivery_rate, 2),
            resilience_score=round(delivery_rate / 100, 3),
            ts=now_ts(),
            details={"disconnect_periods": disconnect_periods},
        )
        with self._lock:
            self._results.append(result)
        return result

    def run_full_suite(self) -> List[DelayTestResult]:
        return [
            self.test_high_latency(),
            self.test_burst_sync(),
            self.test_store_forward(),
        ]

    def results_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._results]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            if not self._results:
                return {"total_tests": 0}
            avg_delivery = sum(r.delivery_rate_pct for r in self._results) / len(self._results)
            avg_resilience = sum(r.resilience_score for r in self._results) / len(self._results)
            return {
                "total_tests": len(self._results),
                "avg_delivery_rate_pct": round(avg_delivery, 2),
                "avg_resilience_score": round(avg_resilience, 3),
                "total_messages_sent": sum(r.messages_sent for r in self._results),
                "total_messages_delivered": sum(r.messages_delivered for r in self._results),
            }
