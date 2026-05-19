"""v16 Delay-Tolerant Queue.

Provides asynchronous delivery, store-and-forward execution, and delayed
synchronization for infrastructure regions experiencing minutes of
communication delay, temporary network isolation, or partitioned
infrastructure.
"""
from __future__ import annotations

import threading
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("deep_space.delay_tolerant_queue")


class MessagePriority(str, Enum):
    critical = "critical"
    high = "high"
    normal = "normal"
    low = "low"
    background = "background"


class MessageState(str, Enum):
    pending = "pending"
    queued = "queued"
    in_transit = "in_transit"
    delivered = "delivered"
    failed = "failed"
    expired = "expired"


class DelayTolerantMessage(BaseModel):
    message_id: str = ""
    source_region: str = ""
    destination_region: str = ""
    priority: MessagePriority = MessagePriority.normal
    state: MessageState = MessageState.pending
    payload_type: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    ttl_s: float = 3600.0
    max_retries: int = 5
    retry_count: int = 0
    created_at: float = 0.0
    queued_at: float = 0.0
    delivered_at: float = 0.0
    estimated_delay_s: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DelayTolerantQueue:
    """Store-and-forward message queue for delay-tolerant networking.

    Supports asynchronous delivery across partitioned infrastructure,
    disconnected regions, and high-latency links with configurable TTL
    and priority-based ordering.
    """

    def __init__(self, max_queue: int = 1000000, max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_queue = max_queue
        self._max_history = max_history
        self._pending: Dict[str, DelayTolerantMessage] = {}
        self._in_transit: Dict[str, DelayTolerantMessage] = {}
        self._delivered: List[DelayTolerantMessage] = []
        self._failed: List[DelayTolerantMessage] = []
        self._total_enqueued = 0
        self._total_delivered = 0
        self._total_failed = 0
        self._total_expired = 0
        self._events: List[Dict[str, Any]] = []

    def enqueue(self, msg: DelayTolerantMessage) -> DelayTolerantMessage:
        with self._lock:
            if len(self._pending) >= self._max_queue:
                logger.warning("queue full, rejecting message %s", msg.message_id)
                msg.state = MessageState.failed
                return msg

            if not msg.message_id:
                msg.message_id = f"dtm-{uuid.uuid4().hex[:12]}"
            msg.state = MessageState.queued
            msg.created_at = now_ts()
            msg.queued_at = now_ts()
            self._pending[msg.message_id] = msg
            self._total_enqueued += 1
            self._add_event("message_enqueued", msg.message_id,
                            destination=msg.destination_region,
                            priority=msg.priority.value)
            return msg

    def enqueue_batch(self, messages: List[DelayTolerantMessage]) -> int:
        priority_order = {
            MessagePriority.critical: 0,
            MessagePriority.high: 1,
            MessagePriority.normal: 2,
            MessagePriority.low: 3,
            MessagePriority.background: 4,
        }
        sorted_msgs = sorted(messages, key=lambda m: priority_order.get(m.priority, 2))
        accepted = 0
        for m in sorted_msgs:
            result = self.enqueue(m)
            if result.state == MessageState.queued:
                accepted += 1
        return accepted

    def dequeue(self, destination: str = "", limit: int = 100) -> List[DelayTolerantMessage]:
        with self._lock:
            ts = now_ts()
            ready = []
            expired_ids = []

            for mid, msg in self._pending.items():
                if msg.ttl_s > 0 and (ts - msg.created_at) > msg.ttl_s:
                    expired_ids.append(mid)
                    continue
                if destination and msg.destination_region != destination:
                    continue
                ready.append((mid, msg))

            for mid in expired_ids:
                expired_msg = self._pending.pop(mid)
                expired_msg.state = MessageState.expired
                self._total_expired += 1
                self._failed.append(expired_msg)
                if len(self._failed) > self._max_history:
                    self._failed = self._failed[-self._max_history:]

            priority_order = {
                MessagePriority.critical: 0,
                MessagePriority.high: 1,
                MessagePriority.normal: 2,
                MessagePriority.low: 3,
                MessagePriority.background: 4,
            }
            ready.sort(key=lambda x: (priority_order.get(x[1].priority, 2), x[1].created_at))

            result = []
            for mid, msg in ready[:limit]:
                msg.state = MessageState.in_transit
                self._in_transit[mid] = self._pending.pop(mid)
                result.append(msg)

            return result

    def acknowledge(self, message_id: str) -> bool:
        with self._lock:
            msg = self._in_transit.pop(message_id, None)
            if not msg:
                return False
            msg.state = MessageState.delivered
            msg.delivered_at = now_ts()
            self._total_delivered += 1
            self._delivered.append(msg)
            if len(self._delivered) > self._max_history:
                self._delivered = self._delivered[-self._max_history:]
            self._add_event("message_delivered", message_id,
                            delay_s=round(msg.delivered_at - msg.created_at, 3))
            return True

    def nack(self, message_id: str) -> bool:
        with self._lock:
            msg = self._in_transit.pop(message_id, None)
            if not msg:
                return False
            msg.retry_count += 1
            if msg.retry_count >= msg.max_retries:
                msg.state = MessageState.failed
                self._total_failed += 1
                self._failed.append(msg)
                if len(self._failed) > self._max_history:
                    self._failed = self._failed[-self._max_history:]
                self._add_event("message_failed", message_id, retries=msg.retry_count)
            else:
                msg.state = MessageState.queued
                self._pending[message_id] = msg
                self._add_event("message_retried", message_id, retry=msg.retry_count)
            return True

    def expire_stale(self) -> int:
        with self._lock:
            ts = now_ts()
            expired = 0
            stale_ids = []
            for mid, msg in self._pending.items():
                if msg.ttl_s > 0 and (ts - msg.created_at) > msg.ttl_s:
                    stale_ids.append(mid)
            for mid in stale_ids:
                msg = self._pending.pop(mid)
                msg.state = MessageState.expired
                self._total_expired += 1
                self._failed.append(msg)
                expired += 1
            if len(self._failed) > self._max_history:
                self._failed = self._failed[-self._max_history:]
            return expired

    def queue_depth(self, destination: str = "") -> int:
        with self._lock:
            if destination:
                return sum(1 for m in self._pending.values()
                           if m.destination_region == destination)
            return len(self._pending)

    def _add_event(self, event_type: str, message_id: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "message_id": message_id, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            recent = self._delivered[-100:] if self._delivered else []
            avg_delay = (sum(m.delivered_at - m.created_at for m in recent) / len(recent)) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_enqueued": self._total_enqueued,
                "total_delivered": self._total_delivered,
                "total_failed": self._total_failed,
                "total_expired": self._total_expired,
                "pending": len(self._pending),
                "in_transit": len(self._in_transit),
                "avg_delivery_delay_s": round(avg_delay, 3),
            }
