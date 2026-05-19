"""v16 Bandwidth Optimizer.

Optimizes bandwidth allocation across communication links with
awareness of link quality, data priority, and synchronization
requirements for delay-tolerant environments.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("communications.bandwidth_optimizer")


class AllocationStrategy(str, Enum):
    proportional = "proportional"
    priority_weighted = "priority_weighted"
    fair_share = "fair_share"
    latency_sensitive = "latency_sensitive"


class BandwidthAllocation(BaseModel):
    link_id: str
    allocated_kbps: float = 0.0
    max_kbps: float = 0.0
    utilization_pct: float = 0.0
    reserved_kbps: float = 0.0
    critical_share_pct: float = 30.0
    operational_share_pct: float = 30.0
    replication_share_pct: float = 20.0
    telemetry_share_pct: float = 10.0
    bulk_share_pct: float = 10.0
    updated_at: float = 0.0


class BandwidthOptimizer:
    """Optimizes bandwidth allocation across links with priority-based
    distribution and adaptive rebalancing."""

    def __init__(self, strategy: AllocationStrategy = AllocationStrategy.priority_weighted,
                 max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._strategy = strategy
        self._max_history = max_history
        self._allocations: Dict[str, BandwidthAllocation] = {}
        self._total_optimizations = 0
        self._events: List[Dict[str, Any]] = []

    def register_link(self, link_id: str, max_kbps: float,
                      reserved_kbps: float = 0.0) -> BandwidthAllocation:
        with self._lock:
            alloc = BandwidthAllocation(
                link_id=link_id,
                allocated_kbps=max_kbps - reserved_kbps,
                max_kbps=max_kbps,
                reserved_kbps=reserved_kbps,
                updated_at=now_ts(),
            )
            self._allocations[link_id] = alloc
            self._add_event("link_registered", link_id, max_kbps=max_kbps)
            return alloc

    def optimize(self, link_id: str, current_utilization_pct: float = 0.0,
                 latency_ms: float = 0.0,
                 packet_loss_pct: float = 0.0) -> Optional[BandwidthAllocation]:
        with self._lock:
            alloc = self._allocations.get(link_id)
            if not alloc:
                return None

            alloc.utilization_pct = current_utilization_pct
            available = alloc.max_kbps - alloc.reserved_kbps

            if self._strategy == AllocationStrategy.priority_weighted:
                if packet_loss_pct > 10:
                    alloc.critical_share_pct = 50.0
                    alloc.operational_share_pct = 30.0
                    alloc.replication_share_pct = 10.0
                    alloc.telemetry_share_pct = 5.0
                    alloc.bulk_share_pct = 5.0
                elif current_utilization_pct > 80:
                    alloc.critical_share_pct = 40.0
                    alloc.operational_share_pct = 30.0
                    alloc.replication_share_pct = 15.0
                    alloc.telemetry_share_pct = 10.0
                    alloc.bulk_share_pct = 5.0
                elif latency_ms > 1000:
                    alloc.critical_share_pct = 35.0
                    alloc.operational_share_pct = 25.0
                    alloc.replication_share_pct = 25.0
                    alloc.telemetry_share_pct = 10.0
                    alloc.bulk_share_pct = 5.0
                else:
                    alloc.critical_share_pct = 30.0
                    alloc.operational_share_pct = 30.0
                    alloc.replication_share_pct = 20.0
                    alloc.telemetry_share_pct = 10.0
                    alloc.bulk_share_pct = 10.0
            elif self._strategy == AllocationStrategy.fair_share:
                alloc.critical_share_pct = 20.0
                alloc.operational_share_pct = 20.0
                alloc.replication_share_pct = 20.0
                alloc.telemetry_share_pct = 20.0
                alloc.bulk_share_pct = 20.0

            alloc.allocated_kbps = available
            alloc.updated_at = now_ts()
            self._total_optimizations += 1
            return alloc

    def get_allocation(self, link_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            alloc = self._allocations.get(link_id)
            if not alloc:
                return None
            return alloc.model_dump(mode="json")

    def all_allocations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [a.model_dump(mode="json") for a in self._allocations.values()]

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            total_bw = sum(a.max_kbps for a in self._allocations.values())
            total_alloc = sum(a.allocated_kbps for a in self._allocations.values())
            avg_util = 0.0
            if self._allocations:
                avg_util = sum(a.utilization_pct for a in self._allocations.values()) / len(self._allocations)
            return {
                "ts": now_ts(),
                "total_links": len(self._allocations),
                "total_bandwidth_kbps": total_bw,
                "total_allocated_kbps": total_alloc,
                "avg_utilization_pct": round(avg_util, 1),
                "total_optimizations": self._total_optimizations,
                "strategy": self._strategy.value,
            }
