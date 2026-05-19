"""v16 Orbital Routing.

Routes requests across orbital and ground infrastructure with awareness
of signal propagation delay, contact windows, bandwidth constraints,
and link quality for multi-orbit compute environments.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("orbital.orbital_routing")


class RouteType(str, Enum):
    direct = "direct"
    relay = "relay"
    store_forward = "store_forward"
    burst = "burst"
    deferred = "deferred"


class OrbitalRoute(BaseModel):
    route_id: str
    source: str = ""
    destination: str = ""
    route_type: RouteType = RouteType.direct
    hops: List[str] = Field(default_factory=list)
    total_latency_ms: float = 0.0
    bandwidth_kbps: float = 0.0
    reliability_pct: float = 99.0
    next_available_ts: float = 0.0
    window_duration_s: float = 0.0
    is_active: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RoutingDecision(BaseModel):
    request_id: str
    source: str = ""
    destination: str = ""
    selected_route: str = ""
    route_type: RouteType = RouteType.direct
    estimated_latency_ms: float = 0.0
    bandwidth_kbps: float = 0.0
    deferred: bool = False
    defer_until_ts: float = 0.0
    ts: float = 0.0


class OrbitalRouter:
    """Routes requests across orbital infrastructure with contact-window
    awareness, multi-hop relay support, and store-forward fallback."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._routes: Dict[str, OrbitalRoute] = {}
        self._decisions: List[RoutingDecision] = []
        self._total_routed = 0
        self._total_deferred = 0
        self._total_relayed = 0
        self._events: List[Dict[str, Any]] = []

    def register_route(self, route: OrbitalRoute) -> None:
        with self._lock:
            self._routes[route.route_id] = route
            self._add_event("route_registered", route.route_id,
                            src=route.source, dst=route.destination,
                            route_type=route.route_type.value)

    def remove_route(self, route_id: str) -> bool:
        with self._lock:
            if route_id not in self._routes:
                return False
            del self._routes[route_id]
            return True

    def update_route(self, route_id: str, is_active: Optional[bool] = None,
                     latency_ms: Optional[float] = None,
                     bandwidth_kbps: Optional[float] = None) -> bool:
        with self._lock:
            route = self._routes.get(route_id)
            if not route:
                return False
            if is_active is not None:
                route.is_active = is_active
            if latency_ms is not None:
                route.total_latency_ms = latency_ms
            if bandwidth_kbps is not None:
                route.bandwidth_kbps = bandwidth_kbps
            return True

    def route_request(self, request_id: str, source: str,
                      destination: str, max_latency_ms: float = 0.0,
                      min_bandwidth_kbps: float = 0.0) -> RoutingDecision:
        with self._lock:
            ts = now_ts()
            candidates = [r for r in self._routes.values()
                          if r.source == source and r.destination == destination
                          and r.is_active]

            if max_latency_ms > 0:
                latency_ok = [r for r in candidates if r.total_latency_ms <= max_latency_ms]
                if latency_ok:
                    candidates = latency_ok

            if min_bandwidth_kbps > 0:
                bw_ok = [r for r in candidates if r.bandwidth_kbps >= min_bandwidth_kbps]
                if bw_ok:
                    candidates = bw_ok

            if not candidates:
                relay_candidates = self._find_relay_routes(source, destination)
                if relay_candidates:
                    candidates = relay_candidates

            if not candidates:
                decision = RoutingDecision(
                    request_id=request_id,
                    source=source,
                    destination=destination,
                    route_type=RouteType.deferred,
                    deferred=True,
                    defer_until_ts=ts + 60,
                    ts=ts,
                )
                self._total_deferred += 1
                self._add_event("request_deferred", request_id,
                                src=source, dst=destination)
            else:
                best = min(candidates, key=lambda r: r.total_latency_ms)
                decision = RoutingDecision(
                    request_id=request_id,
                    source=source,
                    destination=destination,
                    selected_route=best.route_id,
                    route_type=best.route_type,
                    estimated_latency_ms=best.total_latency_ms,
                    bandwidth_kbps=best.bandwidth_kbps,
                    ts=ts,
                )
                self._total_routed += 1
                if best.route_type == RouteType.relay:
                    self._total_relayed += 1
                self._add_event("request_routed", request_id,
                                route=best.route_id,
                                route_type=best.route_type.value)

            self._decisions.append(decision)
            if len(self._decisions) > self._max_history:
                self._decisions = self._decisions[-self._max_history:]
            return decision

    def _find_relay_routes(self, source: str, destination: str) -> List[OrbitalRoute]:
        relays = []
        intermediate = set()
        for r in self._routes.values():
            if r.source == source and r.is_active:
                intermediate.add(r.destination)

        for mid in intermediate:
            second_legs = [r for r in self._routes.values()
                           if r.source == mid and r.destination == destination
                           and r.is_active]
            if second_legs:
                first_leg = min(
                    (r for r in self._routes.values()
                     if r.source == source and r.destination == mid and r.is_active),
                    key=lambda r: r.total_latency_ms
                )
                second_leg = min(second_legs, key=lambda r: r.total_latency_ms)
                relay_route = OrbitalRoute(
                    route_id=f"relay-{source}-{mid}-{destination}",
                    source=source,
                    destination=destination,
                    route_type=RouteType.relay,
                    hops=[source, mid, destination],
                    total_latency_ms=first_leg.total_latency_ms + second_leg.total_latency_ms,
                    bandwidth_kbps=min(first_leg.bandwidth_kbps, second_leg.bandwidth_kbps),
                    reliability_pct=first_leg.reliability_pct * second_leg.reliability_pct / 100,
                    is_active=True,
                )
                relays.append(relay_route)
        return relays

    def active_routes(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._routes.values() if r.is_active]

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def recent_decisions(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.model_dump(mode="json") for d in reversed(self._decisions)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            active = sum(1 for r in self._routes.values() if r.is_active)
            recent = self._decisions[-100:] if self._decisions else []
            avg_latency = sum(d.estimated_latency_ms for d in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_routes": len(self._routes),
                "active_routes": active,
                "total_routed": self._total_routed,
                "total_deferred": self._total_deferred,
                "total_relayed": self._total_relayed,
                "avg_latency_ms": round(avg_latency, 1),
            }
