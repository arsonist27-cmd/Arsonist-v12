"""v16 Store-and-Forward Router.

Routes messages and workload commands across high-latency, partitioned
infrastructure using store-and-forward semantics with intelligent path
selection, link quality awareness, and burst synchronization.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("deep_space.store_forward_router")


class LinkState(str, Enum):
    active = "active"
    degraded = "degraded"
    congested = "congested"
    offline = "offline"
    blackout = "blackout"


class RouteState(str, Enum):
    available = "available"
    delayed = "delayed"
    store_only = "store_only"
    unavailable = "unavailable"


class ForwardLink(BaseModel):
    link_id: str
    source: str = ""
    destination: str = ""
    state: LinkState = LinkState.active
    latency_ms: float = 0.0
    bandwidth_kbps: float = 0.0
    packet_loss_pct: float = 0.0
    last_contact_ts: float = 0.0
    next_window_ts: float = 0.0
    window_duration_s: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ForwardRoute(BaseModel):
    route_id: str
    source: str = ""
    destination: str = ""
    hops: List[str] = Field(default_factory=list)
    total_latency_ms: float = 0.0
    min_bandwidth_kbps: float = 0.0
    state: RouteState = RouteState.available
    last_used_ts: float = 0.0


class StoreForwardRouter:
    """Routes messages across partitioned infrastructure using
    store-and-forward semantics with link quality awareness."""

    def __init__(self, local_node: str = "local", max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._local_node = local_node
        self._max_history = max_history
        self._links: Dict[str, ForwardLink] = {}
        self._routes: Dict[str, ForwardRoute] = {}
        self._stored_messages: Dict[str, List[Dict[str, Any]]] = {}
        self._total_routed = 0
        self._total_stored = 0
        self._total_forwarded = 0
        self._events: List[Dict[str, Any]] = []

    def register_link(self, link: ForwardLink) -> None:
        with self._lock:
            link.last_contact_ts = now_ts()
            self._links[link.link_id] = link
            self._add_event("link_registered", link.link_id,
                            source=link.source, destination=link.destination)

    def update_link(self, link_id: str, state: Optional[str] = None,
                    latency_ms: Optional[float] = None,
                    bandwidth_kbps: Optional[float] = None,
                    packet_loss_pct: Optional[float] = None) -> bool:
        with self._lock:
            link = self._links.get(link_id)
            if not link:
                return False
            if state:
                link.state = LinkState(state)
            if latency_ms is not None:
                link.latency_ms = latency_ms
            if bandwidth_kbps is not None:
                link.bandwidth_kbps = bandwidth_kbps
            if packet_loss_pct is not None:
                link.packet_loss_pct = packet_loss_pct
            link.last_contact_ts = now_ts()
            return True

    def find_route(self, source: str, destination: str) -> Optional[ForwardRoute]:
        with self._lock:
            best_route = None
            best_latency = float("inf")

            direct_links = [l for l in self._links.values()
                            if l.source == source and l.destination == destination
                            and l.state not in (LinkState.offline, LinkState.blackout)]
            if direct_links:
                best_link = min(direct_links, key=lambda l: l.latency_ms)
                route_state = RouteState.available
                if best_link.state == LinkState.degraded:
                    route_state = RouteState.delayed
                elif best_link.state == LinkState.congested:
                    route_state = RouteState.delayed
                best_route = ForwardRoute(
                    route_id=f"route-{source}-{destination}",
                    source=source,
                    destination=destination,
                    hops=[source, destination],
                    total_latency_ms=best_link.latency_ms,
                    min_bandwidth_kbps=best_link.bandwidth_kbps,
                    state=route_state,
                    last_used_ts=now_ts(),
                )
                return best_route

            intermediate_nodes = set()
            for l in self._links.values():
                if l.source == source and l.state not in (LinkState.offline, LinkState.blackout):
                    intermediate_nodes.add(l.destination)

            for mid in intermediate_nodes:
                second_hops = [l for l in self._links.values()
                               if l.source == mid and l.destination == destination
                               and l.state not in (LinkState.offline, LinkState.blackout)]
                if second_hops:
                    first_leg = min(
                        (l for l in self._links.values()
                         if l.source == source and l.destination == mid
                         and l.state not in (LinkState.offline, LinkState.blackout)),
                        key=lambda l: l.latency_ms
                    )
                    second_leg = min(second_hops, key=lambda l: l.latency_ms)
                    total_latency = first_leg.latency_ms + second_leg.latency_ms
                    if total_latency < best_latency:
                        best_latency = total_latency
                        best_route = ForwardRoute(
                            route_id=f"route-{source}-{mid}-{destination}",
                            source=source,
                            destination=destination,
                            hops=[source, mid, destination],
                            total_latency_ms=total_latency,
                            min_bandwidth_kbps=min(first_leg.bandwidth_kbps, second_leg.bandwidth_kbps),
                            state=RouteState.delayed,
                            last_used_ts=now_ts(),
                        )

            return best_route

    def route_message(self, destination: str, message: Dict[str, Any]) -> Optional[ForwardRoute]:
        with self._lock:
            route = self.find_route(self._local_node, destination)
            if route and route.state != RouteState.unavailable:
                self._total_routed += 1
                route.last_used_ts = now_ts()
                self._routes[route.route_id] = route
                self._add_event("message_routed", route.route_id,
                                destination=destination, hops=len(route.hops))
                return route
            else:
                if destination not in self._stored_messages:
                    self._stored_messages[destination] = []
                message["stored_at"] = now_ts()
                self._stored_messages[destination].append(message)
                self._total_stored += 1
                self._add_event("message_stored", destination,
                                queue_depth=len(self._stored_messages[destination]))
                return None

    def flush_stored(self, destination: str) -> List[Dict[str, Any]]:
        with self._lock:
            messages = self._stored_messages.pop(destination, [])
            self._total_forwarded += len(messages)
            if messages:
                self._add_event("stored_flushed", destination, count=len(messages))
            return messages

    def reachable_destinations(self) -> List[str]:
        with self._lock:
            reachable = set()
            for link in self._links.values():
                if link.state not in (LinkState.offline, LinkState.blackout):
                    if link.source == self._local_node:
                        reachable.add(link.destination)
            return sorted(reachable)

    def link_status(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [l.model_dump(mode="json") for l in self._links.values()]

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
            total_stored = sum(len(q) for q in self._stored_messages.values())
            active_links = sum(1 for l in self._links.values()
                               if l.state not in (LinkState.offline, LinkState.blackout))
            return {
                "ts": now_ts(),
                "local_node": self._local_node,
                "total_links": len(self._links),
                "active_links": active_links,
                "total_routed": self._total_routed,
                "total_stored": self._total_stored,
                "total_forwarded": self._total_forwarded,
                "messages_in_store": total_stored,
                "known_routes": len(self._routes),
            }
