from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional, Set, Tuple

from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from shared.utils import now_ts, setup_logging

logger = setup_logging("fabric.topology")


class TopologyLink:
    def __init__(
        self,
        source: str,
        target: str,
        latency_ms: float = 0.0,
        bandwidth_mbps: float = 0.0,
        healthy: bool = True,
    ) -> None:
        self.source = source
        self.target = target
        self.latency_ms = latency_ms
        self.bandwidth_mbps = bandwidth_mbps
        self.healthy = healthy
        self.updated_at = now_ts()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "latency_ms": self.latency_ms,
            "bandwidth_mbps": self.bandwidth_mbps,
            "healthy": self.healthy,
            "updated_at": self.updated_at,
        }


class TopologyManager:
    """Manages the global topology graph of regions and their interconnections."""

    def __init__(self, registry: RegionRegistry) -> None:
        self.registry = registry
        self._lock = threading.Lock()
        self._links: Dict[Tuple[str, str], TopologyLink] = {}

    def update_link(
        self,
        source: str,
        target: str,
        latency_ms: float = 0.0,
        bandwidth_mbps: float = 0.0,
        healthy: bool = True,
    ) -> TopologyLink:
        link = TopologyLink(
            source=source,
            target=target,
            latency_ms=latency_ms,
            bandwidth_mbps=bandwidth_mbps,
            healthy=healthy,
        )
        with self._lock:
            self._links[(source, target)] = link
        return link

    def get_link(self, source: str, target: str) -> Optional[TopologyLink]:
        with self._lock:
            return self._links.get((source, target))

    def remove_link(self, source: str, target: str) -> None:
        with self._lock:
            self._links.pop((source, target), None)

    def neighbors(self, region_id: str) -> List[str]:
        with self._lock:
            result: Set[str] = set()
            for (src, dst), link in self._links.items():
                if not link.healthy:
                    continue
                if src == region_id:
                    result.add(dst)
                elif dst == region_id:
                    result.add(src)
            return sorted(result)

    def healthy_links(self) -> List[TopologyLink]:
        with self._lock:
            return [l for l in self._links.values() if l.healthy]

    def all_links(self) -> List[TopologyLink]:
        with self._lock:
            return list(self._links.values())

    def topology_graph(self) -> Dict[str, Any]:
        regions = self.registry.list_regions()
        nodes = []
        for r in regions:
            nodes.append({
                "id": r.region_id,
                "label": r.display_name or r.region_id,
                "location": r.geographic_location,
                "lat": r.latitude,
                "lon": r.longitude,
                "status": r.status.value,
                "type": r.region_type.value,
            })
        edges = []
        with self._lock:
            for link in self._links.values():
                edges.append(link.to_dict())
        return {
            "ts": now_ts(),
            "nodes": nodes,
            "edges": edges,
        }

    def shortest_path(self, source: str, target: str) -> Optional[List[str]]:
        graph: Dict[str, List[Tuple[str, float]]] = {}
        with self._lock:
            for (src, dst), link in self._links.items():
                if not link.healthy:
                    continue
                if src not in graph:
                    graph[src] = []
                graph[src].append((dst, link.latency_ms))
                if dst not in graph:
                    graph[dst] = []
                graph[dst].append((src, link.latency_ms))

        if source not in graph:
            return None

        import heapq
        dist: Dict[str, float] = {source: 0.0}
        prev: Dict[str, Optional[str]] = {source: None}
        heap = [(0.0, source)]
        visited: Set[str] = set()

        while heap:
            d, node = heapq.heappop(heap)
            if node in visited:
                continue
            visited.add(node)
            if node == target:
                path = []
                cur: Optional[str] = target
                while cur is not None:
                    path.append(cur)
                    cur = prev.get(cur)
                return list(reversed(path))
            for neighbor, weight in graph.get(node, []):
                if neighbor in visited:
                    continue
                new_dist = d + weight
                if new_dist < dist.get(neighbor, float("inf")):
                    dist[neighbor] = new_dist
                    prev[neighbor] = node
                    heapq.heappush(heap, (new_dist, neighbor))
        return None

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            total = len(self._links)
            healthy = sum(1 for l in self._links.values() if l.healthy)
            latencies = [l.latency_ms for l in self._links.values() if l.healthy]
            return {
                "ts": now_ts(),
                "total_links": total,
                "healthy_links": healthy,
                "unhealthy_links": total - healthy,
                "avg_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
            }
