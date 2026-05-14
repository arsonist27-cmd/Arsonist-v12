from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class MeshMetricsCollector:
    gossip_messages_in: int = 0
    gossip_success: int = 0
    gossip_failures: int = 0
    routes_attempted: int = 0
    routes_succeeded: int = 0
    routes_failed: int = 0
    queue_replications: int = 0
    anti_entropy_ticks: int = 0
    edge_sync_bytes: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "gossip_messages_in": self.gossip_messages_in,
            "gossip_success": self.gossip_success,
            "gossip_failures": self.gossip_failures,
            "routes_attempted": self.routes_attempted,
            "routes_succeeded": self.routes_succeeded,
            "routes_failed": self.routes_failed,
            "queue_replications": self.queue_replications,
            "anti_entropy_ticks": self.anti_entropy_ticks,
            "edge_sync_bytes": self.edge_sync_bytes,
            "extra": dict(self.extra),
        }
