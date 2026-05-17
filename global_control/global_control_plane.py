from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from global_control.consensus import GlobalConsensus
from global_control.global_state import GlobalState
from global_control.replication import ReplicationMode, StateReplicator
from regions.latency_map import LatencyMap
from regions.region_health import RegionHealthMonitor
from regions.region_registry import RegionRecord, RegionRegistry, RegionStatus
from regions.regional_capacity import RegionalCapacityTracker
from shared.utils import now_ts, setup_logging

logger = setup_logging("global_control.plane")


class GlobalControlPlane:
    """Top-level coordinator for the global AI compute fabric.

    Aggregates region registry, consensus, state replication,
    health monitoring, and capacity tracking into one interface.
    """

    def __init__(
        self,
        node_id: str | None = None,
        region_db_path: str | None = None,
        state_db_path: str | None = None,
    ) -> None:
        self.node_id = node_id or os.getenv("ARSONIST_GLOBAL_NODE_ID", "global-primary")
        self.region_registry = RegionRegistry(db_path=region_db_path)
        self.global_state = GlobalState(db_path=state_db_path)
        self.latency_map = LatencyMap()
        self.capacity_tracker = RegionalCapacityTracker(self.region_registry)
        self.health_monitor = RegionHealthMonitor(self.region_registry)
        self.consensus = GlobalConsensus(
            node_id=self.node_id,
            on_promote=self._on_leader_promote,
            on_demote=self._on_leader_demote,
        )
        replication_mode = ReplicationMode(
            os.getenv("ARSONIST_REPLICATION_MODE", "async")
        )
        self.replicator = StateReplicator(
            local_region_id=self.node_id,
            state=self.global_state,
            mode=replication_mode,
        )
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self.consensus.start()
        self.health_monitor.start()
        self.replicator.start()
        self._started = True
        logger.info("Global control plane started (node=%s)", self.node_id)

    def stop(self) -> None:
        self.replicator.stop()
        self.health_monitor.stop()
        self.consensus.stop()
        self._started = False
        logger.info("Global control plane stopped")

    def _on_leader_promote(self) -> None:
        logger.info("This node is now the global leader")

    def _on_leader_demote(self) -> None:
        logger.info("This node is no longer the global leader")

    def register_region(self, region: RegionRecord) -> RegionRecord:
        result = self.region_registry.register(region)
        self.global_state.put("regions", region.region_id, region.model_dump(mode="json"))
        return result

    def remove_region(self, region_id: str) -> None:
        self.region_registry.remove(region_id)
        self.global_state.delete("regions", region_id)

    def region_heartbeat(self, region_id: str, updates: Dict[str, Any] | None = None) -> Optional[RegionRecord]:
        return self.region_registry.heartbeat(region_id, updates)

    def get_region(self, region_id: str) -> Optional[RegionRecord]:
        return self.region_registry.get(region_id)

    def list_regions(self, status: Optional[RegionStatus] = None) -> List[RegionRecord]:
        return self.region_registry.list_regions(status=status)

    def active_regions(self) -> List[RegionRecord]:
        return self.region_registry.active_regions()

    def global_status(self) -> Dict[str, Any]:
        return {
            "ts": now_ts(),
            "node_id": self.node_id,
            "consensus": self.consensus.status(),
            "health": self.health_monitor.region_health_summary(),
            "capacity": self.capacity_tracker.global_capacity_summary(),
            "latency": self.latency_map.summary(),
            "replication": self.replicator.status(),
        }
