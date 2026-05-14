from __future__ import annotations

from typing import Any, Dict

from distributed_queue.event_log import EventLog
from mesh.mesh_protocol import MeshEventType


def emit_cluster_event(log: EventLog, cluster_id: str, event_type: MeshEventType, payload: Dict[str, Any]) -> None:
    log.append(event_type, payload, cluster_id)
