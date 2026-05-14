from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MeshEventType(str, Enum):
    JOB_CREATED = "JOB_CREATED"
    JOB_STARTED = "JOB_STARTED"
    JOB_COMPLETED = "JOB_COMPLETED"
    JOB_FAILED = "JOB_FAILED"
    NODE_JOINED = "NODE_JOINED"
    NODE_LEFT = "NODE_LEFT"
    PEER_DISCOVERED = "PEER_DISCOVERED"
    PARTITION_DETECTED = "PARTITION_DETECTED"


def _mesh_secret() -> str:
    return (
        os.getenv("ARSONIST_MESH_HMAC_SECRET", "").strip()
        or os.getenv("ARSONIST_FEDERATION_SECRET", os.getenv("FEDERATION_SHARED_SECRET", "")).strip()
    )


def _max_ts_skew_sec() -> int:
    return int(os.getenv("ARSONIST_MESH_SIGNATURE_MAX_SKEW_SEC", os.getenv("FEDERATION_SIGNATURE_MAX_SKEW_SEC", "300")))


def canonical_json(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_mesh_payload(payload: Dict[str, Any]) -> str:
    key = _mesh_secret().encode("utf-8")
    return hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()


def verify_mesh_payload(payload: Dict[str, Any], signature: str | None) -> bool:
    if not _mesh_secret():
        return True
    if not signature:
        return False
    return hmac.compare_digest(sign_mesh_payload(payload), signature)


def verify_mesh_timestamp(ts_header: str | None) -> bool:
    if not _mesh_secret():
        return True
    if not ts_header:
        return False
    try:
        ts = int(str(ts_header).strip())
    except ValueError:
        return False
    return abs(int(time.time()) - ts) <= _max_ts_skew_sec()


def mesh_auth_headers(payload: Dict[str, Any]) -> Dict[str, str]:
    return {"X-Mesh-Signature": sign_mesh_payload(payload), "X-Mesh-Timestamp": str(int(time.time()))}


class ClusterGossipState(BaseModel):
    """Ephemeral cluster view exchanged over gossip (eventually consistent)."""

    cluster_id: str
    public_url: str
    region: str = "default"
    gpu_capacity: int = 0
    load: float = 0.0
    health: str = "healthy"  # healthy | degraded | offline
    queue_depth: int = 0
    latency_ms: float = 0.0
    heartbeat_ts: float = 0.0
    version: int = 0
    reliability_score: float = 1.0
    hop_distance: int = 0


class GossipEnvelope(BaseModel):
    sender: ClusterGossipState
    peers: List[ClusterGossipState] = Field(default_factory=list)
    trace_id: str = ""
    nonce: str = ""


class GossipAck(BaseModel):
    merged_count: int = 0
    accepted: bool = True
    message: str = ""
