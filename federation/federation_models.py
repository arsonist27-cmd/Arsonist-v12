from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ClusterHealth(str, Enum):
    healthy = "healthy"
    degraded = "degraded"
    offline = "offline"
    unknown = "unknown"


class ClusterRegistration(BaseModel):
    cluster_id: str
    region: str = "default"
    control_plane_url: str = Field(..., description="Base URL e.g. http://cluster-a:8000")
    api_token: str = ""
    node_count: int = 0
    gpu_capacity: int = 0
    current_load: float = 0.0
    queue_depth: int = 0
    avg_latency_ms: float = 50.0
    health_state: ClusterHealth = ClusterHealth.healthy


class ClusterRecord(ClusterRegistration):
    last_heartbeat: float = 0.0
    registered_at: float = 0.0
    consecutive_misses: int = 0


class GlobalJobStatus(str, Enum):
    queued = "queued"
    routed = "routed"
    running = "running"
    completed = "completed"
    failed = "failed"
    migrated = "migrated"


class GlobalJobRecord(BaseModel):
    id: str
    type: str
    task: str
    required_nodes: int = 1
    power: str = "low"
    gpu_required: bool = False
    status: GlobalJobStatus = GlobalJobStatus.queued
    originating_cluster_id: Optional[str] = None
    preferred_region: Optional[str] = None
    assigned_cluster_id: Optional[str] = None
    execution_logs: List[str] = Field(default_factory=list)
    result: Optional[Dict[str, Any]] = None
    created_at: float = 0.0
    updated_at: float = 0.0


class FederationHeartbeatPayload(BaseModel):
    cluster_id: str
    node_count: int = 0
    gpu_capacity: int = 0
    current_load: float = 0.0
    queue_depth: int = 0
    avg_latency_ms: float = 50.0
    health_state: ClusterHealth = ClusterHealth.healthy


class RoutingDecision(BaseModel):
    target_cluster_id: str
    score: float
    ranked: List[Dict[str, Any]] = Field(default_factory=list)
    decision_ms: float = 0.0


class FailoverEvent(BaseModel):
    ts: float
    dead_cluster_id: str
    jobs_rerouted: int
    detail: str = ""


class FederationInboundPayload(BaseModel):
    """Job envelope pushed from federation controller to a cluster control plane."""

    global_job_id: str
    originating_cluster_id: Optional[str] = None
    preferred_region: Optional[str] = None
    type: str = "code"
    task: str = ""
    required_nodes: int = 1
    power: str = "low"
    gpu_required: bool = False


class GlobalJobCompletePayload(BaseModel):
    job_id: str
    cluster_id: str
    ok: bool
    result: Optional[Dict[str, Any]] = None
