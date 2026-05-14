from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class JobType(str, Enum):
    ai = "ai"
    code = "code"
    system = "system"
    shell = "shell"


class JobPower(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class NodeType(str, Enum):
    gpu = "GPU"
    cpu = "CPU"
    edge = "EDGE"


class JobRequest(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    type: JobType
    task: str
    required_nodes: int = Field(default=1, ge=1, le=5)
    power: JobPower
    gpu_required: bool = False


class JobRecord(JobRequest):
    assigned_nodes: List[str] = Field(default_factory=list)
    status: str = "queued"
    result: Optional[Dict[str, Any]] = None
    attempts: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0
    execution_logs: List[str] = Field(default_factory=list)
    # v9 federation (optional; single-cluster mode ignores these)
    originating_cluster_id: Optional[str] = None
    report_to_federation: bool = False
    federation_controller_url: Optional[str] = None


class NodeRegistration(BaseModel):
    node_id: str
    host: str
    port: int
    node_type: NodeType
    has_gpu: bool = False
    node_secret: str = ""


class NodeState(NodeRegistration):
    current_load: float = 0.0
    healthy: bool = True
    running_jobs: List[str] = Field(default_factory=list)
    last_seen: float = 0.0
    avg_latency_ms: float = 0.0
    queue_size: int = 0
    jobs_completed_ok: int = 0
    jobs_failed: int = 0
    total_cpu_ms: float = 0.0
    historical_load_ema: float = 0.0


class EventRecord(BaseModel):
    ts: float
    level: str
    event: str
    details: Dict[str, Any] = Field(default_factory=dict)
