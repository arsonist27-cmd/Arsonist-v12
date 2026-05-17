"""v15 Distributed Execution.

Manages distributed execution of AI workloads across multiple regions
and nodes, supporting parallel inference, model-parallel execution,
and cross-region task coordination.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("runtime.distributed_execution")


class ExecutionMode(str, Enum):
    single_node = "single_node"
    data_parallel = "data_parallel"
    model_parallel = "model_parallel"
    pipeline_parallel = "pipeline_parallel"
    hybrid = "hybrid"


class ShardState(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class ExecutionShard(BaseModel):
    shard_id: str
    execution_id: str = ""
    region: str = ""
    node_id: str = ""
    state: ShardState = ShardState.pending
    input_size: int = 0
    output_size: int = 0
    tokens_processed: int = 0
    execution_time_ms: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0


class DistributedExecution(BaseModel):
    execution_id: str
    workload_id: str = ""
    mode: ExecutionMode = ExecutionMode.single_node
    total_shards: int = 1
    completed_shards: int = 0
    failed_shards: int = 0
    regions: List[str] = Field(default_factory=list)
    shards: List[ExecutionShard] = Field(default_factory=list)
    total_tokens: int = 0
    aggregate_time_ms: float = 0.0
    created_at: float = 0.0
    completed_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DistributedExecutionManager:
    """Manages distributed execution of workloads across regions with
    support for data-parallel, model-parallel, and pipeline-parallel modes."""

    def __init__(self, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._max_history = max_history
        self._executions: Dict[str, DistributedExecution] = {}
        self._completed: List[DistributedExecution] = []
        self._total_created = 0
        self._total_completed = 0
        self._total_shards_completed = 0
        self._events: List[Dict[str, Any]] = []

    def create_execution(self, execution: DistributedExecution) -> None:
        with self._lock:
            execution.created_at = now_ts()
            execution.total_shards = len(execution.shards)
            self._executions[execution.execution_id] = execution
            self._total_created += 1
            self._events.append({
                "type": "execution_created",
                "execution_id": execution.execution_id,
                "mode": execution.mode.value,
                "shards": execution.total_shards,
                "regions": execution.regions,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]

    def plan_execution(self, workload_id: str, mode: ExecutionMode,
                       regions: List[Dict[str, Any]], shard_count: int = 1) -> DistributedExecution:
        execution_id = f"exec-{workload_id}-{int(now_ts())}"
        shards = []
        region_ids = [r.get("region_id", f"region-{i}") for i, r in enumerate(regions)]

        for i in range(shard_count):
            region_idx = i % len(region_ids) if region_ids else 0
            shard = ExecutionShard(
                shard_id=f"{execution_id}-shard-{i}",
                execution_id=execution_id,
                region=region_ids[region_idx] if region_ids else "",
            )
            shards.append(shard)

        execution = DistributedExecution(
            execution_id=execution_id,
            workload_id=workload_id,
            mode=mode,
            regions=region_ids,
            shards=shards,
        )
        self.create_execution(execution)
        return execution

    def start_shard(self, execution_id: str, shard_id: str) -> bool:
        with self._lock:
            execution = self._executions.get(execution_id)
            if not execution:
                return False
            for shard in execution.shards:
                if shard.shard_id == shard_id:
                    shard.state = ShardState.running
                    shard.started_at = now_ts()
                    return True
            return False

    def complete_shard(self, execution_id: str, shard_id: str,
                       tokens: int = 0, success: bool = True) -> bool:
        with self._lock:
            execution = self._executions.get(execution_id)
            if not execution:
                return False
            for shard in execution.shards:
                if shard.shard_id == shard_id:
                    shard.completed_at = now_ts()
                    shard.execution_time_ms = round(
                        (shard.completed_at - shard.started_at) * 1000, 2) if shard.started_at else 0.0
                    shard.tokens_processed = tokens
                    if success:
                        shard.state = ShardState.completed
                        execution.completed_shards += 1
                        self._total_shards_completed += 1
                    else:
                        shard.state = ShardState.failed
                        execution.failed_shards += 1

                    execution.total_tokens += tokens

                    if execution.completed_shards + execution.failed_shards >= execution.total_shards:
                        self._finalize_execution(execution_id)
                    return True
            return False

    def _finalize_execution(self, execution_id: str) -> None:
        execution = self._executions.pop(execution_id, None)
        if not execution:
            return
        execution.completed_at = now_ts()
        execution.aggregate_time_ms = round(
            (execution.completed_at - execution.created_at) * 1000, 2)
        self._total_completed += 1
        self._completed.append(execution)
        if len(self._completed) > self._max_history:
            self._completed = self._completed[-self._max_history:]
        self._events.append({
            "type": "execution_completed",
            "execution_id": execution_id,
            "shards": execution.total_shards,
            "completed": execution.completed_shards,
            "failed": execution.failed_shards,
            "tokens": execution.total_tokens,
            "time_ms": execution.aggregate_time_ms,
            "ts": now_ts(),
        })

    def active_executions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.model_dump(mode="json") for e in self._executions.values()]

    def recent_completed(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.model_dump(mode="json") for e in reversed(self._completed)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_created": self._total_created,
                "total_completed": self._total_completed,
                "total_shards_completed": self._total_shards_completed,
                "active_executions": len(self._executions),
            }
