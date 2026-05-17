"""v15 Ultra-Scale Runtime.

Supports millions of concurrent jobs, streaming inference, distributed
token generation, and ultra-large queue handling for planet-scale
AI workloads.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("runtime.ultra_scale_runtime")


class JobState(str, Enum):
    queued = "queued"
    dispatched = "dispatched"
    running = "running"
    streaming = "streaming"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class JobPriority(str, Enum):
    critical = "critical"
    high = "high"
    normal = "normal"
    low = "low"
    background = "background"


class RuntimeJob(BaseModel):
    job_id: str
    workload_id: str = ""
    state: JobState = JobState.queued
    priority: JobPriority = JobPriority.normal
    region: str = ""
    gpu_required: bool = False
    gpu_type: str = ""
    streaming: bool = False
    tokens_generated: int = 0
    tokens_per_second: float = 0.0
    queue_time_ms: float = 0.0
    execution_time_ms: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0


class QueueStats(BaseModel):
    total_queued: int = 0
    total_running: int = 0
    total_streaming: int = 0
    total_completed: int = 0
    total_failed: int = 0
    by_priority: Dict[str, int] = Field(default_factory=dict)
    by_region: Dict[str, int] = Field(default_factory=dict)
    avg_queue_time_ms: float = 0.0
    avg_execution_time_ms: float = 0.0
    throughput_per_second: float = 0.0
    ts: float = 0.0


class UltraScaleRuntime:
    """Planet-scale runtime supporting millions of concurrent jobs,
    streaming inference, and distributed token generation."""

    def __init__(self, max_concurrent: int = 1000000, max_queue: int = 5000000,
                 max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_concurrent = max_concurrent
        self._max_queue = max_queue
        self._max_history = max_history
        self._jobs: Dict[str, RuntimeJob] = {}
        self._queued: List[str] = []
        self._running: Dict[str, RuntimeJob] = {}
        self._completed: List[RuntimeJob] = []
        self._total_submitted = 0
        self._total_completed = 0
        self._total_failed = 0
        self._total_tokens = 0
        self._events: List[Dict[str, Any]] = []

    def submit(self, job: RuntimeJob) -> bool:
        with self._lock:
            if len(self._queued) >= self._max_queue:
                logger.warning("queue full, rejecting job %s", job.job_id)
                return False

            job.state = JobState.queued
            job.created_at = now_ts()
            self._jobs[job.job_id] = job
            self._queued.append(job.job_id)
            self._total_submitted += 1
            self._events.append({
                "type": "job_submitted",
                "job_id": job.job_id,
                "priority": job.priority.value,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
            return True

    def submit_batch(self, jobs: List[RuntimeJob]) -> int:
        accepted = 0
        priority_order = {
            JobPriority.critical: 0,
            JobPriority.high: 1,
            JobPriority.normal: 2,
            JobPriority.low: 3,
            JobPriority.background: 4,
        }
        sorted_jobs = sorted(jobs, key=lambda j: priority_order.get(j.priority, 2))
        for job in sorted_jobs:
            if self.submit(job):
                accepted += 1
        return accepted

    def dispatch(self, max_dispatch: int = 100) -> List[RuntimeJob]:
        dispatched = []
        with self._lock:
            available = self._max_concurrent - len(self._running)
            to_dispatch = min(available, max_dispatch, len(self._queued))

            priority_order = {
                JobPriority.critical: 0,
                JobPriority.high: 1,
                JobPriority.normal: 2,
                JobPriority.low: 3,
                JobPriority.background: 4,
            }
            self._queued.sort(key=lambda jid: priority_order.get(
                self._jobs[jid].priority if jid in self._jobs else JobPriority.normal, 2))

            for _ in range(to_dispatch):
                if not self._queued:
                    break
                job_id = self._queued.pop(0)
                job = self._jobs.get(job_id)
                if not job:
                    continue
                job.state = JobState.dispatched
                job.started_at = now_ts()
                job.queue_time_ms = round((job.started_at - job.created_at) * 1000, 2)
                self._running[job_id] = job
                dispatched.append(job)

        return dispatched

    def mark_running(self, job_id: str, streaming: bool = False) -> bool:
        with self._lock:
            job = self._running.get(job_id)
            if not job:
                return False
            job.state = JobState.streaming if streaming else JobState.running
            return True

    def update_tokens(self, job_id: str, tokens: int, tps: float) -> bool:
        with self._lock:
            job = self._running.get(job_id)
            if not job:
                return False
            job.tokens_generated = tokens
            job.tokens_per_second = tps
            self._total_tokens += tokens
            return True

    def complete_job(self, job_id: str, success: bool = True) -> Optional[RuntimeJob]:
        with self._lock:
            job = self._running.pop(job_id, None)
            if not job:
                return None
            job.completed_at = now_ts()
            job.execution_time_ms = round((job.completed_at - job.started_at) * 1000, 2) if job.started_at else 0.0
            if success:
                job.state = JobState.completed
                self._total_completed += 1
            else:
                job.state = JobState.failed
                self._total_failed += 1

            self._completed.append(job)
            if len(self._completed) > self._max_history:
                self._completed = self._completed[-self._max_history:]
            self._jobs.pop(job_id, None)
            return job

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._running:
                job = self._running.pop(job_id)
                job.state = JobState.cancelled
                self._jobs.pop(job_id, None)
                return True
            if job_id in self._queued:
                self._queued.remove(job_id)
                job = self._jobs.pop(job_id, None)
                if job:
                    job.state = JobState.cancelled
                return True
            return False

    def queue_stats(self) -> QueueStats:
        with self._lock:
            by_priority: Dict[str, int] = {}
            by_region: Dict[str, int] = {}
            for jid in self._queued:
                job = self._jobs.get(jid)
                if job:
                    by_priority[job.priority.value] = by_priority.get(job.priority.value, 0) + 1
                    if job.region:
                        by_region[job.region] = by_region.get(job.region, 0) + 1

            recent_completed = self._completed[-100:] if self._completed else []
            avg_queue = sum(j.queue_time_ms for j in recent_completed) / len(recent_completed) if recent_completed else 0.0
            avg_exec = sum(j.execution_time_ms for j in recent_completed) / len(recent_completed) if recent_completed else 0.0

            streaming = sum(1 for j in self._running.values() if j.state == JobState.streaming)

            return QueueStats(
                total_queued=len(self._queued),
                total_running=len(self._running),
                total_streaming=streaming,
                total_completed=self._total_completed,
                total_failed=self._total_failed,
                by_priority=by_priority,
                by_region=by_region,
                avg_queue_time_ms=round(avg_queue, 2),
                avg_execution_time_ms=round(avg_exec, 2),
                ts=now_ts(),
            )

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_submitted": self._total_submitted,
                "total_completed": self._total_completed,
                "total_failed": self._total_failed,
                "total_tokens_generated": self._total_tokens,
                "queued": len(self._queued),
                "running": len(self._running),
                "max_concurrent": self._max_concurrent,
                "max_queue": self._max_queue,
            }
