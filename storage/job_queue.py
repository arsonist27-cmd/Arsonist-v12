from __future__ import annotations

from control_plane.memory import ClusterMemory
from shared.models import JobRecord, JobRequest
from shared.utils import now_ts

MAX_JOB_RETRIES = 3


def build_job_record(payload: JobRequest) -> JobRecord:
    ts = now_ts()
    return JobRecord(
        **payload.model_dump(),
        status="queued",
        created_at=ts,
        updated_at=ts,
        execution_logs=[f"{ts}: job accepted"],
    )


def log_job(memory: ClusterMemory, job: JobRecord, message: str) -> None:
    ts = now_ts()
    job.updated_at = ts
    job.execution_logs.append(f"{ts}: {message}")
    memory.save_job(job)


def mark_running(memory: ClusterMemory, job: JobRecord) -> None:
    ts = now_ts()
    if job.started_at == 0.0:
        job.started_at = ts
    job.status = "running"
    job.updated_at = ts
    job.attempts += 1
    log_job(memory, job, "job running")


def mark_finished(memory: ClusterMemory, job: JobRecord, ok: bool) -> None:
    ts = now_ts()
    job.finished_at = ts
    job.updated_at = ts
    job.status = "completed" if ok else "failed"
    log_job(memory, job, f"job finished status={job.status}")


def requeue_or_fail(memory: ClusterMemory, job: JobRecord, reason: str) -> None:
    if job.attempts >= MAX_JOB_RETRIES:
        job.status = "failed"
        log_job(memory, job, f"retry exhausted; failed: {reason}")
        return
    job.status = "queued"
    log_job(memory, job, f"requeued: {reason}")
    memory.ensure_job_queued(job.id)
