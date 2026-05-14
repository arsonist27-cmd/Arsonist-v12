from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException, Request

from federation.federation_models import FederationInboundPayload
from federation.federation_security import verify_signed_cluster_request
from storage.job_queue import build_job_record
from shared.models import JobPower, JobRequest, JobType

router = APIRouter(prefix="/federation", tags=["federation"])

# Lazy refs set by wire_federation_routes(app, memory, schedule_once)
_MEMORY = None
_SCHEDULE_ONCE = None


def wire_federation_routes(memory: Any, schedule_once_fn: Any) -> None:
    global _MEMORY, _SCHEDULE_ONCE
    _MEMORY = memory
    _SCHEDULE_ONCE = schedule_once_fn


def _federation_enabled() -> bool:
    return os.getenv("ARSONIST_FEDERATION_INBOUND", "true").lower() in ("1", "true", "yes")


@router.post("/inbound_job")
async def inbound_job(
    request: Request,
    x_federation_signature: str | None = Header(default=None, alias="X-Federation-Signature"),
) -> Dict[str, Any]:
    if not _federation_enabled():
        raise HTTPException(status_code=404, detail="federation inbound disabled")
    body = await request.json()
    ts_hdr = request.headers.get("X-Federation-Timestamp") or request.headers.get("x-federation-timestamp")
    if not verify_signed_cluster_request(body, x_federation_signature, ts_hdr):
        raise HTTPException(status_code=403, detail="invalid federation signature or timestamp")
    env = FederationInboundPayload(**body)
    if _MEMORY is None:
        raise HTTPException(status_code=503, detail="federation not wired")
    if env.global_job_id in _MEMORY.jobs:
        return {"status": "duplicate", "job_id": env.global_job_id}
    try:
        jr = JobRequest(
            id=env.global_job_id,
            type=JobType(env.type),
            task=env.task,
            required_nodes=env.required_nodes,
            power=JobPower(env.power),
            gpu_required=env.gpu_required,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    record = build_job_record(jr)
    record.originating_cluster_id = env.originating_cluster_id
    record.report_to_federation = True
    record.federation_controller_url = os.getenv("ARSONIST_FEDERATION_URL", "")
    _MEMORY.enqueue_job(record)
    if _SCHEDULE_ONCE:
        _SCHEDULE_ONCE()
    return {"status": "accepted", "job_id": env.global_job_id}


def attach_completion_hook() -> None:
    """Placeholder for future middleware hooks."""
    return
