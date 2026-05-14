from __future__ import annotations

import hashlib
import os
from typing import Any, Callable, Dict, Optional

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

from control_plane.mesh_bootstrap import get_runtime
from distributed_queue.replicated_queue import ReplicatedJobState
from mesh.gossip import verify_incoming_gossip
from mesh.mesh_failover import mesh_failover_snapshot
from mesh.mesh_protocol import GossipEnvelope, MeshEventType, mesh_auth_headers
from mesh.mesh_router import MeshRouter
from observability.tracing import new_trace_id, trace_headers
from shared.models import JobPower, JobRequest, JobType
from storage.job_queue import build_job_record

router = APIRouter(tags=["mesh-v10"])

_MEMORY: Any = None
_SCHEDULE_ONCE: Callable[[], None] | None = None


def wire_mesh_routes(memory: Any, schedule_once_fn: Callable[[], None]) -> None:
    global _MEMORY, _SCHEDULE_ONCE
    _MEMORY = memory
    _SCHEDULE_ONCE = schedule_once_fn


def _peer_trusted(cluster_id: str) -> bool:
    raw = os.getenv("ARSONIST_MESH_TRUSTED_PEERS", "").strip()
    if not raw:
        return True
    allowed = {x.strip() for x in raw.split(",") if x.strip()}
    return cluster_id in allowed


@router.post("/mesh/gossip")
async def mesh_gossip(
    request: Request,
    x_mesh_signature: str | None = Header(default=None, alias="X-Mesh-Signature"),
    x_mesh_timestamp: str | None = Header(default=None, alias="X-Mesh-Timestamp"),
) -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None:
        raise HTTPException(status_code=503, detail="mesh disabled")
    body = await request.json()
    if not verify_incoming_gossip(body, x_mesh_signature, x_mesh_timestamp):
        raise HTTPException(status_code=403, detail="invalid mesh signature or timestamp")
    env = GossipEnvelope(**body)
    if not _peer_trusted(env.sender.cluster_id):
        raise HTTPException(status_code=403, detail="untrusted peer")
    ack = await rt.gossip.handle_incoming(env)
    return ack.model_dump()


@router.post("/mesh/receive_routed_job")
async def receive_routed_job(
    request: Request,
    x_mesh_signature: str | None = Header(default=None, alias="X-Mesh-Signature"),
    x_mesh_timestamp: str | None = Header(default=None, alias="X-Mesh-Timestamp"),
) -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None or _MEMORY is None:
        raise HTTPException(status_code=503, detail="mesh disabled")
    body = await request.json()
    if not verify_incoming_gossip(body, x_mesh_signature, x_mesh_timestamp):
        raise HTTPException(status_code=403, detail="invalid mesh signature or timestamp")
    if not _peer_trusted(str(body.get("source_cluster_id", ""))):
        raise HTTPException(status_code=403, detail="untrusted source")
    job_id_field = str(body.get("id", "")).strip()
    if not job_id_field:
        raise HTTPException(status_code=400, detail="job id required")
    try:
        jr = JobRequest(
            id=job_id_field,
            type=JobType(body.get("type", "code")),
            task=str(body.get("task", "")),
            required_nodes=int(body.get("required_nodes", 1)),
            power=JobPower(body.get("power", "low")),
            gpu_required=bool(body.get("gpu_required", False)),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if jr.id in _MEMORY.jobs:
        return {"status": "duplicate", "job_id": jr.id}
    record = build_job_record(jr)
    record.originating_cluster_id = str(body.get("source_cluster_id", ""))
    digest = hashlib.sha256(record.task.encode("utf-8")).hexdigest()[:24]
    rt.replicated.upsert(
        ReplicatedJobState(job_id=record.id, state="queued", owner_cluster_id=rt.cluster_id, payload_digest=digest)
    )
    _MEMORY.enqueue_job(record)
    rt.event_log.append(MeshEventType.JOB_CREATED, {"job_id": record.id, "via": "mesh_route"}, rt.cluster_id)
    if _SCHEDULE_ONCE:
        _SCHEDULE_ONCE()
    rt.metrics.routes_succeeded += 1
    return {"status": "accepted", "job_id": record.id}


@router.post("/mesh/forward_job")
async def forward_job(
    request: Request,
    x_mesh_signature: str | None = Header(default=None, alias="X-Mesh-Signature"),
    x_mesh_timestamp: str | None = Header(default=None, alias="X-Mesh-Timestamp"),
) -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None or _MEMORY is None:
        raise HTTPException(status_code=503, detail="mesh disabled")
    body = await request.json()
    if not verify_incoming_gossip(body, x_mesh_signature, x_mesh_timestamp):
        raise HTTPException(status_code=403, detail="invalid mesh signature or timestamp")
    job_id = str(body.get("job_id", ""))
    job = _MEMORY.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    router = MeshRouter(rt.registry, rt.cluster_id, rt.region)
    decision = router.best_forward(job)
    if not decision:
        rt.metrics.routes_failed += 1
        raise HTTPException(status_code=409, detail="no eligible peer")
    rt.metrics.routes_attempted += 1
    payload = {
        "id": job.id,
        "type": job.type.value,
        "task": job.task,
        "required_nodes": job.required_nodes,
        "power": job.power.value,
        "gpu_required": job.gpu_required,
        "source_cluster_id": rt.cluster_id,
    }
    headers = {"Content-Type": "application/json", **mesh_auth_headers(payload), **trace_headers()}
    new_trace_id()
    url = f"{decision.target_public_url}/mesh/receive_routed_job"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=2.0)) as client:
            resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code >= 400:
            rt.metrics.routes_failed += 1
            raise HTTPException(status_code=502, detail=f"peer returned {resp.status_code}")
        rt.metrics.routes_succeeded += 1
        rt.replicated.upsert(ReplicatedJobState(job_id=job.id, state="migrated", owner_cluster_id=decision.target_cluster_id))
        return {"status": "forwarded", "target": decision.target_public_url, "score": decision.score}
    except HTTPException:
        raise
    except Exception as exc:
        rt.metrics.routes_failed += 1
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/mesh/peers")
def mesh_peers() -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None:
        raise HTTPException(status_code=503, detail="mesh disabled")
    return {"peers": [p.model_dump() for p in rt.registry.list_peers()]}


@router.get("/mesh/events")
def mesh_events(since_seq: int = 0, limit: int = 200) -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None:
        raise HTTPException(status_code=503, detail="mesh disabled")
    evs = rt.event_log.replay(since_seq=since_seq, limit=limit)
    return {"events": [e.model_dump() for e in evs], "last_seq": rt.event_log.last_seq()}


@router.post("/mesh/events/merge")
async def mesh_events_merge(
    request: Request,
    x_mesh_signature: str | None = Header(default=None, alias="X-Mesh-Signature"),
    x_mesh_timestamp: str | None = Header(default=None, alias="X-Mesh-Timestamp"),
) -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None:
        raise HTTPException(status_code=503, detail="mesh disabled")
    body = await request.json()
    if not verify_incoming_gossip(body, x_mesh_signature, x_mesh_timestamp):
        raise HTTPException(status_code=403, detail="invalid mesh signature or timestamp")
    events = body.get("events") or []
    if not isinstance(events, list):
        raise HTTPException(status_code=400, detail="events must be a list")
    inserted = rt.event_log.merge_events(events)
    rt.metrics.queue_replications += inserted
    return {"inserted": inserted}


@router.get("/mesh_metrics")
def mesh_metrics() -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None:
        return {"mesh_enabled": False}
    snap = rt.metrics.snapshot()
    snap["mesh_enabled"] = True
    snap["cluster_id"] = rt.cluster_id
    return snap


@router.get("/mesh_health")
def mesh_health() -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None:
        return {"mesh_enabled": False, "status": "standalone"}
    fo = mesh_failover_snapshot(_MEMORY, rt.registry) if _MEMORY else {}
    return {
        "mesh_enabled": True,
        "cluster_id": rt.cluster_id,
        "peer_count": len(rt.registry.list_peers()),
        "partition": rt.partition.snapshot(),
        "failover": fo,
        "raft_leader": rt.raft.is_leader() if rt.raft else True,
        "consensus_mode": os.getenv("ARSONIST_CONSENSUS_MODE", "disabled"),
    }


@router.get("/mesh_routes")
def mesh_routes_view() -> Dict[str, Any]:
    rt = get_runtime()
    if rt is None or _MEMORY is None:
        return {"mesh_enabled": False}
    router = MeshRouter(rt.registry, rt.cluster_id, rt.region)
    qpeek = _MEMORY.queue_snapshot()[:1]
    sample = None
    if qpeek:
        job = _MEMORY.jobs.get(qpeek[0])
        if job:
            sample = [r.model_dump() for r in router.rank_peers_for_job(job)[:8]]
    return {"mesh_enabled": True, "routing": router.routing_metrics(), "sample_ranking": sample}
