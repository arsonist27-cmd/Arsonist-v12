from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from federation.failover import reroute_jobs_from_dead_cluster
from federation.federation_models import (
    ClusterHealth,
    ClusterRecord,
    ClusterRegistration,
    FederationHeartbeatPayload,
    FederationInboundPayload,
    GlobalJobCompletePayload,
    GlobalJobRecord,
    GlobalJobStatus,
)
from federation.federation_security import build_headers, verify_signed_cluster_request
from federation.heartbeat import apply_heartbeat, sweep_stale_clusters
from federation.registry import FederationRegistry
from federation.router import decide_route
from shared.utils import now_ts, setup_logging

logger = setup_logging("federation.controller")

REGISTRY = FederationRegistry()
API_TOKEN = os.getenv("FEDERATION_API_TOKEN", os.getenv("ARSONIST_FEDERATION_TOKEN", ""))


def require_federation_token(authorization: str | None = Header(default=None)) -> None:
    if not API_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")


class SubmitGlobalJobBody(BaseModel):
    id: Optional[str] = None
    type: str = "code"
    task: str = ""
    required_nodes: int = Field(default=1, ge=1, le=5)
    power: str = "low"
    gpu_required: bool = False
    originating_cluster_id: Optional[str] = None
    preferred_region: Optional[str] = None


async def push_inbound(cluster: ClusterRecord, job: GlobalJobRecord) -> None:
    raw = FederationInboundPayload(
        global_job_id=job.id,
        originating_cluster_id=job.originating_cluster_id,
        preferred_region=job.preferred_region,
        type=job.type,
        task=job.task,
        required_nodes=job.required_nodes,
        power=job.power,
        gpu_required=job.gpu_required,
    ).model_dump(mode="json", exclude_none=True)
    headers = dict(build_headers(raw))
    if cluster.api_token:
        headers["Authorization"] = f"Bearer {cluster.api_token}"
    url = f"{cluster.control_plane_url.rstrip('/')}/federation/inbound_job"
    timeout = httpx.Timeout(5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=raw, headers=headers)
        resp.raise_for_status()


async def federation_watch_loop() -> None:
    interval = float(os.getenv("FEDERATION_HEARTBEAT_SWEEP_SEC", "10"))
    while True:
        await asyncio.sleep(interval)
        try:
            dead = await asyncio.to_thread(sweep_stale_clusters, REGISTRY)
            for cid in dead:
                logger.warning("cluster marked offline: %s", cid)
                _, pushes = await asyncio.to_thread(reroute_jobs_from_dead_cluster, REGISTRY, cid)
                for job, cluster in pushes:
                    try:
                        await push_inbound(cluster, job)
                    except Exception:
                        logger.exception("failover push failed job=%s cluster=%s", job.id, cluster.cluster_id)
        except Exception:
            logger.exception("federation watch iteration failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(federation_watch_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Arsonist OS v9 Federation Controller", lifespan=lifespan)


def _normalize_cp_url(url: str) -> str:
    return url.rstrip("/").lower()


@app.post("/register_cluster")
async def register_cluster(
    request: Request,
    _: None = Depends(require_federation_token),
) -> Dict[str, Any]:
    raw = await request.json()
    sig = request.headers.get("x-federation-signature") or request.headers.get("X-Federation-Signature")
    ts_hdr = request.headers.get("x-federation-timestamp") or request.headers.get("X-Federation-Timestamp")
    if not verify_signed_cluster_request(raw, sig, ts_hdr):
        raise HTTPException(status_code=403, detail="invalid federation body signature or timestamp")
    body = ClusterRegistration.model_validate(raw)
    url_norm = _normalize_cp_url(body.control_plane_url)
    for c in REGISTRY.list_clusters():
        if c.cluster_id == body.cluster_id:
            continue
        if _normalize_cp_url(c.control_plane_url) == url_norm:
            raise HTTPException(
                status_code=409,
                detail=f"control_plane_url already registered as cluster_id={c.cluster_id}",
            )
    existing = REGISTRY.get_cluster(body.cluster_id)
    rec = ClusterRecord(
        **body.model_dump(mode="json"),
        last_heartbeat=now_ts(),
        registered_at=existing.registered_at if existing else now_ts(),
        consecutive_misses=0,
    )
    REGISTRY.upsert_cluster(rec)
    REGISTRY.emit_event("cluster_register", {"cluster_id": body.cluster_id, "region": body.region})
    return {"status": "registered", "cluster_id": body.cluster_id}


@app.get("/clusters")
async def list_clusters(_: None = Depends(require_federation_token)) -> Dict[str, Any]:
    clusters = REGISTRY.list_clusters()
    return {"clusters": [c.model_dump(mode="json") for c in clusters]}


@app.get("/global_health")
async def global_health(_: None = Depends(require_federation_token)) -> Dict[str, Any]:
    clusters = REGISTRY.list_clusters()
    healthy = sum(1 for c in clusters if c.health_state == ClusterHealth.healthy)
    degraded = sum(1 for c in clusters if c.health_state == ClusterHealth.degraded)
    offline = sum(1 for c in clusters if c.health_state == ClusterHealth.offline)
    jobs = REGISTRY.list_global_jobs()
    active_global = (
        GlobalJobStatus.queued,
        GlobalJobStatus.routed,
        GlobalJobStatus.running,
        GlobalJobStatus.migrated,
    )
    q = sum(1 for j in jobs if j.status in active_global)
    return {
        "total_clusters": len(clusters),
        "healthy_clusters": healthy,
        "degraded_clusters": degraded,
        "offline_clusters": offline,
        "global_queue_depth": q,
        "ts": now_ts(),
    }


@app.post("/submit_global_job")
async def submit_global_job(
    body: SubmitGlobalJobBody,
    _: None = Depends(require_federation_token),
) -> Dict[str, Any]:
    job_id = body.id or str(uuid4())
    gj = GlobalJobRecord(
        id=job_id,
        type=body.type,
        task=body.task,
        required_nodes=body.required_nodes,
        power=body.power,
        gpu_required=body.gpu_required,
        status=GlobalJobStatus.queued,
        originating_cluster_id=body.originating_cluster_id,
        preferred_region=body.preferred_region,
        created_at=now_ts(),
        updated_at=now_ts(),
        execution_logs=[f"{now_ts()}: accepted by federation"],
    )
    REGISTRY.save_global_job(gj)
    decision = decide_route(REGISTRY, gj)
    if not decision.target_cluster_id:
        return {"status": "queued", "job_id": job_id, "reason": "no_healthy_cluster", "ranked": decision.ranked}

    routed = gj.model_copy(
        update={
            "assigned_cluster_id": decision.target_cluster_id,
            "status": GlobalJobStatus.routed,
            "execution_logs": gj.execution_logs + [f"{now_ts()}: routed to {decision.target_cluster_id}"],
        }
    )
    REGISTRY.save_global_job(routed)
    cluster = REGISTRY.get_cluster(decision.target_cluster_id)
    if not cluster:
        raise HTTPException(status_code=500, detail="cluster disappeared during route")
    try:
        await push_inbound(cluster, routed)
        REGISTRY.increment_metric("cross_cluster_transfers_total")
    except Exception as exc:
        logger.exception("push failed")
        fail = routed.model_copy(
            update={
                "status": GlobalJobStatus.queued,
                "assigned_cluster_id": None,
                "execution_logs": routed.execution_logs + [f"{now_ts()}: route push failed: {exc}"],
            }
        )
        REGISTRY.save_global_job(fail)
        raise HTTPException(status_code=502, detail=f"cluster_unreachable: {exc}") from exc

    return {
        "status": "routed",
        "job_id": job_id,
        "target_cluster_id": decision.target_cluster_id,
        "score": decision.score,
        "ranked": decision.ranked,
        "decision_ms": decision.decision_ms,
    }


@app.post("/heartbeat")
async def federation_cluster_heartbeat(
    request: Request,
    _: None = Depends(require_federation_token),
) -> Dict[str, Any]:
    raw = await request.json()
    sig = request.headers.get("x-federation-signature") or request.headers.get("X-Federation-Signature")
    ts_hdr = request.headers.get("x-federation-timestamp") or request.headers.get("X-Federation-Timestamp")
    if not verify_signed_cluster_request(raw, sig, ts_hdr):
        raise HTTPException(status_code=403, detail="invalid federation body signature or timestamp")
    payload = FederationHeartbeatPayload.model_validate(raw)
    try:
        rec = await asyncio.to_thread(apply_heartbeat, REGISTRY, payload)
    except KeyError:
        raise HTTPException(status_code=404, detail="cluster not registered") from None
    return {"status": "ok", "cluster_id": rec.cluster_id, "last_heartbeat": rec.last_heartbeat}


@app.post("/global_job_complete")
async def global_job_complete(
    request: Request,
    _: None = Depends(require_federation_token),
) -> Dict[str, Any]:
    raw = await request.json()
    sig = request.headers.get("x-federation-signature") or request.headers.get("X-Federation-Signature")
    ts_hdr = request.headers.get("x-federation-timestamp") or request.headers.get("X-Federation-Timestamp")
    if not verify_signed_cluster_request(raw, sig, ts_hdr):
        raise HTTPException(status_code=403, detail="invalid federation body signature or timestamp")
    body = GlobalJobCompletePayload.model_validate(raw)
    job = REGISTRY.get_global_job(body.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="unknown global job")
    done = job.model_copy(
        update={
            "status": GlobalJobStatus.completed if body.ok else GlobalJobStatus.failed,
            "result": body.result or {},
            "execution_logs": job.execution_logs + [f"{now_ts()}: completed by {body.cluster_id} ok={body.ok}"],
        }
    )
    REGISTRY.save_global_job(done)
    return {"status": "recorded"}


@app.get("/federation_metrics")
async def federation_metrics(_: None = Depends(require_federation_token)) -> Dict[str, Any]:
    clusters = REGISTRY.list_clusters()
    jobs = REGISTRY.list_global_jobs()
    active_global = (
        GlobalJobStatus.queued,
        GlobalJobStatus.routed,
        GlobalJobStatus.running,
        GlobalJobStatus.migrated,
    )
    by_status: Dict[str, int] = {}
    for j in jobs:
        k = j.status.value
        by_status[k] = by_status.get(k, 0) + 1
    load_map = {
        c.cluster_id: {
            "region": c.region,
            "current_load": c.current_load,
            "queue_depth": c.queue_depth,
            "health_state": c.health_state.value,
        }
        for c in clusters
    }
    return {
        "total_clusters": len(clusters),
        "active_clusters": sum(1 for c in clusters if c.health_state != ClusterHealth.offline),
        "failed_clusters": sum(1 for c in clusters if c.health_state == ClusterHealth.offline),
        "global_queue_size": sum(1 for j in jobs if j.status in active_global),
        "cross_cluster_transfers": REGISTRY.get_metric("cross_cluster_transfers_total"),
        "failover_events": REGISTRY.get_metric("failover_events_total"),
        "failover_reroutes": REGISTRY.get_metric("failover_reroutes_total"),
        "jobs_total": len(jobs),
        "jobs_by_status": by_status,
        "cluster_load_map": load_map,
        "ts": now_ts(),
    }


@app.get("/cluster_metrics")
async def cluster_metrics(_: None = Depends(require_federation_token)) -> Dict[str, Any]:
    out = []
    for c in REGISTRY.list_clusters():
        out.append(
            {
                "cluster_id": c.cluster_id,
                "region": c.region,
                "health_state": c.health_state.value,
                "current_load": c.current_load,
                "queue_depth": c.queue_depth,
                "node_count": c.node_count,
                "gpu_capacity": c.gpu_capacity,
                "avg_latency_ms": c.avg_latency_ms,
                "last_heartbeat": c.last_heartbeat,
            }
        )
    return {"clusters": out}


@app.get("/routing_metrics")
async def routing_metrics(_: None = Depends(require_federation_token)) -> Dict[str, Any]:
    return {
        "cross_cluster_transfers": REGISTRY.get_metric("cross_cluster_transfers_total"),
        "failover_events": REGISTRY.get_metric("failover_events_total"),
        "failover_reroutes": REGISTRY.get_metric("failover_reroutes_total"),
        "recent_events": REGISTRY.recent_events(30),
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
