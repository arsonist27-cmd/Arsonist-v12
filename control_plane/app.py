from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict

import requests
from fastapi import Depends, FastAPI, Header, HTTPException, Request

from cluster_client.cluster_agent import maybe_start_cluster_agent
from control_plane import federation_api, mesh_bootstrap, mesh_routes, v11_api
from control_plane.autoscaler import start_autoscaler_loop
from control_plane.coordinator import Coordinator, build_coordinator
from control_plane.discovery import start_discovery_task
from control_plane.federation_callbacks import maybe_report_global_completion
from control_plane.health import start_health_loop
from control_plane.memory import ClusterMemory
from control_plane.nodes import dispatch_job, list_nodes, register_node
from control_plane.scheduler import ranked_nodes, select_nodes
from security.hmac_auth import verify_headers
from security.jwt_auth import issue_node_token, verify_node_token
from storage.job_queue import MAX_JOB_RETRIES, build_job_record, log_job, mark_finished, mark_running, requeue_or_fail
from shared.models import JobRequest, NodeRegistration, NodeType
from shared.utils import now_ts, setup_logging

logger = setup_logging("control.app")
app = FastAPI(title="Arsonist OS v11 Control Plane (v8/v9/v10 compatible)")
COORDINATOR: Coordinator = build_coordinator()
memory = ClusterMemory(
    db_path=os.getenv("ARSONIST_DB_PATH", "control_plane/arsonist.db"),
    database_url=os.getenv("ARSONIST_DATABASE_URL", ""),
)
API_TOKEN = os.getenv("ARSONIST_API_TOKEN", "")
PROVISIONER_URL = os.getenv("ARSONIST_PROVISIONER_URL", "http://provisioner:8100/provision")
PROVISIONER_TOKEN = os.getenv("ARSONIST_PROVISIONER_TOKEN", "")
JWT_SECRET = os.getenv("ARSONIST_JWT_SECRET", "")


def require_token(authorization: str | None = Header(default=None)) -> None:
    if not API_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")


def require_node_auth(
    request: Request,
    payload: Dict[str, Any],
    node_id_header: str | None,
    ts_header: str | None,
    sig_header: str | None,
) -> str:
    node_jwt = request.headers.get("x-node-jwt") or request.headers.get("X-Node-JWT")
    if JWT_SECRET and node_jwt:
        claim_id = verify_node_token(node_jwt.strip())
        if claim_id:
            body_id = payload.get("node_id")
            if body_id and body_id != claim_id:
                raise HTTPException(status_code=403, detail="node id mismatch vs jwt")
            if node_id_header and node_id_header != claim_id:
                raise HTTPException(status_code=403, detail="node header mismatch vs jwt")
            return claim_id

    node_id = node_id_header or payload.get("node_id")
    if not node_id:
        raise HTTPException(status_code=401, detail="missing node identity")
    secret = memory.get_node_secret(node_id)
    if not secret:
        raise HTTPException(status_code=403, detail="unknown node")
    ok = verify_headers(
        node_secret=secret,
        node_id=node_id,
        method=request.method,
        path=request.url.path,
        payload=payload,
        ts=ts_header,
        signature=sig_header,
    )
    if not ok:
        raise HTTPException(status_code=403, detail="invalid node signature")
    return node_id


def schedule_once() -> None:
    if not COORDINATOR.is_leader():
        return
    job = memory.pop_next_job()
    if not job:
        return
    if job.status in ("completed", "failed"):
        return
    if job.attempts >= MAX_JOB_RETRIES:
        job.status = "failed"
        log_job(memory, job, "retry limit reached before dispatch")
        return
    candidates = select_nodes(job, list(memory.nodes.values()))
    if len(candidates) < job.required_nodes:
        requeue_or_fail(memory, job, "insufficient_nodes")
        memory.emit("warning", "insufficient_nodes", {"job_id": job.id})
        return
    assigned = []
    mark_running(memory, job)
    for node in candidates:
        try:
            dispatch_job(memory, node, job)
            assigned.append(node.node_id)
        except Exception:
            log_job(memory, job, f"dispatch failure node={node.node_id}")
            continue
    if len(assigned) >= job.required_nodes:
        job.assigned_nodes = assigned
        memory.save_job(job)
        memory.persist_queue()
    else:
        requeue_or_fail(memory, job, "partial_dispatch_failure")


federation_api.wire_federation_routes(memory, schedule_once)
app.include_router(federation_api.router)
mesh_routes.wire_mesh_routes(memory, schedule_once)
app.include_router(mesh_routes.router)
v11_api.attach_v11(app, require_token)


def _provision_node(node_type: NodeType) -> None:
    payload = {"node_type": node_type.value}
    headers = {"Authorization": f"Bearer {PROVISIONER_TOKEN}"} if PROVISIONER_TOKEN else {}
    try:
        resp = requests.post(PROVISIONER_URL, json=payload, headers=headers, timeout=4)
        resp.raise_for_status()
        data = resp.json()
        reg = NodeRegistration(
            node_id=data["node_id"],
            host=data["host"],
            port=int(data["port"]),
            node_type=NodeType(data["node_type"]),
            has_gpu=bool(data.get("has_gpu", False)),
        )
        register_node(memory, reg)
        memory.emit("info", "node_spinup_requested", {"node_id": reg.node_id, "source": "provisioner"})
    except requests.RequestException:
        count = len(memory.nodes) + 1
        node_id = f"autoscaled-{count}"
        reg = NodeRegistration(
            node_id=node_id,
            host="127.0.0.1",
            port=9100 + count,
            node_type=node_type,
            has_gpu=node_type == NodeType.gpu,
        )
        register_node(memory, reg)
        memory.emit("warning", "node_spinup_fallback", {"node_id": node_id})


def _scheduler_loop() -> None:
    while True:
        try:
            if COORDINATOR.is_leader():
                schedule_once()
        except Exception:
            logger.exception("scheduler tick failed")
        time.sleep(float(os.getenv("ARSONIST_SCHEDULER_TICK_SEC", "0.85")))


@app.on_event("startup")
def startup() -> None:
    COORDINATOR.start()
    start_health_loop(memory, leader_ok=COORDINATOR.is_leader)
    start_autoscaler_loop(memory, _provision_node, leader_ok=COORDINATOR.is_leader)
    start_discovery_task(memory, COORDINATOR.is_leader)
    threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler-loop").start()
    maybe_start_cluster_agent(memory)
    mesh_bootstrap.maybe_start_mesh(memory, schedule_once)


@app.get("/health")
def health() -> Dict[str, Any]:
    q = memory.queue_snapshot()
    return {
        "status": "ok",
        "nodes": len(memory.nodes),
        "queued_jobs": len(q),
        "leader": COORDINATOR.is_leader(),
        "coordinator": os.getenv("ARSONIST_COORDINATOR_MODE", "single"),
    }


@app.get("/nodes")
def nodes(_: None = Depends(require_token)) -> Dict[str, Any]:
    return {"nodes": [n.model_dump() for n in list_nodes(memory)]}


@app.get("/jobs")
def jobs(_: None = Depends(require_token)) -> Dict[str, Any]:
    return {"jobs": [j.model_dump() for j in memory.jobs.values()], "queue": memory.queue_snapshot()}


@app.get("/metrics")
def metrics(_: None = Depends(require_token)) -> Dict[str, Any]:
    queued = sum(1 for j in memory.jobs.values() if j.status == "queued")
    running = sum(1 for j in memory.jobs.values() if j.status == "running")
    failed = sum(1 for j in memory.jobs.values() if j.status == "failed")
    completed = sum(1 for j in memory.jobs.values() if j.status == "completed")
    gpu_nodes = [n for n in memory.nodes.values() if n.has_gpu]
    avg_load = (sum(n.current_load for n in memory.nodes.values()) / len(memory.nodes)) if memory.nodes else 0.0
    gpu_load = (sum(n.current_load for n in gpu_nodes) / len(gpu_nodes)) if gpu_nodes else 0.0
    return {
        "active_nodes": len(memory.nodes),
        "avg_cluster_load": round(avg_load, 4),
        "avg_gpu_load": round(gpu_load, 4),
        "queued_jobs": queued,
        "running_jobs": running,
        "failed_jobs": failed,
        "completed_jobs": completed,
        "leader": COORDINATOR.is_leader(),
    }


@app.get("/cluster/status")
def cluster_status(_: None = Depends(require_token)) -> Dict[str, Any]:
    node_dump = [n.model_dump() for n in memory.nodes.values()]
    job_dump = [j.model_dump() for j in memory.jobs.values()]
    ranking = []
    qsnap = memory.queue_snapshot()
    if qsnap:
        peek_id = qsnap[0]
        peek = memory.jobs.get(peek_id) if peek_id else None
        if peek:
            ranking = [{"node_id": n.node_id, "score": s} for n, s in ranked_nodes(peek, list(memory.nodes.values()))]
    return {
        "nodes": node_dump,
        "jobs": job_dump,
        "queue_depth": len(qsnap),
        "ranked_nodes": ranking,
        "leader": COORDINATOR.is_leader(),
        "ts": now_ts(),
    }


@app.get("/registry/{key}")
def registry_get(key: str, _: None = Depends(require_token)) -> Dict[str, Any]:
    val = memory.registry_get(key)
    if val is None:
        raise HTTPException(status_code=404, detail="unknown key")
    return {"key": key, "value": val}


@app.put("/registry/{key}")
def registry_put(key: str, payload: Dict[str, Any], _: None = Depends(require_token)) -> Dict[str, Any]:
    memory.registry_put(key, payload)
    return {"key": key, "status": "ok"}


@app.post("/register_node")
def register(payload: NodeRegistration, _: None = Depends(require_token)) -> Dict[str, Any]:
    if not payload.node_secret:
        raise HTTPException(status_code=400, detail="node_secret required")
    node = register_node(memory, payload)
    out: Dict[str, Any] = {"status": "registered", "node": node.model_dump()}
    if JWT_SECRET:
        try:
            out["node_token"] = issue_node_token(node.node_id)
        except ValueError:
            pass
    return out


@app.post("/submit_job")
def submit_job(payload: JobRequest, _: None = Depends(require_token)) -> Dict[str, Any]:
    record = build_job_record(payload)
    memory.enqueue_job(record)
    memory.emit("info", "job_submitted", {"job_id": record.id, "type": record.type.value})
    schedule_once()
    return {"status": "accepted", "job": record.model_dump()}


@app.post("/job_result/{job_id}")
def job_result(
    job_id: str,
    payload: Dict[str, Any],
    request: Request,
    node_id: str | None = Header(default=None, alias="X-Node-Id"),
    auth_ts: str | None = Header(default=None, alias="X-Auth-Timestamp"),
    auth_sig: str | None = Header(default=None, alias="X-Auth-Signature"),
    _: None = Depends(require_token),
) -> Dict[str, Any]:
    auth_node_id = require_node_auth(request, payload, node_id, auth_ts, auth_sig)
    job = memory.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    job.result = payload
    ok = bool(payload.get("ok", False))
    mark_finished(memory, job, ok=ok)
    node = memory.nodes.get(auth_node_id)
    if node:
        if ok:
            node.jobs_completed_ok += 1
        else:
            node.jobs_failed += 1
        beta = float(os.getenv("ARSONIST_LOAD_EMA_BETA", "0.25"))
        node.historical_load_ema = beta * node.current_load + (1.0 - beta) * node.historical_load_ema
        memory.add_node(node)
    for nid in job.assigned_nodes:
        if nid in memory.nodes:
            n = memory.nodes[nid]
            n.current_load = max(0.0, n.current_load - 0.2)
            if job_id in n.running_jobs:
                n.running_jobs.remove(job_id)
            memory.add_node(n)
    memory.emit("info", "job_finished", {"job_id": job_id, "status": job.status, "node_id": auth_node_id})
    maybe_report_global_completion(job)
    if job.status == "failed":
        requeue_or_fail(memory, job, "node_reported_failure")
    schedule_once()
    return {"status": "recorded"}


@app.post("/reschedule")
def reschedule(_: None = Depends(require_token)) -> Dict[str, Any]:
    schedule_once()
    return {"status": "ok"}


@app.post("/heartbeat")
def heartbeat(
    payload: Dict[str, Any],
    request: Request,
    node_id: str | None = Header(default=None, alias="X-Node-Id"),
    auth_ts: str | None = Header(default=None, alias="X-Auth-Timestamp"),
    auth_sig: str | None = Header(default=None, alias="X-Auth-Signature"),
    _: None = Depends(require_token),
) -> Dict[str, Any]:
    auth_node_id = require_node_auth(request, payload, node_id, auth_ts, auth_sig)
    node = memory.nodes.get(auth_node_id)
    if not node:
        raise HTTPException(status_code=404, detail="node not registered")
    node.current_load = float(payload.get("current_load", node.current_load))
    node.running_jobs = payload.get("running_jobs", node.running_jobs)
    node.queue_size = int(payload.get("queue_size", len(node.running_jobs)))
    node.avg_latency_ms = float(payload.get("avg_latency_ms", node.avg_latency_ms))
    node.last_seen = now_ts()
    node.healthy = True
    beta = float(os.getenv("ARSONIST_LOAD_EMA_BETA", "0.25"))
    node.historical_load_ema = beta * node.current_load + (1.0 - beta) * node.historical_load_ema
    memory.add_node(node)
    memory.registry_put(
        f"node:{auth_node_id}",
        {
            "host": node.host,
            "port": node.port,
            "last_seen": node.last_seen,
            "load": node.current_load,
            "healthy": node.healthy,
        },
    )
    return {"status": "ok"}


@app.post("/provision/request")
def provision_request(payload: Dict[str, str], _: None = Depends(require_token)) -> Dict[str, Any]:
    node_type = NodeType(payload.get("node_type", "CPU"))
    _provision_node(node_type)
    return {"status": "requested", "node_type": node_type.value}
