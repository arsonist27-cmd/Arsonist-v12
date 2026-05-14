from __future__ import annotations

from typing import List

import requests

from control_plane.memory import ClusterMemory
from security.hmac_auth import build_auth_headers
from security.job_payload import sign_job_payload
from shared.models import JobRecord, NodeRegistration, NodeState
from shared.utils import now_ts

REQ_TIMEOUT = 3


def register_node(memory: ClusterMemory, payload: NodeRegistration) -> NodeState:
    node = NodeState(**payload.model_dump(), last_seen=now_ts())
    memory.add_node(node)
    if payload.node_secret:
        memory.save_node_secret(payload.node_id, payload.node_secret)
    return node


def list_nodes(memory: ClusterMemory) -> List[NodeState]:
    return list(memory.nodes.values())


def dispatch_job(memory: ClusterMemory, node: NodeState, job: JobRecord) -> dict:
    url = f"http://{node.host}:{node.port}/run_job"
    body = job.model_dump()
    sig = sign_job_payload(body)
    if sig:
        body["arsonist_payload_sig"] = sig
    node_secret = memory.get_node_secret(node.node_id)
    headers = {}
    if node_secret:
        headers.update(build_auth_headers(node.node_id, node_secret, "POST", "/run_job", body))
    try:
        t0 = now_ts()
        resp = requests.post(url, json=body, headers=headers, timeout=REQ_TIMEOUT)
        resp.raise_for_status()
        node.current_load = min(1.0, node.current_load + 0.2)
        node.running_jobs.append(job.id)
        node.last_seen = now_ts()
        latency_ms = (now_ts() - t0) * 1000
        node.avg_latency_ms = latency_ms if node.avg_latency_ms == 0 else ((node.avg_latency_ms * 0.7) + (latency_ms * 0.3))
        node.queue_size = len(node.running_jobs)
        memory.add_node(node)
        memory.emit("info", "job_dispatched", {"job_id": job.id, "node_id": node.node_id})
        return resp.json()
    except requests.RequestException as exc:
        memory.emit(
            "error",
            "job_dispatch_failed",
            {"job_id": job.id, "node_id": node.node_id, "error": str(exc)},
        )
        raise
