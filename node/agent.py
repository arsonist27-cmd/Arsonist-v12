from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict

import requests
from fastapi import FastAPI, Header, HTTPException, Request

# Ensure project root is importable when run as script (python node/agent.py).
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sandbox.docker_runner import run_python_in_docker
from security.hmac_auth import build_auth_headers, verify_headers
from security.job_payload import verify_job_payload
from shared.models import NodeType
from shared.utils import setup_logging

logger = setup_logging("node.agent")
app = FastAPI(title="Arsonist OS v8 Node Agent")

STATE: Dict[str, Any] = {
    "node_id": os.getenv("NODE_ID", f"node-{os.getenv('PORT', '9001')}"),
    "host": os.getenv("HOST", "127.0.0.1"),
    "port": int(os.getenv("PORT", "9001")),
    "node_type": os.getenv("NODE_TYPE", "CPU"),
    "has_gpu": os.getenv("HAS_GPU", "false").lower() == "true",
    "current_load": 0.0,
    "running_jobs": [],
    "avg_latency_ms": 0.0,
    "node_secret": os.getenv("NODE_SECRET", ""),
    "node_jwt": os.getenv("NODE_JWT", ""),
}


def _control_url() -> str:
    return os.getenv("CONTROL_PLANE_URL", "http://127.0.0.1:8000")


def _control_token() -> str:
    return os.getenv("CONTROL_PLANE_TOKEN", "")


def _control_headers(path: str, method: str, payload: Dict[str, Any] | None = None) -> Dict[str, str]:
    headers = {}
    token = _control_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if STATE.get("node_jwt"):
        headers["X-Node-JWT"] = STATE["node_jwt"]
    if STATE["node_secret"]:
        headers.update(
            build_auth_headers(
                node_id=STATE["node_id"],
                node_secret=STATE["node_secret"],
                method=method,
                path=path,
                payload=payload,
            )
        )
    return headers


def _register() -> bool:
    payload = {
        "node_id": STATE["node_id"],
        "host": STATE["host"],
        "port": STATE["port"],
        "node_type": STATE["node_type"],
        "has_gpu": STATE["has_gpu"],
        "node_secret": STATE["node_secret"],
    }
    try:
        resp = requests.post(
            f"{_control_url()}/register_node",
            json=payload,
            headers=_control_headers("/register_node", "POST", payload),
            timeout=3,
        )
        resp.raise_for_status()
        data = resp.json()
        tok = data.get("node_token")
        if tok:
            STATE["node_jwt"] = tok
        logger.info("Registered node=%s with control plane", STATE["node_id"])
        return True
    except requests.RequestException:
        return False


def _registration_loop() -> None:
    while True:
        if _register():
            return
        logger.warning("Registration retry for node=%s", STATE["node_id"])
        time.sleep(2)


def _finish_job(job_id: str, result: Dict[str, Any]) -> None:
    payload = dict(result)
    payload["node_id"] = STATE["node_id"]
    try:
        requests.post(
            f"{_control_url()}/job_result/{job_id}",
            json=payload,
            headers=_control_headers(f"/job_result/{job_id}", "POST", payload),
            timeout=4,
        )
    except requests.RequestException:
        logger.exception("Failed to report result job_id=%s", job_id)
    finally:
        STATE["current_load"] = max(0.0, STATE["current_load"] - 0.25)
        if job_id in STATE["running_jobs"]:
            STATE["running_jobs"].remove(job_id)


def _execute(job: Dict[str, Any]) -> None:
    job_id = job["id"]
    task = job["task"]
    STATE["running_jobs"].append(job_id)
    STATE["current_load"] = min(1.0, STATE["current_load"] + 0.25)
    if job["type"] in ("code", "ai"):
        result = run_python_in_docker(task, timeout=35)
    elif job["type"] in ("shell", "system"):
        # Keep shell/system sandboxed via python subprocess in container.
        wrapped = f"import subprocess; print(subprocess.getoutput({task!r}))"
        result = run_python_in_docker(wrapped, timeout=25)
    else:
        result = {"ok": False, "exit_code": 1, "stdout": "", "stderr": "Unknown job type"}
    _finish_job(job_id, result)


def _heartbeat_loop() -> None:
    while True:
        payload = {
            "node_id": STATE["node_id"],
            "current_load": STATE["current_load"],
            "running_jobs": STATE["running_jobs"],
            "queue_size": len(STATE["running_jobs"]),
            "avg_latency_ms": STATE["avg_latency_ms"],
        }
        try:
            requests.post(
                f"{_control_url()}/heartbeat",
                json=payload,
                headers=_control_headers("/heartbeat", "POST", payload),
                timeout=3,
            )
        except requests.RequestException:
            pass
        time.sleep(5)


@app.on_event("startup")
def startup() -> None:
    threading.Thread(target=_registration_loop, daemon=True, name="register-loop").start()
    threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat-loop").start()


@app.get("/ping")
def ping() -> Dict[str, Any]:
    return {
        "node_id": STATE["node_id"],
        "host": STATE["host"],
        "port": STATE["port"],
        "node_type": STATE["node_type"],
        "has_gpu": STATE["has_gpu"],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "current_load": STATE["current_load"],
        "running_jobs": STATE["running_jobs"],
    }


@app.post("/run_job")
def run_job(
    payload: Dict[str, Any],
    request: Request,
    node_id: str | None = Header(default=None, alias="X-Node-Id"),
    auth_ts: str | None = Header(default=None, alias="X-Auth-Timestamp"),
    auth_sig: str | None = Header(default=None, alias="X-Auth-Signature"),
) -> Dict[str, Any]:
    if node_id != STATE["node_id"]:
        raise HTTPException(status_code=403, detail="invalid node identity")
    if STATE["node_secret"]:
        ok = verify_headers(
            node_secret=STATE["node_secret"],
            node_id=STATE["node_id"],
            method=request.method,
            path=request.url.path,
            payload=payload,
            ts=auth_ts,
            signature=auth_sig,
        )
        if not ok:
            raise HTTPException(status_code=403, detail="invalid signature")
    sig = payload.pop("arsonist_payload_sig", None)
    if not verify_job_payload(dict(payload), sig):
        raise HTTPException(status_code=403, detail="invalid job payload signature")
    thread = threading.Thread(target=_execute, args=(payload,), daemon=True)
    thread.start()
    return {"status": "accepted", "node_id": STATE["node_id"], "job_id": payload["id"]}


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--node-type", type=str, default="CPU")
    parser.add_argument("--gpu", action="store_true")
    args = parser.parse_args()
    STATE["port"] = args.port
    STATE["node_type"] = NodeType(args.node_type.upper()).value
    STATE["has_gpu"] = args.gpu
    STATE["node_id"] = f"node-{args.port}"
    STATE["node_secret"] = os.getenv("NODE_SECRET", f"secret-{STATE['node_id']}")
    # Keep HOST from environment for container networking; fallback to localhost.
    STATE["host"] = os.getenv("HOST", "127.0.0.1")
    uvicorn.run(app, host="0.0.0.0", port=args.port)
