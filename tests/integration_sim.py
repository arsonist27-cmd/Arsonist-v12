from __future__ import annotations

import os
import signal
import subprocess
import time
from typing import List

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOKEN = "sim-token"


def _auth_headers() -> dict:
    return {"Authorization": f"Bearer {TOKEN}"}


def _spawn(cmd: List[str], env: dict | None = None) -> subprocess.Popen:
    return subprocess.Popen(cmd, cwd=ROOT, env=env or os.environ.copy())


def _wait(url: str, tries: int = 30) -> None:
    for _ in range(tries):
        try:
            requests.get(url, timeout=1.5).raise_for_status()
            return
        except requests.RequestException:
            time.sleep(1)
    raise RuntimeError(f"Service not ready: {url}")


def main() -> None:
    procs: List[subprocess.Popen] = []
    try:
        control_env = os.environ.copy()
        control_env["ARSONIST_API_TOKEN"] = TOKEN
        control_env["ARSONIST_DB_PATH"] = os.path.join(ROOT, "data", "sim.db")
        procs.append(
            _spawn(
                ["python", "-m", "uvicorn", "control_plane.app:app", "--host", "0.0.0.0", "--port", "8000"],
                env=control_env,
            )
        )
        _wait("http://127.0.0.1:8000/health")

        node_specs = [
            ("9001", "GPU", "true"),
            ("9002", "CPU", "false"),
            ("9003", "EDGE", "false"),
        ]
        for port, ntype, gpu in node_specs:
            env = os.environ.copy()
            env["PORT"] = port
            env["NODE_TYPE"] = ntype
            env["HAS_GPU"] = gpu
            env["CONTROL_PLANE_URL"] = "http://127.0.0.1:8000"
            env["CONTROL_PLANE_TOKEN"] = TOKEN
            env["NODE_SECRET"] = f"sim-secret-{port}"
            procs.append(_spawn(["python", "node/agent.py", "--port", port, "--node-type", ntype, *(["--gpu"] if gpu == "true" else [])], env=env))

        time.sleep(4)
        print("Nodes:", requests.get("http://127.0.0.1:8000/nodes", headers=_auth_headers(), timeout=3).json())

        print("Submitting AI job...")
        job1 = requests.post(
            "http://127.0.0.1:8000/submit_job",
            headers=_auth_headers(),
            json={
                "type": "ai",
                "task": "print('gpu-heavy simulation complete')",
                "required_nodes": 1,
                "power": "high",
                "gpu_required": True,
            },
            timeout=4,
        ).json()
        print("Job submit response:", job1)
        time.sleep(6)
        print("Jobs after assignment:", requests.get("http://127.0.0.1:8000/jobs", headers=_auth_headers(), timeout=3).json())

        print("Simulating node failure (kill GPU node on 9001)...")
        procs[1].send_signal(signal.SIGTERM)
        time.sleep(7)
        requests.post("http://127.0.0.1:8000/reschedule", headers=_auth_headers(), timeout=3)
        print("Nodes after failure:", requests.get("http://127.0.0.1:8000/nodes", headers=_auth_headers(), timeout=3).json())
        print("Jobs after reassignment attempt:", requests.get("http://127.0.0.1:8000/jobs", headers=_auth_headers(), timeout=3).json())

        print("Trigger scaling with queue backlog...")
        for i in range(6):
            requests.post(
                "http://127.0.0.1:8000/submit_job",
                headers=_auth_headers(),
                json={
                    "type": "code",
                    "task": f"print('queued-{i}')",
                    "required_nodes": 1,
                    "power": "medium",
                    "gpu_required": False,
                },
                timeout=4,
            )
        time.sleep(10)
        print("Nodes after autoscaling:", requests.get("http://127.0.0.1:8000/nodes", headers=_auth_headers(), timeout=3).json())
        print("Final jobs:", requests.get("http://127.0.0.1:8000/jobs", headers=_auth_headers(), timeout=3).json())
        print("Integration simulation completed.")
    finally:
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
        time.sleep(1)


if __name__ == "__main__":
    main()
