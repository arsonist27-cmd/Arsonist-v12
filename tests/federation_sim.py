#!/usr/bin/env python3
"""
Multi-cluster federation simulation (Arsonist OS v9).

Starts a federation controller plus three isolated control-plane processes,
registers clusters via the cluster agent, submits global jobs, optionally
simulates cluster failure and observes rerouting metrics.

Usage:
  python tests/federation_sim.py

Environment:
  SIM_KILL=1     kill one cluster mid-run and wait for failover sweep
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

FED_TOKEN = "sim-fed-token"
FED_SECRET = "sim-fed-hmac-secret"
TOK_A = "tok-cluster-a"
TOK_B = "tok-cluster-b"
TOK_C = "tok-cluster-c"


def _spawn(cmd: List[str], env: Dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(cmd, cwd=ROOT, env=env)


def _wait(url: str, tries: int = 40) -> None:
    for _ in range(tries):
        try:
            requests.get(url, timeout=1.5).raise_for_status()
            return
        except requests.RequestException:
            time.sleep(0.5)
    raise RuntimeError(f"timeout waiting for {url}")


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="arsonist-fed-sim-")
    fed_db = os.path.join(tmp, "fed.db")
    db_a = os.path.join(tmp, "a.db")
    db_b = os.path.join(tmp, "b.db")
    db_c = os.path.join(tmp, "c.db")

    env_base = os.environ.copy()
    env_base["PYTHONPATH"] = ROOT
    env_base["ARSONIST_FEDERATION_SECRET"] = FED_SECRET

    procs: List[subprocess.Popen] = []

    fed_env = env_base.copy()
    fed_env["FEDERATION_DB_PATH"] = fed_db
    fed_env["FEDERATION_API_TOKEN"] = FED_TOKEN
    fed_env["ARSONIST_FEDERATION_TOKEN"] = FED_TOKEN
    fed_env["ARSONIST_FEDERATION_SECRET"] = FED_SECRET
    fed_env["FEDERATION_HEARTBEAT_SWEEP_SEC"] = "4"
    fed_env["FEDERATION_HEARTBEAT_TIMEOUT_SEC"] = "12"

    try:
        procs.append(
            _spawn(
                [sys.executable, "-m", "uvicorn", "federation.controller:app", "--host", "127.0.0.1", "--port", "18500"],
                fed_env,
            )
        )
        _wait("http://127.0.0.1:18500/health")

        specs = [
            ("18001", "cluster-a", TOK_A, db_a),
            ("18002", "cluster-b", TOK_B, db_b),
            ("18003", "cluster-c", TOK_C, db_c),
        ]
        for port, cid, tok, dbp in specs:
            e = env_base.copy()
            e["ARSONIST_DB_PATH"] = dbp
            e["ARSONIST_API_TOKEN"] = tok
            e["ARSONIST_CLUSTER_ID"] = cid
            e["ARSONIST_CLUSTER_REGION"] = f"region-{cid[-1]}"
            e["ARSONIST_FEDERATION_URL"] = "http://127.0.0.1:18500"
            e["ARSONIST_FEDERATION_TOKEN"] = FED_TOKEN
            e["ARSONIST_CONTROL_PLANE_PUBLIC_URL"] = f"http://127.0.0.1:{port}"
            procs.append(
                _spawn(
                    [sys.executable, "-m", "uvicorn", "control_plane.app:app", "--host", "127.0.0.1", "--port", port],
                    e,
                )
            )

        for port, _, tok, _ in specs:
            _wait(f"http://127.0.0.1:{port}/health")

        print("Waiting for cluster agents to register with federation...")
        time.sleep(8)

        clusters = requests.get(
            "http://127.0.0.1:18500/clusters",
            headers={"Authorization": f"Bearer {FED_TOKEN}"},
            timeout=5,
        ).json()
        print("Federation clusters:", clusters)

        gh = requests.get(
            "http://127.0.0.1:18500/global_health",
            headers={"Authorization": f"Bearer {FED_TOKEN}"},
            timeout=5,
        ).json()
        print("Global health:", gh)

        job_resp = requests.post(
            "http://127.0.0.1:18500/submit_global_job",
            headers={"Authorization": f"Bearer {FED_TOKEN}"},
            json={
                "type": "code",
                "task": "print('federation-sim')",
                "required_nodes": 1,
                "power": "low",
                "gpu_required": False,
            },
            timeout=10,
        )
        print("submit_global_job:", job_resp.status_code, job_resp.text[:500])

        routed_to = job_resp.json().get("target_cluster_id") if job_resp.ok else None
        print("Routed to:", routed_to)

        for port, cid, tok, _ in specs:
            jobs = requests.get(
                f"http://127.0.0.1:{port}/jobs",
                headers={"Authorization": f"Bearer {tok}"},
                timeout=5,
            ).json()
            q = jobs.get("queue") or []
            print(f"Cluster {cid} queue depth={len(q)}")

        fm = requests.get(
            "http://127.0.0.1:18500/federation_metrics",
            headers={"Authorization": f"Bearer {FED_TOKEN}"},
            timeout=5,
        ).json()
        print("Federation metrics:", fm)
        print(
            "Recovery / routing snapshot: jobs_by_status=%s failover_events=%s failover_reroutes=%s transfers=%s"
            % (
                fm.get("jobs_by_status"),
                fm.get("failover_events"),
                fm.get("failover_reroutes"),
                fm.get("cross_cluster_transfers"),
            )
        )

        if os.getenv("SIM_KILL") == "1" and len(procs) > 3:
            victim_idx = 3  # first cluster CP after federation
            print(f"Stopping cluster process idx={victim_idx} (simulated failure)...")
            try:
                procs[victim_idx].send_signal(signal.SIGTERM)
                procs[victim_idx].wait(timeout=5)
            except Exception:
                pass
            print("Waiting for heartbeat sweep + failover...")
            time.sleep(18)
            fm2 = requests.get(
                "http://127.0.0.1:18500/federation_metrics",
                headers={"Authorization": f"Bearer {FED_TOKEN}"},
                timeout=5,
            ).json()
            rm = requests.get(
                "http://127.0.0.1:18500/routing_metrics",
                headers={"Authorization": f"Bearer {FED_TOKEN}"},
                timeout=5,
            ).json()
            print("Metrics after failure:", fm2)
            print("Routing metrics:", rm)
            print(
                "Recovery metrics: failover_events=%s failover_reroutes=%s jobs_by_status=%s"
                % (fm2.get("failover_events"), fm2.get("failover_reroutes"), fm2.get("jobs_by_status"))
            )

        print("Simulation complete. tmp dir:", tmp)
    finally:
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
        time.sleep(0.5)


if __name__ == "__main__":
    main()
