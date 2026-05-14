from __future__ import annotations

import os
import subprocess
import time
from statistics import mean

import requests

CONTROL = os.getenv("CONTROL_URL", "http://127.0.0.1:8000")
TOKEN = os.getenv("ARSONIST_API_TOKEN", "change-me-token")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
TOTAL_JOBS = int(os.getenv("STRESS_JOBS", "20"))


def _submit_job(i: int) -> str:
    payload = {
        "type": "code",
        "task": f"import time; time.sleep(0.4); print('stress-{i}')",
        "required_nodes": 1,
        "power": "medium" if i % 3 else "high",
        "gpu_required": i % 5 == 0,
    }
    resp = requests.post(f"{CONTROL}/submit_job", json=payload, headers=HEADERS, timeout=5)
    resp.raise_for_status()
    return resp.json()["job"]["id"]


def _get_jobs() -> dict:
    r = requests.get(f"{CONTROL}/jobs", headers=HEADERS, timeout=5)
    r.raise_for_status()
    return r.json()


def _kill_node() -> None:
    subprocess.run(["sudo", "docker", "compose", "stop", "node-cpu"], check=False)


def main() -> None:
    start = time.time()
    print(f"Submitting {TOTAL_JOBS} jobs...")
    ids = [_submit_job(i) for i in range(TOTAL_JOBS)]

    time.sleep(3)
    print("Simulating node failure (node-cpu stop)...")
    fail_t = time.time()
    _kill_node()

    recovered_at = None
    while time.time() - start < 180:
        jobs = _get_jobs().get("jobs", [])
        states = {j["id"]: j["status"] for j in jobs if j["id"] in ids}
        if recovered_at is None and any(v in ("queued", "running") for v in states.values()):
            recovered_at = time.time()
        if all(v in ("completed", "failed") for v in states.values()) and len(states) == len(ids):
            break
        time.sleep(2)

    elapsed = time.time() - start
    jobs = _get_jobs().get("jobs", [])
    tracked = [j for j in jobs if j["id"] in ids]
    completed = [j for j in tracked if j["status"] == "completed"]
    failed = [j for j in tracked if j["status"] == "failed"]
    attempts = [j.get("attempts", 0) for j in tracked]
    recovery_time = (recovered_at - fail_t) if recovered_at else -1

    print("\n=== Cluster Stability Report ===")
    print(f"Total jobs: {len(tracked)}")
    print(f"Completed: {len(completed)}")
    print(f"Failed: {len(failed)}")
    print(f"Average attempts: {mean(attempts) if attempts else 0:.2f}")
    print(f"Recovery detection time: {recovery_time:.2f}s")
    print(f"Total elapsed: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
