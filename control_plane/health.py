from __future__ import annotations

import threading
import time
from typing import Callable, List

from control_plane.memory import ClusterMemory
from storage.job_queue import requeue_or_fail
from shared.utils import now_ts, setup_logging

logger = setup_logging("control.health")


def _requeue_jobs(memory: ClusterMemory, job_ids: List[str]) -> None:
    for job_id in job_ids:
        job = memory.jobs.get(job_id)
        if not job:
            continue
        requeue_or_fail(memory, job, "node_dead_recovery")
        memory.emit("warning", "job_requeued", {"job_id": job_id})


def check_nodes_once(memory: ClusterMemory, dead_after_sec: float = 15.0) -> None:
    now = now_ts()
    for node_id, node in list(memory.nodes.items()):
        if now - node.last_seen > dead_after_sec:
            node.healthy = False
            memory.emit("error", "node_dead", {"node_id": node_id, "last_seen": node.last_seen})
            failed_jobs = list(node.running_jobs)
            memory.remove_node(node_id)
            _requeue_jobs(memory, failed_jobs)


def start_health_loop(
    memory: ClusterMemory,
    interval_sec: float = 5.0,
    leader_ok: Callable[[], bool] | None = None,
) -> threading.Thread:
    def _loop() -> None:
        while True:
            if leader_ok is not None and not leader_ok():
                time.sleep(interval_sec)
                continue
            check_nodes_once(memory)
            time.sleep(interval_sec)

    t = threading.Thread(target=_loop, daemon=True, name="health-loop")
    t.start()
    logger.info("Health loop started interval=%s", interval_sec)
    return t
