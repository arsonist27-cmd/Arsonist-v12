from __future__ import annotations

import threading
import time
from collections import Counter, deque
from typing import Callable, Deque

from control_plane.memory import ClusterMemory
from shared.models import NodeType
from shared.utils import setup_logging

logger = setup_logging("control.autoscaler")

_QUEUE_OBS: Deque[int] = deque(maxlen=24)
_EMA_QUEUE: float = 0.0
_EMA_LOAD: float = 0.0
_EMA_GPU: float = 0.0
_ALPHA = 0.35


def avg_cluster_load(memory: ClusterMemory) -> float:
    if not memory.nodes:
        return 0.0
    return sum(n.current_load for n in memory.nodes.values()) / len(memory.nodes)


def gpu_saturation(memory: ClusterMemory) -> float:
    gpu_nodes = [n for n in memory.nodes.values() if n.node_type == NodeType.gpu]
    if not gpu_nodes:
        return 0.0
    return sum(n.current_load for n in gpu_nodes) / len(gpu_nodes)


def evaluate_scaling(
    memory: ClusterMemory,
    queue_threshold: int = 4,
    load_threshold: float = 0.75,
    gpu_threshold: float = 0.80,
    horizon_min: int = 3,
) -> bool:
    """
    Predictive scaling: exponential smoothing on queue depth + cluster load + GPU saturation,
    plus short-horizon queue growth (jobs/min implied by sample cadence).
    """
    global _EMA_QUEUE, _EMA_LOAD, _EMA_GPU
    q = len(memory.queue_snapshot())
    load = avg_cluster_load(memory)
    gpu = gpu_saturation(memory)
    _QUEUE_OBS.append(q)
    _EMA_QUEUE = _ALPHA * q + (1.0 - _ALPHA) * _EMA_QUEUE
    _EMA_LOAD = _ALPHA * load + (1.0 - _ALPHA) * _EMA_LOAD
    _EMA_GPU = _ALPHA * gpu + (1.0 - _ALPHA) * _EMA_GPU

    if _EMA_QUEUE >= queue_threshold:
        return True
    if _EMA_LOAD > load_threshold:
        return True
    if _EMA_GPU > gpu_threshold:
        return True

    if len(_QUEUE_OBS) >= horizon_min:
        dq = _QUEUE_OBS[-1] - _QUEUE_OBS[0]
        if dq >= 2 and q >= 1:
            return True
        if len(_QUEUE_OBS) >= horizon_min + 2:
            velocity = (_QUEUE_OBS[-1] - _QUEUE_OBS[-3]) / 2.0
            if velocity > 0.8 and _EMA_QUEUE > queue_threshold * 0.55:
                return True
    return False


def desired_node_type(memory: ClusterMemory) -> NodeType:
    type_counts = Counter(job.power.value for job in memory.jobs.values() if job.status == "queued")
    if type_counts.get("high", 0) > 0:
        return NodeType.gpu
    if type_counts.get("medium", 0) > 0:
        return NodeType.cpu
    return NodeType.edge


def start_autoscaler_loop(
    memory: ClusterMemory,
    installer_callback: Callable[[NodeType], None],
    interval_sec: float = 8.0,
    leader_ok: Callable[[], bool] | None = None,
) -> threading.Thread:
    def _loop() -> None:
        while True:
            if leader_ok is not None and not leader_ok():
                time.sleep(interval_sec)
                continue
            if evaluate_scaling(memory):
                ntype = desired_node_type(memory)
                memory.emit("warning", "scaling_triggered", {"node_type": ntype.value})
                try:
                    installer_callback(ntype)
                except Exception as exc:
                    logger.exception("Scaling callback failed: %s", exc)
            time.sleep(interval_sec)

    t = threading.Thread(target=_loop, daemon=True, name="autoscaler-loop")
    t.start()
    return t
