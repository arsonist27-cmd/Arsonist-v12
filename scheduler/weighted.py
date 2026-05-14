from __future__ import annotations

from typing import List, Tuple

from shared.models import JobPower, JobRequest, NodeState


def _latency_prediction_ms(node: NodeState) -> float:
    """Blend measured RTT with load EMA as a simple latency predictor."""
    load_hint = node.historical_load_ema * 1200.0
    return max(node.avg_latency_ms, load_hint, 1.0)


def _history_reliability(node: NodeState) -> float:
    total = node.jobs_completed_ok + node.jobs_failed
    if total <= 0:
        return 0.5
    return node.jobs_completed_ok / total


def node_score(node: NodeState, job: JobRequest) -> float:
    load_component = max(0.0, 1.0 - node.current_load) * 0.32
    gpu_component = (0.28 if (job.gpu_required and node.has_gpu) else 0.12 if node.has_gpu else 0.0)
    pred = _latency_prediction_ms(node)
    latency_component = max(0.0, 1.0 - min(pred, 2500.0) / 2500.0) * 0.18
    queue_component = max(0.0, 1.0 - min(node.queue_size, 50) / 50.0) * 0.10
    history_component = (_history_reliability(node) - 0.5) * 0.30

    if job.power == JobPower.high and not node.has_gpu:
        gpu_component -= 0.25
    if job.gpu_required and not node.has_gpu:
        gpu_component -= 0.55

    return round(load_component + gpu_component + latency_component + queue_component + history_component, 6)


def rank_nodes(job: JobRequest, nodes: List[NodeState]) -> List[Tuple[NodeState, float]]:
    eligible = [n for n in nodes if n.healthy]
    if job.gpu_required:
        eligible = [n for n in eligible if n.has_gpu]
    ranked = [(n, node_score(n, job)) for n in eligible]
    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked


def select_best_nodes(job: JobRequest, nodes: List[NodeState]) -> List[NodeState]:
    ranked = rank_nodes(job, nodes)
    return [node for node, _ in ranked[: job.required_nodes]]
