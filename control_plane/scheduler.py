from __future__ import annotations

from typing import List, Tuple

from scheduler.weighted import rank_nodes, select_best_nodes
from shared.models import JobRequest, NodeState


def select_nodes(job: JobRequest, candidates: List[NodeState]) -> List[NodeState]:
    return select_best_nodes(job, candidates)


def ranked_nodes(job: JobRequest, candidates: List[NodeState]) -> List[Tuple[NodeState, float]]:
    return rank_nodes(job, candidates)
