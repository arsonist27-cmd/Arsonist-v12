"""v15 Consensus Mesh.

Provides consensus-assisted coordination across planetary infrastructure
nodes. Supports quorum-based decision making, leader election, and
distributed agreement for critical infrastructure operations.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("coordination.consensus_mesh")


class ConsensusState(str, Enum):
    leader = "leader"
    follower = "follower"
    candidate = "candidate"
    observer = "observer"


class VoteResult(str, Enum):
    pending = "pending"
    accepted = "accepted"
    rejected = "rejected"
    timeout = "timeout"


class MeshNode(BaseModel):
    node_id: str
    region: str = ""
    continent: str = ""
    state: ConsensusState = ConsensusState.follower
    term: int = 0
    last_heartbeat: float = 0.0
    votes_received: int = 0
    is_healthy: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ConsensusRound(BaseModel):
    round_id: str
    topic: str = ""
    term: int = 0
    proposer: str = ""
    votes_for: int = 0
    votes_against: int = 0
    quorum: int = 1
    result: VoteResult = VoteResult.pending
    proposed_at: float = 0.0
    decided_at: float = 0.0
    payload: Dict[str, Any] = Field(default_factory=dict)


class ConsensusMesh:
    """Distributed consensus mesh for planetary-scale coordination.

    Manages node membership, leader election, and quorum-based voting
    for critical infrastructure decisions.
    """

    def __init__(self, quorum_fraction: float = 0.5, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._quorum_fraction = quorum_fraction
        self._max_history = max_history
        self._nodes: Dict[str, MeshNode] = {}
        self._rounds: Dict[str, ConsensusRound] = {}
        self._completed_rounds: List[ConsensusRound] = []
        self._current_term = 0
        self._leader_id: Optional[str] = None
        self._events: List[Dict[str, Any]] = []

    def register_node(self, node: MeshNode) -> None:
        with self._lock:
            node.last_heartbeat = now_ts()
            node.term = self._current_term
            self._nodes[node.node_id] = node
            self._events.append({
                "type": "node_joined",
                "node_id": node.node_id,
                "region": node.region,
                "ts": now_ts(),
            })

    def remove_node(self, node_id: str) -> bool:
        with self._lock:
            if node_id not in self._nodes:
                return False
            del self._nodes[node_id]
            if self._leader_id == node_id:
                self._leader_id = None
            return True

    def heartbeat(self, node_id: str) -> bool:
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return False
            node.last_heartbeat = now_ts()
            node.is_healthy = True
            return True

    def check_health(self, timeout_s: float = 30.0) -> List[str]:
        ts = now_ts()
        unhealthy = []
        with self._lock:
            for node in self._nodes.values():
                if ts - node.last_heartbeat > timeout_s:
                    node.is_healthy = False
                    unhealthy.append(node.node_id)
        return unhealthy

    def elect_leader(self) -> Optional[str]:
        with self._lock:
            self._current_term += 1
            healthy = [n for n in self._nodes.values() if n.is_healthy]
            if not healthy:
                self._leader_id = None
                return None

            best = max(healthy, key=lambda n: (n.last_heartbeat, n.node_id))
            for n in self._nodes.values():
                n.state = ConsensusState.follower
                n.term = self._current_term
            best.state = ConsensusState.leader
            self._leader_id = best.node_id

            self._events.append({
                "type": "leader_elected",
                "leader": best.node_id,
                "term": self._current_term,
                "ts": now_ts(),
            })
            if len(self._events) > self._max_history:
                self._events = self._events[-self._max_history:]
            return best.node_id

    def propose_round(self, round_obj: ConsensusRound) -> ConsensusRound:
        with self._lock:
            round_obj.proposed_at = now_ts()
            round_obj.term = self._current_term
            healthy_count = sum(1 for n in self._nodes.values() if n.is_healthy)
            round_obj.quorum = max(1, int(healthy_count * self._quorum_fraction) + 1)
            self._rounds[round_obj.round_id] = round_obj
            self._events.append({
                "type": "round_proposed",
                "round_id": round_obj.round_id,
                "topic": round_obj.topic,
                "quorum": round_obj.quorum,
                "ts": now_ts(),
            })
            return round_obj

    def vote_round(self, round_id: str, approve: bool) -> Optional[ConsensusRound]:
        with self._lock:
            r = self._rounds.get(round_id)
            if not r or r.result != VoteResult.pending:
                return None
            if approve:
                r.votes_for += 1
            else:
                r.votes_against += 1

            if r.votes_for >= r.quorum:
                r.result = VoteResult.accepted
                r.decided_at = now_ts()
                self._finalize_round(round_id)
            elif r.votes_against >= r.quorum:
                r.result = VoteResult.rejected
                r.decided_at = now_ts()
                self._finalize_round(round_id)
            return r

    def auto_approve_round(self, round_id: str) -> Optional[ConsensusRound]:
        with self._lock:
            r = self._rounds.get(round_id)
            if not r:
                return None
            r.votes_for = r.quorum
            r.result = VoteResult.accepted
            r.decided_at = now_ts()
            self._finalize_round(round_id)
            return r

    def _finalize_round(self, round_id: str) -> None:
        r = self._rounds.pop(round_id, None)
        if r:
            self._completed_rounds.append(r)
            if len(self._completed_rounds) > self._max_history:
                self._completed_rounds = self._completed_rounds[-self._max_history:]

    def leader(self) -> Optional[str]:
        with self._lock:
            return self._leader_id

    def active_nodes(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [n.model_dump(mode="json") for n in self._nodes.values() if n.is_healthy]

    def pending_rounds(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in self._rounds.values()
                    if r.result == VoteResult.pending]

    def recent_rounds(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.model_dump(mode="json") for r in reversed(self._completed_rounds)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            healthy = sum(1 for n in self._nodes.values() if n.is_healthy)
            accepted = sum(1 for r in self._completed_rounds if r.result == VoteResult.accepted)
            rejected = sum(1 for r in self._completed_rounds if r.result == VoteResult.rejected)
            return {
                "ts": now_ts(),
                "total_nodes": len(self._nodes),
                "healthy_nodes": healthy,
                "current_term": self._current_term,
                "leader": self._leader_id,
                "pending_rounds": len(self._rounds),
                "total_accepted": accepted,
                "total_rejected": rejected,
            }
