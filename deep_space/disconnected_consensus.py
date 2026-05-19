"""v16 Disconnected Consensus.

Provides partition-tolerant consensus supporting partial synchronization,
delayed consensus rounds, eventual consistency, and temporary authority
delegation for infrastructure operating during communication blackouts
or regional outages.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("deep_space.disconnected_consensus")


class AuthorityMode(str, Enum):
    centralized = "centralized"
    delegated = "delegated"
    autonomous = "autonomous"
    partitioned = "partitioned"


class ConsensusPhase(str, Enum):
    proposing = "proposing"
    collecting = "collecting"
    waiting = "waiting"
    decided = "decided"
    expired = "expired"


class DisconnectedProposal(BaseModel):
    proposal_id: str
    topic: str = ""
    source_region: str = ""
    authority_mode: AuthorityMode = AuthorityMode.centralized
    phase: ConsensusPhase = ConsensusPhase.proposing
    votes_for: int = 0
    votes_against: int = 0
    quorum_needed: int = 1
    regions_responded: List[str] = Field(default_factory=list)
    regions_unreachable: List[str] = Field(default_factory=list)
    ttl_s: float = 300.0
    allow_partial: bool = True
    partial_threshold: float = 0.5
    proposed_at: float = 0.0
    decided_at: float = 0.0
    payload: Dict[str, Any] = Field(default_factory=dict)
    decision: str = ""


class DelegatedAuthority(BaseModel):
    region: str
    delegated_at: float = 0.0
    expires_at: float = 0.0
    scope: List[str] = Field(default_factory=list)
    reason: str = ""
    is_active: bool = True


class DisconnectedConsensus:
    """Partition-tolerant consensus engine supporting delayed rounds,
    temporary authority delegation, and autonomous decision-making
    during communication blackouts."""

    def __init__(self, local_region: str = "local",
                 default_ttl_s: float = 300.0,
                 partial_threshold: float = 0.5,
                 max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._local_region = local_region
        self._default_ttl = default_ttl_s
        self._partial_threshold = partial_threshold
        self._max_history = max_history
        self._proposals: Dict[str, DisconnectedProposal] = {}
        self._completed: List[DisconnectedProposal] = []
        self._delegations: Dict[str, DelegatedAuthority] = {}
        self._authority_mode = AuthorityMode.centralized
        self._total_proposals = 0
        self._total_decided = 0
        self._total_delegated = 0
        self._events: List[Dict[str, Any]] = []

    def propose(self, proposal: DisconnectedProposal) -> DisconnectedProposal:
        with self._lock:
            proposal.proposed_at = now_ts()
            proposal.phase = ConsensusPhase.collecting
            proposal.source_region = self._local_region
            if proposal.ttl_s <= 0:
                proposal.ttl_s = self._default_ttl
            if proposal.partial_threshold <= 0:
                proposal.partial_threshold = self._partial_threshold
            self._proposals[proposal.proposal_id] = proposal
            self._total_proposals += 1
            self._add_event("proposal_created", proposal.proposal_id,
                            topic=proposal.topic)
            return proposal

    def vote(self, proposal_id: str, region: str, approve: bool) -> Optional[DisconnectedProposal]:
        with self._lock:
            proposal = self._proposals.get(proposal_id)
            if not proposal or proposal.phase not in (ConsensusPhase.collecting, ConsensusPhase.waiting):
                return None

            if region in proposal.regions_responded:
                return proposal

            if approve:
                proposal.votes_for += 1
            else:
                proposal.votes_against += 1
            proposal.regions_responded.append(region)

            if region in proposal.regions_unreachable:
                proposal.regions_unreachable.remove(region)

            total_votes = proposal.votes_for + proposal.votes_against
            if total_votes >= proposal.quorum_needed:
                if proposal.votes_for > proposal.votes_against:
                    proposal.decision = "approved"
                else:
                    proposal.decision = "rejected"
                proposal.phase = ConsensusPhase.decided
                proposal.decided_at = now_ts()
                self._total_decided += 1
                self._finalize(proposal_id)
                self._add_event("proposal_decided", proposal_id,
                                decision=proposal.decision)
            elif proposal.allow_partial and total_votes >= int(proposal.quorum_needed * proposal.partial_threshold):
                proposal.phase = ConsensusPhase.waiting

            return proposal

    def check_partial_decisions(self) -> List[DisconnectedProposal]:
        with self._lock:
            ts = now_ts()
            decided = []
            expired_ids = []

            for pid, p in self._proposals.items():
                if p.phase == ConsensusPhase.decided:
                    continue
                elapsed = ts - p.proposed_at
                if elapsed > p.ttl_s:
                    if p.allow_partial and p.votes_for > p.votes_against and (p.votes_for + p.votes_against) > 0:
                        p.decision = "approved_partial"
                        p.phase = ConsensusPhase.decided
                        p.decided_at = ts
                        self._total_decided += 1
                        decided.append(p)
                        self._add_event("proposal_partial_decided", pid,
                                        decision=p.decision,
                                        votes_for=p.votes_for,
                                        votes_against=p.votes_against)
                    else:
                        p.phase = ConsensusPhase.expired
                        expired_ids.append(pid)
                        self._add_event("proposal_expired", pid)

            for pid in expired_ids:
                self._finalize(pid)
            for p in decided:
                self._finalize(p.proposal_id)

            return decided

    def delegate_authority(self, region: str, scope: List[str],
                           duration_s: float = 600.0,
                           reason: str = "") -> DelegatedAuthority:
        with self._lock:
            ts = now_ts()
            delegation = DelegatedAuthority(
                region=region,
                delegated_at=ts,
                expires_at=ts + duration_s,
                scope=scope,
                reason=reason,
                is_active=True,
            )
            self._delegations[region] = delegation
            self._total_delegated += 1
            self._authority_mode = AuthorityMode.delegated
            self._add_event("authority_delegated", region,
                            scope=scope, duration_s=duration_s)
            return delegation

    def revoke_delegation(self, region: str) -> bool:
        with self._lock:
            delegation = self._delegations.get(region)
            if not delegation:
                return False
            delegation.is_active = False
            self._add_event("authority_revoked", region)
            active = [d for d in self._delegations.values() if d.is_active]
            if not active:
                self._authority_mode = AuthorityMode.centralized
            return True

    def check_delegation_expiry(self) -> List[str]:
        with self._lock:
            ts = now_ts()
            expired = []
            for region, delegation in self._delegations.items():
                if delegation.is_active and ts > delegation.expires_at:
                    delegation.is_active = False
                    expired.append(region)
                    self._add_event("delegation_expired", region)
            if expired:
                active = [d for d in self._delegations.values() if d.is_active]
                if not active:
                    self._authority_mode = AuthorityMode.centralized
            return expired

    def has_authority(self, region: str, scope: str) -> bool:
        with self._lock:
            delegation = self._delegations.get(region)
            if not delegation or not delegation.is_active:
                return False
            if now_ts() > delegation.expires_at:
                delegation.is_active = False
                return False
            return scope in delegation.scope or "*" in delegation.scope

    def set_autonomous(self) -> None:
        with self._lock:
            self._authority_mode = AuthorityMode.autonomous
            self._add_event("mode_autonomous", self._local_region)

    def _finalize(self, proposal_id: str) -> None:
        proposal = self._proposals.pop(proposal_id, None)
        if proposal:
            self._completed.append(proposal)
            if len(self._completed) > self._max_history:
                self._completed = self._completed[-self._max_history:]

    def _add_event(self, event_type: str, ref: str, **kwargs: Any) -> None:
        event: Dict[str, Any] = {"type": event_type, "ref": ref, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def active_proposals(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._proposals.values()
                    if p.phase not in (ConsensusPhase.decided, ConsensusPhase.expired)]

    def active_delegations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.model_dump(mode="json") for d in self._delegations.values()
                    if d.is_active]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            active_delegations = sum(1 for d in self._delegations.values() if d.is_active)
            return {
                "ts": now_ts(),
                "local_region": self._local_region,
                "authority_mode": self._authority_mode.value,
                "total_proposals": self._total_proposals,
                "total_decided": self._total_decided,
                "pending_proposals": len(self._proposals),
                "active_delegations": active_delegations,
                "total_delegated": self._total_delegated,
            }
