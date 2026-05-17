"""v15 Distributed Decision Engine.

Coordinates scaling, migration, placement, and repair decisions across
planetary infrastructure using decentralized coordination and
consensus-assisted orchestration.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("coordination.decision_engine")


class DecisionType(str, Enum):
    scaling = "scaling"
    migration = "migration"
    placement = "placement"
    repair = "repair"
    failover = "failover"
    rebalance = "rebalance"
    carbon_shift = "carbon_shift"


class DecisionPriority(str, Enum):
    critical = "critical"
    high = "high"
    normal = "normal"
    low = "low"


class DecisionStatus(str, Enum):
    proposed = "proposed"
    voting = "voting"
    approved = "approved"
    executing = "executing"
    completed = "completed"
    rejected = "rejected"
    failed = "failed"


class DecisionProposal(BaseModel):
    proposal_id: str
    decision_type: DecisionType = DecisionType.placement
    priority: DecisionPriority = DecisionPriority.normal
    status: DecisionStatus = DecisionStatus.proposed
    source_region: str = ""
    target_region: str = ""
    affected_workloads: List[str] = Field(default_factory=list)
    rationale: str = ""
    score: float = 0.0
    votes_for: int = 0
    votes_against: int = 0
    quorum_needed: int = 1
    proposed_at: float = 0.0
    decided_at: float = 0.0
    executed_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DistributedDecisionEngine:
    """Coordinates infrastructure decisions across planetary-scale
    systems using decentralized voting and consensus-assisted orchestration."""

    def __init__(self, quorum_size: int = 3, max_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._quorum_size = quorum_size
        self._max_history = max_history
        self._proposals: Dict[str, DecisionProposal] = {}
        self._completed: List[DecisionProposal] = []
        self._total_proposed = 0
        self._total_approved = 0
        self._total_rejected = 0
        self._total_executed = 0
        self._events: List[Dict[str, Any]] = []

    def propose(self, proposal: DecisionProposal) -> DecisionProposal:
        with self._lock:
            proposal.proposed_at = now_ts()
            proposal.status = DecisionStatus.proposed
            proposal.quorum_needed = self._quorum_size
            self._proposals[proposal.proposal_id] = proposal
            self._total_proposed += 1
            self._add_event("decision_proposed", proposal.proposal_id,
                            decision_type=proposal.decision_type.value,
                            priority=proposal.priority.value)
            return proposal

    def analyze_and_propose(self, telemetry: Dict[str, Any]) -> List[DecisionProposal]:
        proposals = []
        ts = now_ts()
        regions = telemetry.get("regions", [])

        for r in regions:
            rid = r.get("region_id", "")
            sat = r.get("workload_saturation", 0.0)
            if sat > 0.9:
                proposal = DecisionProposal(
                    proposal_id=f"scale-{rid}-{int(ts)}",
                    decision_type=DecisionType.scaling,
                    priority=DecisionPriority.high,
                    source_region=rid,
                    rationale=f"Region {rid} saturation at {sat:.0%}, needs scaling",
                    score=sat,
                )
                proposals.append(self.propose(proposal))

            thermal = r.get("thermal_pressure", 0.0)
            if thermal > 0.85:
                low_thermal = [o for o in regions if o.get("thermal_pressure", 0) < 0.4
                               and o.get("region_id", "") != rid]
                if low_thermal:
                    target = min(low_thermal, key=lambda o: o.get("thermal_pressure", 1))
                    proposal = DecisionProposal(
                        proposal_id=f"migrate-thermal-{rid}-{int(ts)}",
                        decision_type=DecisionType.migration,
                        priority=DecisionPriority.high,
                        source_region=rid,
                        target_region=target.get("region_id", ""),
                        rationale=f"Thermal pressure {thermal:.0%} in {rid}, migrating to cooler region",
                        score=thermal,
                    )
                    proposals.append(self.propose(proposal))

            if r.get("status") == "degraded":
                proposal = DecisionProposal(
                    proposal_id=f"repair-{rid}-{int(ts)}",
                    decision_type=DecisionType.repair,
                    priority=DecisionPriority.critical,
                    source_region=rid,
                    rationale=f"Region {rid} degraded, needs repair",
                    score=0.95,
                )
                proposals.append(self.propose(proposal))

            carbon = r.get("carbon_intensity", 0.5)
            if carbon > 0.7:
                green = [o for o in regions if o.get("carbon_intensity", 1) < 0.3
                         and o.get("region_id", "") != rid]
                if green:
                    target = min(green, key=lambda o: o.get("carbon_intensity", 1))
                    proposal = DecisionProposal(
                        proposal_id=f"carbon-shift-{rid}-{int(ts)}",
                        decision_type=DecisionType.carbon_shift,
                        priority=DecisionPriority.low,
                        source_region=rid,
                        target_region=target.get("region_id", ""),
                        rationale=f"Carbon intensity {carbon:.2f} in {rid}, shifting to greener region",
                        score=carbon,
                    )
                    proposals.append(self.propose(proposal))

        return proposals

    def vote(self, proposal_id: str, approve: bool) -> Optional[DecisionProposal]:
        with self._lock:
            proposal = self._proposals.get(proposal_id)
            if not proposal:
                return None
            if approve:
                proposal.votes_for += 1
            else:
                proposal.votes_against += 1

            if proposal.votes_for >= proposal.quorum_needed:
                proposal.status = DecisionStatus.approved
                proposal.decided_at = now_ts()
                self._total_approved += 1
                self._add_event("decision_approved", proposal_id)
            elif proposal.votes_against >= proposal.quorum_needed:
                proposal.status = DecisionStatus.rejected
                proposal.decided_at = now_ts()
                self._total_rejected += 1
                self._finalize(proposal_id)
                self._add_event("decision_rejected", proposal_id)

            return proposal

    def auto_approve(self, proposal_id: str) -> Optional[DecisionProposal]:
        with self._lock:
            proposal = self._proposals.get(proposal_id)
            if not proposal:
                return None
            proposal.votes_for = proposal.quorum_needed
            proposal.status = DecisionStatus.approved
            proposal.decided_at = now_ts()
            self._total_approved += 1
            self._add_event("decision_auto_approved", proposal_id)
            return proposal

    def execute(self, proposal_id: str) -> Optional[DecisionProposal]:
        with self._lock:
            proposal = self._proposals.get(proposal_id)
            if not proposal or proposal.status != DecisionStatus.approved:
                return None
            proposal.status = DecisionStatus.executing
            proposal.executed_at = now_ts()
            return proposal

    def complete(self, proposal_id: str, success: bool = True) -> Optional[DecisionProposal]:
        with self._lock:
            proposal = self._proposals.get(proposal_id)
            if not proposal:
                return None
            proposal.status = DecisionStatus.completed if success else DecisionStatus.failed
            self._total_executed += 1
            self._finalize(proposal_id)
            self._add_event("decision_completed" if success else "decision_failed", proposal_id)
            return proposal

    def _finalize(self, proposal_id: str) -> None:
        proposal = self._proposals.pop(proposal_id, None)
        if proposal:
            self._completed.append(proposal)
            if len(self._completed) > self._max_history:
                self._completed = self._completed[-self._max_history:]

    def _add_event(self, event_type: str, proposal_id: str, **kwargs: Any) -> None:
        event = {"type": event_type, "proposal_id": proposal_id, "ts": now_ts()}
        event.update(kwargs)
        self._events.append(event)
        if len(self._events) > self._max_history:
            self._events = self._events[-self._max_history:]

    def pending_proposals(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._proposals.values()
                    if p.status in (DecisionStatus.proposed, DecisionStatus.voting)]

    def approved_proposals(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in self._proposals.values()
                    if p.status == DecisionStatus.approved]

    def recent_completed(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [p.model_dump(mode="json") for p in reversed(self._completed)][:limit]

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._events))[:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "total_proposed": self._total_proposed,
                "total_approved": self._total_approved,
                "total_rejected": self._total_rejected,
                "total_executed": self._total_executed,
                "pending": len(self._proposals),
                "quorum_size": self._quorum_size,
            }
