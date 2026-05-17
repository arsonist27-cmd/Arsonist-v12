from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from identity.registry_state import STATE, new_id


class Organization(BaseModel):
    org_id: str
    name: str
    slug: str
    plan: str = Field(default="free")
    quota_profile: Dict[str, float] = Field(default_factory=dict)


def create_organization(name: str, slug: str, plan: str = "free") -> Organization:
    org_id = new_id("org")
    org = Organization(org_id=org_id, name=name, slug=slug.lower().replace(" ", "-"), plan=plan)
    with STATE._lock:
        STATE.orgs[org_id] = org.model_dump()
        STATE.memberships.setdefault(org_id, [])
    return org


def get_organization(org_id: str) -> Optional[Organization]:
    with STATE._lock:
        raw = STATE.orgs.get(org_id)
    if not raw:
        return None
    return Organization(**raw)


def list_organizations() -> List[Organization]:
    with STATE._lock:
        return [Organization(**v) for v in STATE.orgs.values()]


def update_org_plan(org_id: str, plan: str) -> Optional[Organization]:
    with STATE._lock:
        if org_id not in STATE.orgs:
            return None
        STATE.orgs[org_id]["plan"] = plan
        return Organization(**STATE.orgs[org_id])
