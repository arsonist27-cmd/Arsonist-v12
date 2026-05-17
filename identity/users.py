from __future__ import annotations

import hashlib
import secrets
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from identity.permissions import has_permission
from identity.registry_state import STATE, new_id
from identity.roles import Role


class User(BaseModel):
    user_id: str
    email: str
    display_name: str = ""
    password_hash: str = Field(default="", repr=False)


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(8)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}${h}"


def verify_password(user: User, password: str) -> bool:
    if not user.password_hash or "$" not in user.password_hash:
        return False
    salt, stored = user.password_hash.split("$", 1)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return secrets.compare_digest(h, stored)


def create_user(email: str, display_name: str, password: str) -> User:
    user_id = new_id("usr")
    u = User(
        user_id=user_id,
        email=email.lower().strip(),
        display_name=display_name,
        password_hash=_hash_password(password),
    )
    with STATE._lock:
        STATE.users[user_id] = u.model_dump()
    return u


def get_user(user_id: str) -> Optional[User]:
    with STATE._lock:
        raw = STATE.users.get(user_id)
    return User(**raw) if raw else None


def get_user_by_email(email: str) -> Optional[User]:
    e = email.lower().strip()
    with STATE._lock:
        for raw in STATE.users.values():
            if raw.get("email") == e:
                return User(**raw)
    return None


def add_user_to_org(user_id: str, org_id: str, role: Role) -> Dict[str, Any]:
    with STATE._lock:
        if user_id not in STATE.users or org_id not in STATE.orgs:
            raise KeyError("unknown user or org")
        m = STATE.memberships.setdefault(org_id, [])
        for row in m:
            if row["user_id"] == user_id:
                row["role"] = role.value
                return {"status": "updated", "user_id": user_id, "org_id": org_id, "role": role.value}
        m.append({"user_id": user_id, "role": role.value})
        return {"status": "added", "user_id": user_id, "org_id": org_id, "role": role.value}


def membership(user_id: str, org_id: str) -> Optional[Role]:
    with STATE._lock:
        for row in STATE.memberships.get(org_id, []):
            if row["user_id"] == user_id:
                return Role(row["role"])
    return None


def list_org_members(org_id: str) -> List[Dict[str, Any]]:
    with STATE._lock:
        return list(STATE.memberships.get(org_id, []))


def user_can(user_id: str, org_id: str, permission: str) -> bool:
    role = membership(user_id, org_id)
    if not role:
        return False
    return has_permission(role, permission)
