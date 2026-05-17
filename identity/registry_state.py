from __future__ import annotations

import secrets
import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

from identity.roles import Role


class _RegistryState:
    """Process-local identity store (v12). Back with Redis/Postgres in production."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.orgs: Dict[str, Dict[str, Any]] = {}
        self.users: Dict[str, Dict[str, Any]] = {}
        self.memberships: Dict[str, List[Dict[str, Any]]] = {}  # org_id -> [{user_id, role}]
        self.api_tokens: Dict[str, Dict[str, Any]] = {}  # token_id -> {org_id, hash, prefix, name}
        self.token_hash_index: Dict[str, str] = {}  # sha256 -> token_id

    def reset(self) -> None:
        with self._lock:
            self.orgs.clear()
            self.users.clear()
            self.memberships.clear()
            self.api_tokens.clear()
            self.token_hash_index.clear()


STATE = _RegistryState()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"
