from __future__ import annotations

import threading
from typing import Any, Dict, List


class EnvironmentManager:
    """Per-tenant logical environments (dev/stage/prod) for rollout isolation metadata."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._envs: Dict[str, List[Dict[str, Any]]] = {}

    def list_envs(self, org_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._envs.get(org_id, [{"name": "default", "region": "local"}]))

    def register(self, org_id: str, name: str, region: str) -> Dict[str, Any]:
        row = {"name": name, "region": region}
        with self._lock:
            lst = self._envs.setdefault(org_id, [])
            lst.append(row)
        return row


ENV = EnvironmentManager()
