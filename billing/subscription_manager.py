from __future__ import annotations

import threading
from typing import Any, Dict

from identity.organizations import get_organization, update_org_plan

_DEFAULT_LIMITS: Dict[str, Dict[str, float]] = {
    "free": {
        "requests_per_sec": 5.0,
        "tokens_per_minute": 20_000.0,
        "gpu_seconds_per_day": 60.0,
        "max_concurrency": 2.0,
        "storage_gb": 1.0,
    },
    "pro": {
        "requests_per_sec": 50.0,
        "tokens_per_minute": 500_000.0,
        "gpu_seconds_per_day": 36_000.0,
        "max_concurrency": 32.0,
        "storage_gb": 100.0,
    },
    "enterprise": {
        "requests_per_sec": 500.0,
        "tokens_per_minute": 5_000_000.0,
        "gpu_seconds_per_day": 864_000.0,
        "max_concurrency": 256.0,
        "storage_gb": 5000.0,
    },
}


class SubscriptionManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._custom: Dict[str, Dict[str, float]] = {}

    def get_subscription(self, org_id: str) -> Dict[str, Any]:
        org = get_organization(org_id)
        plan = org.plan if org else "free"
        limits = dict(_DEFAULT_LIMITS.get(plan, _DEFAULT_LIMITS["free"]))
        with self._lock:
            if org_id in self._custom:
                limits.update(self._custom[org_id])
        return {"org_id": org_id, "plan": plan, "limits": limits}

    def set_plan(self, org_id: str, plan: str) -> None:
        update_org_plan(org_id, plan)

    def set_custom_limits(self, org_id: str, limits: Dict[str, float]) -> None:
        with self._lock:
            self._custom[org_id] = limits


SUBSCRIPTIONS = SubscriptionManager()


def get_subscription(org_id: str) -> Dict[str, Any]:
    return SUBSCRIPTIONS.get_subscription(org_id)


def set_plan(org_id: str, plan: str) -> None:
    SUBSCRIPTIONS.set_plan(org_id, plan)
