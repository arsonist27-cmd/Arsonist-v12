from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("routing.affinity")


class AffinityRule:
    def __init__(
        self,
        key: str,
        target_region: str,
        weight: float = 1.0,
        ttl_sec: float = 3600.0,
    ) -> None:
        self.key = key
        self.target_region = target_region
        self.weight = weight
        self.created_at = now_ts()
        self.ttl_sec = ttl_sec

    def is_expired(self) -> bool:
        return now_ts() - self.created_at > self.ttl_sec

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "target_region": self.target_region,
            "weight": self.weight,
            "created_at": self.created_at,
            "ttl_sec": self.ttl_sec,
            "expired": self.is_expired(),
        }


class RequestAffinityManager:
    """Manages request-to-region affinity for session stickiness and model locality."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._session_affinity: Dict[str, AffinityRule] = {}
        self._model_affinity: Dict[str, AffinityRule] = {}
        self._client_affinity: Dict[str, AffinityRule] = {}

    def set_session_affinity(self, session_id: str, region_id: str, ttl_sec: float = 3600.0) -> None:
        with self._lock:
            self._session_affinity[session_id] = AffinityRule(
                key=session_id, target_region=region_id, ttl_sec=ttl_sec
            )

    def get_session_affinity(self, session_id: str) -> Optional[str]:
        with self._lock:
            rule = self._session_affinity.get(session_id)
            if rule and not rule.is_expired():
                return rule.target_region
            if rule and rule.is_expired():
                del self._session_affinity[session_id]
            return None

    def set_model_affinity(self, model_id: str, region_id: str, weight: float = 1.0) -> None:
        with self._lock:
            self._model_affinity[model_id] = AffinityRule(
                key=model_id, target_region=region_id, weight=weight, ttl_sec=86400.0
            )

    def get_model_affinity(self, model_id: str) -> Optional[str]:
        with self._lock:
            rule = self._model_affinity.get(model_id)
            if rule and not rule.is_expired():
                return rule.target_region
            return None

    def set_client_affinity(self, client_id: str, region_id: str, ttl_sec: float = 1800.0) -> None:
        with self._lock:
            self._client_affinity[client_id] = AffinityRule(
                key=client_id, target_region=region_id, ttl_sec=ttl_sec
            )

    def get_client_affinity(self, client_id: str) -> Optional[str]:
        with self._lock:
            rule = self._client_affinity.get(client_id)
            if rule and not rule.is_expired():
                return rule.target_region
            return None

    def resolve_affinity(self, session_id: str = "", model_id: str = "", client_id: str = "") -> Optional[str]:
        if session_id:
            region = self.get_session_affinity(session_id)
            if region:
                return region
        if model_id:
            region = self.get_model_affinity(model_id)
            if region:
                return region
        if client_id:
            region = self.get_client_affinity(client_id)
            if region:
                return region
        return None

    def cleanup_expired(self) -> int:
        removed = 0
        with self._lock:
            for store in [self._session_affinity, self._model_affinity, self._client_affinity]:
                expired_keys = [k for k, v in store.items() if v.is_expired()]
                for k in expired_keys:
                    del store[k]
                    removed += 1
        return removed

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ts": now_ts(),
                "session_rules": len(self._session_affinity),
                "model_rules": len(self._model_affinity),
                "client_rules": len(self._client_affinity),
            }
