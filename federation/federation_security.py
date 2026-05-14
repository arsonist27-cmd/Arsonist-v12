from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, Optional


def _secret() -> str:
    return os.getenv("ARSONIST_FEDERATION_SECRET", os.getenv("FEDERATION_SHARED_SECRET", ""))


def _max_ts_skew_sec() -> int:
    return int(os.getenv("FEDERATION_SIGNATURE_MAX_SKEW_SEC", "300"))


def sign_payload(payload: Dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    key = _secret().encode("utf-8")
    return hmac.new(key, body, hashlib.sha256).hexdigest()


def verify_payload(payload: Dict[str, Any], signature: str | None) -> bool:
    if not _secret():
        return True
    if not signature:
        return False
    return hmac.compare_digest(sign_payload(payload), signature)


def verify_timestamp_header(timestamp_header: str | None) -> bool:
    """Reject missing or stale timestamps when HMAC secret is configured (caller gates on secret)."""
    if not timestamp_header:
        return False
    try:
        ts = int(str(timestamp_header).strip())
    except ValueError:
        return False
    return abs(int(time.time()) - ts) <= _max_ts_skew_sec()


def verify_signed_cluster_request(
    payload: Dict[str, Any],
    signature: str | None,
    timestamp_header: str | None,
) -> bool:
    """
    Validates HMAC over canonical JSON + timestamp skew.
    When no shared secret is set, accepts all requests (dev / single-cluster).
    """
    if not _secret():
        return True
    if not verify_timestamp_header(timestamp_header):
        return False
    return verify_payload(payload, signature)


def build_headers(payload: Dict[str, Any]) -> Dict[str, str]:
    return {"X-Federation-Signature": sign_payload(payload), "X-Federation-Timestamp": str(int(time.time()))}
