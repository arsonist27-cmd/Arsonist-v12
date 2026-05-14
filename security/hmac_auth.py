from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Dict


def _body_bytes(payload: Dict[str, Any] | None) -> bytes:
    if payload is None:
        return b""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(payload: Dict[str, Any] | None) -> str:
    return hashlib.sha256(_body_bytes(payload)).hexdigest()


def _message(node_id: str, ts: int, method: str, path: str, payload: Dict[str, Any] | None) -> bytes:
    data = f"{node_id}:{ts}:{method.upper()}:{path}:{_digest(payload)}"
    return data.encode("utf-8")


def sign_request(
    node_id: str,
    node_secret: str,
    method: str,
    path: str,
    payload: Dict[str, Any] | None = None,
    now_ts: int | None = None,
) -> str:
    ts = now_ts if now_ts is not None else int(time.time())
    msg = _message(node_id, ts, method, path, payload)
    return hmac.new(node_secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def build_auth_headers(
    node_id: str,
    node_secret: str,
    method: str,
    path: str,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, str]:
    ts = int(time.time())
    sig = sign_request(node_id, node_secret, method, path, payload, now_ts=ts)
    return {
        "X-Node-Id": node_id,
        "X-Auth-Timestamp": str(ts),
        "X-Auth-Signature": sig,
    }


def verify_headers(
    node_secret: str,
    node_id: str,
    method: str,
    path: str,
    payload: Dict[str, Any] | None,
    ts: str | None,
    signature: str | None,
    max_skew_sec: int = 60,
) -> bool:
    if not ts or not signature:
        return False
    try:
        ts_i = int(ts)
    except ValueError:
        return False
    if abs(int(time.time()) - ts_i) > max_skew_sec:
        return False
    expected = sign_request(node_id, node_secret, method, path, payload, now_ts=ts_i)
    return hmac.compare_digest(signature, expected)
