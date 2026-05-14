from __future__ import annotations

import hashlib
import hmac
import json
import os
from typing import Any, Dict


def _signing_key() -> str:
    return os.getenv("ARSONIST_JOB_SIGNING_KEY", "")


def canonical_job_bytes(job: Dict[str, Any]) -> bytes:
    """Stable serialization for HMAC (excludes envelope-only fields)."""
    core = {
        "id": job.get("id"),
        "type": job.get("type"),
        "task": job.get("task"),
        "required_nodes": job.get("required_nodes"),
        "power": job.get("power"),
        "gpu_required": job.get("gpu_required"),
    }
    return json.dumps(core, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_job_payload(job: Dict[str, Any]) -> str:
    key = _signing_key()
    if not key:
        return ""
    digest = hmac.new(key.encode("utf-8"), canonical_job_bytes(job), hashlib.sha256).hexdigest()
    return digest


def verify_job_payload(job: Dict[str, Any], signature: str | None) -> bool:
    key = _signing_key()
    if not key:
        return True
    if not signature:
        return False
    expected = sign_job_payload(job)
    return hmac.compare_digest(expected, signature)
