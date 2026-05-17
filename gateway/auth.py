from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException

from identity.api_tokens import verify_org_api_secret
from identity.sessions import decode_v12_subject
from tenancy.tenant_router import TenantContext


@dataclass
class V12AuthResult:
    tenant: Optional[TenantContext]
    legacy_inference_ok: bool


def _legacy_tokens() -> set[str]:
    out = set()
    for env in ("ARSONIST_API_TOKEN", "ARSONIST_INFERENCE_API_TOKEN"):
        v = os.getenv(env, "").strip()
        if v:
            out.add(v)
    return out


def authenticate_bearer(authorization: str | None, path: str) -> V12AuthResult:
    """
    Resolve v12 tenant from Bearer token (JWT user/api-key scope or sk_ org secret).
    Optionally allow legacy static inference tokens when ARSONIST_V12_ALLOW_LEGACY_INFERENCE is true.
    """
    allow_legacy = os.getenv("ARSONIST_V12_ALLOW_LEGACY_INFERENCE", "true").lower() in ("1", "true", "yes")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    raw = authorization.removeprefix("Bearer ").strip()

    if raw.startswith("sk_"):
        rec = verify_org_api_secret(raw)
        if not rec:
            raise HTTPException(status_code=403, detail="invalid api key")
        return V12AuthResult(
            tenant=TenantContext(org_id=rec["org_id"], token_id=rec["token_id"], auth_kind="api_key"),
            legacy_inference_ok=False,
        )

    claims = decode_v12_subject(raw)
    if claims:
        if claims["scope"] == "arsonist-api-key":
            return V12AuthResult(
                tenant=TenantContext(org_id=claims["org_id"], token_id=claims["sub"], auth_kind="jwt"),
                legacy_inference_ok=False,
            )
        return V12AuthResult(
            tenant=TenantContext(
                org_id=claims["org_id"],
                user_id=claims["sub"],
                role=str(claims.get("role") or "viewer"),
                auth_kind="jwt",
            ),
            legacy_inference_ok=False,
        )

    if allow_legacy and raw in _legacy_tokens():
        return V12AuthResult(tenant=None, legacy_inference_ok=True)

    raise HTTPException(status_code=403, detail="v12 requires org JWT or sk_ API key")


def canonical_request(method: str, path: str, ts: str, body_sha256_hex: str) -> str:
    """Deterministic string for HMAC request signing (enterprise integrations)."""
    return f"{method.upper()}\n{path}\n{ts}\n{body_sha256_hex}"


def verify_request_signature(
    method: str,
    path: str,
    body: bytes,
    ts_header: str | None,
    sig_header: str | None,
    *,
    signing_secret: str | None = None,
    max_skew_sec: int = 300,
) -> bool:
    """
    Validates X-Arsonist-Timestamp + X-Arsonist-Signature (hex HMAC-SHA256 of canonical_request).
    Set ARSONIST_V12_REQUEST_SIGNING_SECRET to enable verification in upstream proxies.
    """
    import hashlib
    import hmac
    import time

    secret = signing_secret or os.getenv("ARSONIST_V12_REQUEST_SIGNING_SECRET", "").strip()
    if not secret or not ts_header or not sig_header:
        return False
    try:
        ts = int(ts_header)
    except ValueError:
        return False
    now = int(time.time())
    if abs(now - ts) > max_skew_sec:
        return False
    body_hash = hashlib.sha256(body or b"").hexdigest()
    msg = canonical_request(method, path, ts_header, body_hash).encode()
    expected = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_header.strip().lower())
