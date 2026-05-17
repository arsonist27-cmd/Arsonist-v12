from __future__ import annotations

import os

import jwt
from fastapi import Header, HTTPException, Request

from security.jwt_auth import DEFAULT_ALGORITHM

API_TOKEN = os.getenv("ARSONIST_API_TOKEN", "")
INFERENCE_TOKEN = os.getenv("ARSONIST_INFERENCE_API_TOKEN", "")
JWT_SECRET = os.getenv("ARSONIST_JWT_SECRET", "")


def _verify_jwt_inference(token: str) -> bool:
    if not JWT_SECRET:
        return False
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[DEFAULT_ALGORITHM])
    except jwt.PyJWTError:
        return False
    scope = claims.get("scope")
    return scope in ("arsonist-inference", "arsonist-node", "arsonist-admin")


def require_inference_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """
    Accepts:
    - Bearer ARSONIST_API_TOKEN or ARSONIST_INFERENCE_API_TOKEN
    - Bearer JWT with scope arsonist-inference (or node/admin for same cluster ops)
    When v12 multi-tenant mode is on, /v1 is authenticated by gateway middleware (JWT/sk_);
    legacy static tokens remain available if ARSONIST_V12_ALLOW_LEGACY_INFERENCE is true.
    """
    from gateway.api_gateway import v12_enabled

    if v12_enabled():
        if getattr(request.state, "v12_tenant", None) is not None:
            return
        if getattr(request.state, "v12_legacy_inference_ok", False):
            return
        raise HTTPException(status_code=401, detail="v12 gateway authentication required")

    if not API_TOKEN and not INFERENCE_TOKEN and not JWT_SECRET:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    if API_TOKEN and token == API_TOKEN:
        return
    if INFERENCE_TOKEN and token == INFERENCE_TOKEN:
        return
    if _verify_jwt_inference(token):
        return
    raise HTTPException(status_code=403, detail="invalid inference credentials")
