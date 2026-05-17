from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import jwt

from security.jwt_auth import DEFAULT_ALGORITHM

USER_SCOPE = "arsonist-user"
API_KEY_SCOPE = "arsonist-api-key"


def _secret() -> str:
    s = os.getenv("ARSONIST_JWT_SECRET", "")
    if not s:
        raise ValueError("ARSONIST_JWT_SECRET required for v12 user/API JWTs")
    return s


def issue_user_jwt(user_id: str, org_id: str, role: str, ttl_sec: int | None = None) -> str:
    now = int(time.time())
    exp = ttl_sec if ttl_sec is not None else int(os.getenv("ARSONIST_V12_JWT_TTL_SEC", "3600"))
    payload: Dict[str, Any] = {
        "sub": user_id,
        "org_id": org_id,
        "role": role,
        "iat": now,
        "exp": now + exp,
        "scope": USER_SCOPE,
    }
    return jwt.encode(payload, _secret(), algorithm=DEFAULT_ALGORITHM)


def issue_api_key_jwt(org_id: str, token_id: str, ttl_sec: int | None = None) -> str:
    """JWT representing an org API key (for clients that prefer Bearer JWT over sk_ secret)."""
    now = int(time.time())
    exp = ttl_sec if ttl_sec is not None else int(os.getenv("ARSONIST_V12_API_JWT_TTL_SEC", "86400"))
    payload: Dict[str, Any] = {
        "sub": token_id,
        "org_id": org_id,
        "iat": now,
        "exp": now + exp,
        "scope": API_KEY_SCOPE,
    }
    return jwt.encode(payload, _secret(), algorithm=DEFAULT_ALGORITHM)


def decode_v12_subject(token: str) -> Optional[Dict[str, Any]]:
    try:
        claims = jwt.decode(token, _secret(), algorithms=[DEFAULT_ALGORITHM])
    except jwt.PyJWTError:
        return None
    scope = claims.get("scope")
    if scope not in (USER_SCOPE, API_KEY_SCOPE):
        return None
    org_id = claims.get("org_id")
    if not isinstance(org_id, str):
        return None
    sub = claims.get("sub")
    if not isinstance(sub, str):
        return None
    return {
        "scope": scope,
        "org_id": org_id,
        "sub": sub,
        "role": claims.get("role") if scope == USER_SCOPE else None,
    }
