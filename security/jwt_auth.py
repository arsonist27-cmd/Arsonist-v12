from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import jwt

DEFAULT_ALGORITHM = "HS256"


def _secret() -> str:
    return os.getenv("ARSONIST_JWT_SECRET", "")


def issue_node_token(node_id: str, ttl_sec: int | None = None) -> str:
    secret = _secret()
    if not secret:
        raise ValueError("ARSONIST_JWT_SECRET is not set")
    now = int(time.time())
    exp = ttl_sec if ttl_sec is not None else int(os.getenv("ARSONIST_JWT_TTL_SEC", "86400"))
    payload: Dict[str, Any] = {
        "sub": node_id,
        "iat": now,
        "exp": now + exp,
        "scope": "arsonist-node",
    }
    return jwt.encode(payload, secret, algorithm=DEFAULT_ALGORITHM)


def verify_node_token(token: str) -> Optional[str]:
    secret = _secret()
    if not secret:
        return None
    try:
        claims = jwt.decode(token, secret, algorithms=[DEFAULT_ALGORITHM])
    except jwt.PyJWTError:
        return None
    sub = claims.get("sub")
    if not isinstance(sub, str):
        return None
    if claims.get("scope") != "arsonist-node":
        return None
    return sub
