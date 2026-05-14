from __future__ import annotations

import os
import time
from typing import Optional

import jwt
from fastapi import Header, HTTPException

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


def require_inference_auth(authorization: str | None = Header(default=None)) -> None:
    """
    Accepts:
    - Bearer ARSONIST_API_TOKEN or ARSONIST_INFERENCE_API_TOKEN
    - Bearer JWT with scope arsonist-inference (or node/admin for same cluster ops)
    """
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
