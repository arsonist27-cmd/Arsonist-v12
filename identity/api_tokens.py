from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from identity.registry_state import STATE, new_id


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def create_org_api_token(org_id: str, name: str = "default") -> Dict[str, Any]:
    """Returns { token_id, secret } — secret shown once (sk_...)."""
    if org_id not in STATE.orgs:
        raise KeyError("unknown org")
    secret = f"sk_{secrets.token_urlsafe(32)}"
    token_id = new_id("atk")
    digest = _sha256(secret)
    with STATE._lock:
        STATE.api_tokens[token_id] = {
            "token_id": token_id,
            "org_id": org_id,
            "name": name,
            "hash": digest,
            "prefix": secret[:12],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "revoked": False,
        }
        STATE.token_hash_index[digest] = token_id
    return {"token_id": token_id, "secret": secret, "prefix": secret[:12]}


def verify_org_api_secret(secret: str) -> Optional[Dict[str, Any]]:
    if not secret.startswith("sk_"):
        return None
    digest = _sha256(secret)
    with STATE._lock:
        tid = STATE.token_hash_index.get(digest)
        if not tid:
            return None
        rec = STATE.api_tokens.get(tid)
        if not rec or rec.get("revoked"):
            return None
        return {"org_id": rec["org_id"], "token_id": tid, "name": rec.get("name")}


def revoke_org_api_token(token_id: str) -> bool:
    with STATE._lock:
        rec = STATE.api_tokens.get(token_id)
        if not rec:
            return False
        rec["revoked"] = True
        digest = rec["hash"]
        STATE.token_hash_index.pop(digest, None)
        return True


def revoke_org_api_token_for_org(org_id: str, token_id: str) -> bool:
    with STATE._lock:
        rec = STATE.api_tokens.get(token_id)
        if not rec or rec["org_id"] != org_id:
            return False
    return revoke_org_api_token(token_id)


def rotate_org_api_token(token_id: str) -> Dict[str, Any]:
    """Revoke old id and issue a new secret for same org/name."""
    with STATE._lock:
        rec = STATE.api_tokens.get(token_id)
        if not rec or rec.get("revoked"):
            raise KeyError("token not found")
        org_id = rec["org_id"]
        name = rec.get("name", "default")
    revoke_org_api_token(token_id)
    return create_org_api_token(org_id, name=name)


def list_org_tokens(org_id: str) -> List[Dict[str, Any]]:
    with STATE._lock:
        out = []
        for rec in STATE.api_tokens.values():
            if rec["org_id"] == org_id:
                out.append(
                    {
                        "token_id": rec["token_id"],
                        "name": rec.get("name"),
                        "prefix": rec.get("prefix"),
                        "created_at": rec.get("created_at"),
                        "revoked": bool(rec.get("revoked")),
                    }
                )
        return out
