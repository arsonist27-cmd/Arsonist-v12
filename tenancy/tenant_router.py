from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class TenantContext:
    org_id: str
    user_id: Optional[str] = None
    token_id: Optional[str] = None
    role: Optional[str] = None
    auth_kind: str = "jwt"  # jwt | api_key

    def to_headers(self) -> Dict[str, str]:
        h = {"X-Arsonist-Org-Id": self.org_id}
        if self.user_id:
            h["X-Arsonist-User-Id"] = self.user_id
        return h


def resolve_tenant_header(org_header: Optional[str], ctx: TenantContext) -> str:
    """Prefer explicit gateway-resolved org; header must match when both present."""
    if org_header and org_header != ctx.org_id:
        raise ValueError("org header mismatch")
    return ctx.org_id
