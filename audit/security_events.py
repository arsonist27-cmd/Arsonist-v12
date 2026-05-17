from __future__ import annotations

from enum import Enum
from typing import Any, Dict

from audit.audit_log import append_audit


class SecurityEvent(str, Enum):
    auth_failure = "auth_failure"
    rate_limited = "rate_limited"
    quota_exceeded = "quota_exceeded"
    token_revoked = "token_revoked"
    suspicious_path = "suspicious_path"


def log_security_event(kind: SecurityEvent, **ctx: Any) -> None:
    append_audit(type="security", event=kind.value, **ctx)
