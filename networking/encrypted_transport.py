from __future__ import annotations

import hashlib
import hmac
import os
import threading
import time
from typing import Any, Dict, List, Optional

from shared.utils import now_ts, setup_logging

logger = setup_logging("networking.transport")


class TransportSession:
    def __init__(self, session_id: str, peer_id: str, direction: str = "outbound") -> None:
        self.session_id = session_id
        self.peer_id = peer_id
        self.direction = direction
        self.created_at = now_ts()
        self.last_active = self.created_at
        self.messages_sent = 0
        self.messages_received = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        self.authenticated = False
        self.encryption_algo = "AES-256-GCM"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "peer_id": self.peer_id,
            "direction": self.direction,
            "created_at": self.created_at,
            "last_active": self.last_active,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "authenticated": self.authenticated,
            "encryption_algo": self.encryption_algo,
        }


class EncryptedTransport:
    """Mutual authentication and encrypted transport layer for inter-region communication.

    Supports request signing, regional trust validation, and audit logging.
    """

    def __init__(
        self,
        local_id: str,
        shared_secret: str | None = None,
        max_skew_sec: float = 300.0,
    ) -> None:
        self.local_id = local_id
        self._secret = shared_secret or os.getenv("ARSONIST_TRANSPORT_SECRET", "")
        self.max_skew_sec = max_skew_sec
        self._lock = threading.Lock()
        self._sessions: Dict[str, TransportSession] = {}
        self._trusted_peers: set[str] = set()
        self._audit_log: List[Dict[str, Any]] = []
        self._auth_failures = 0

    def add_trusted_peer(self, peer_id: str) -> None:
        with self._lock:
            self._trusted_peers.add(peer_id)

    def remove_trusted_peer(self, peer_id: str) -> None:
        with self._lock:
            self._trusted_peers.discard(peer_id)

    def is_trusted(self, peer_id: str) -> bool:
        with self._lock:
            if not self._trusted_peers:
                return True
            return peer_id in self._trusted_peers

    def sign_request(self, payload: bytes, timestamp: float | None = None) -> Dict[str, str]:
        ts = timestamp or now_ts()
        msg = payload + str(ts).encode()
        sig = hmac.new(
            self._secret.encode() if self._secret else b"default",
            msg,
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-Transport-Signature": sig,
            "X-Transport-Timestamp": str(ts),
            "X-Transport-Sender": self.local_id,
        }

    def verify_request(self, payload: bytes, signature: str, timestamp: str, sender: str) -> bool:
        if not self.is_trusted(sender):
            self._record_audit("auth_rejected_untrusted", sender)
            self._auth_failures += 1
            return False

        try:
            ts = float(timestamp)
        except (ValueError, TypeError):
            self._record_audit("auth_rejected_bad_timestamp", sender)
            self._auth_failures += 1
            return False

        skew = abs(now_ts() - ts)
        if skew > self.max_skew_sec:
            self._record_audit("auth_rejected_clock_skew", sender, {"skew": skew})
            self._auth_failures += 1
            return False

        msg = payload + timestamp.encode()
        expected = hmac.new(
            self._secret.encode() if self._secret else b"default",
            msg,
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(expected, signature):
            self._record_audit("auth_rejected_bad_signature", sender)
            self._auth_failures += 1
            return False

        self._record_audit("auth_success", sender)
        return True

    def create_session(self, peer_id: str, direction: str = "outbound") -> TransportSession:
        session_id = f"{self.local_id}:{peer_id}:{now_ts()}"
        session = TransportSession(session_id=session_id, peer_id=peer_id, direction=direction)
        session.authenticated = self.is_trusted(peer_id)
        with self._lock:
            self._sessions[session_id] = session
        return session

    def close_session(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)

    def record_message(self, session_id: str, sent: bool, size_bytes: int) -> None:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return
            session.last_active = now_ts()
            if sent:
                session.messages_sent += 1
                session.bytes_sent += size_bytes
            else:
                session.messages_received += 1
                session.bytes_received += size_bytes

    def _record_audit(self, event: str, peer: str, details: Dict[str, Any] | None = None) -> None:
        entry = {"ts": now_ts(), "event": event, "peer": peer}
        if details:
            entry["details"] = details
        with self._lock:
            self._audit_log.append(entry)
            if len(self._audit_log) > 500:
                self._audit_log = self._audit_log[-500:]

    def audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self._audit_log))[:limit]

    def active_sessions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [s.to_dict() for s in self._sessions.values()]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            total_sent = sum(s.bytes_sent for s in self._sessions.values())
            total_received = sum(s.bytes_received for s in self._sessions.values())
            return {
                "ts": now_ts(),
                "local_id": self.local_id,
                "active_sessions": len(self._sessions),
                "trusted_peers": len(self._trusted_peers),
                "total_bytes_sent": total_sent,
                "total_bytes_received": total_received,
                "auth_failures": self._auth_failures,
                "audit_entries": len(self._audit_log),
            }
