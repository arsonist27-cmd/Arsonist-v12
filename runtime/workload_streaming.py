"""v15 Workload Streaming.

Manages streaming inference workloads with distributed token generation,
progressive output delivery, and backpressure handling for planet-scale
AI operations.
"""
from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.utils import now_ts, setup_logging

logger = setup_logging("runtime.workload_streaming")


class StreamState(str, Enum):
    initializing = "initializing"
    active = "active"
    paused = "paused"
    backpressure = "backpressure"
    completed = "completed"
    failed = "failed"


class StreamingSession(BaseModel):
    session_id: str
    workload_id: str = ""
    region: str = ""
    state: StreamState = StreamState.initializing
    tokens_generated: int = 0
    tokens_per_second: float = 0.0
    chunks_sent: int = 0
    bytes_sent: int = 0
    latency_first_token_ms: float = 0.0
    latency_inter_token_ms: float = 0.0
    backpressure_events: int = 0
    created_at: float = 0.0
    completed_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StreamingManager:
    """Manages streaming inference sessions with backpressure handling,
    distributed token generation, and progressive output delivery."""

    def __init__(self, max_concurrent: int = 500000, max_history: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_concurrent = max_concurrent
        self._max_history = max_history
        self._sessions: Dict[str, StreamingSession] = {}
        self._completed: List[StreamingSession] = []
        self._total_created = 0
        self._total_completed = 0
        self._total_tokens = 0
        self._events: List[Dict[str, Any]] = []

    def create_session(self, session: StreamingSession) -> bool:
        with self._lock:
            if len(self._sessions) >= self._max_concurrent:
                return False
            session.created_at = now_ts()
            session.state = StreamState.initializing
            self._sessions[session.session_id] = session
            self._total_created += 1
            return True

    def activate(self, session_id: str, first_token_latency_ms: float = 0.0) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False
            session.state = StreamState.active
            session.latency_first_token_ms = first_token_latency_ms
            return True

    def stream_tokens(self, session_id: str, tokens: int, tps: float,
                      inter_token_ms: float = 0.0, bytes_sent: int = 0) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session or session.state not in (StreamState.active, StreamState.backpressure):
                return False
            session.tokens_generated += tokens
            session.tokens_per_second = tps
            session.chunks_sent += 1
            session.bytes_sent += bytes_sent
            if inter_token_ms > 0:
                session.latency_inter_token_ms = inter_token_ms
            self._total_tokens += tokens
            return True

    def apply_backpressure(self, session_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False
            session.state = StreamState.backpressure
            session.backpressure_events += 1
            return True

    def resume(self, session_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session or session.state != StreamState.backpressure:
                return False
            session.state = StreamState.active
            return True

    def complete_session(self, session_id: str, success: bool = True) -> Optional[StreamingSession]:
        with self._lock:
            session = self._sessions.pop(session_id, None)
            if not session:
                return None
            session.completed_at = now_ts()
            session.state = StreamState.completed if success else StreamState.failed
            self._total_completed += 1
            self._completed.append(session)
            if len(self._completed) > self._max_history:
                self._completed = self._completed[-self._max_history:]
            return session

    def active_sessions(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            sessions = [s for s in self._sessions.values() if s.state in (StreamState.active, StreamState.backpressure)]
            return [s.model_dump(mode="json") for s in sessions[:limit]]

    def recent_completed(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [s.model_dump(mode="json") for s in reversed(self._completed)][:limit]

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            active = sum(1 for s in self._sessions.values() if s.state == StreamState.active)
            backpressured = sum(1 for s in self._sessions.values() if s.state == StreamState.backpressure)
            recent = self._completed[-100:] if self._completed else []
            avg_first_token = sum(s.latency_first_token_ms for s in recent) / len(recent) if recent else 0.0
            avg_tps = sum(s.tokens_per_second for s in recent) / len(recent) if recent else 0.0
            return {
                "ts": now_ts(),
                "total_created": self._total_created,
                "total_completed": self._total_completed,
                "total_tokens": self._total_tokens,
                "active": active,
                "backpressured": backpressured,
                "avg_first_token_ms": round(avg_first_token, 2),
                "avg_tps": round(avg_tps, 1),
            }
