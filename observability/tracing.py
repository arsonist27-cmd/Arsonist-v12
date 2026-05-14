from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Dict

_TRACE: ContextVar[str] = ContextVar("arsonist_trace_id", default="")


def new_trace_id() -> str:
    tid = str(uuid.uuid4())
    _TRACE.set(tid)
    return tid


def current_trace_id() -> str:
    return _TRACE.get() or new_trace_id()


def trace_headers() -> Dict[str, str]:
    return {"X-Arsonist-Trace-Id": current_trace_id()}
