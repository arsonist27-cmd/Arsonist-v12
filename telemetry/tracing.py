from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Dict

_TRACE = ContextVar("arsonist_inference_trace", default="")


def new_inference_trace_id() -> str:
    tid = str(uuid.uuid4())
    _TRACE.set(tid)
    return tid


def current_inference_trace() -> str:
    return _TRACE.get() or new_inference_trace_id()


def trace_headers() -> Dict[str, str]:
    return {"X-Arsonist-Inference-Trace": current_inference_trace()}
