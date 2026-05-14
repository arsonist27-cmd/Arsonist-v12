from __future__ import annotations

import gzip
import json
from typing import Any, Dict, List, Tuple

from distributed_queue.event_log import MeshEvent
from distributed_queue.replicated_queue import ReplicatedJobState, ReplicatedQueue


def serialize_events(events: List[MeshEvent], compress: bool = False) -> Tuple[bytes, str]:
    """Returns (body, content_encoding)."""
    raw = json.dumps([e.model_dump() for e in events], separators=(",", ":")).encode("utf-8")
    if compress:
        return gzip.compress(raw, compresslevel=6), "gzip"
    return raw, "identity"


def deserialize_events(body: bytes, content_encoding: str) -> List[Dict[str, Any]]:
    if content_encoding == "gzip":
        body = gzip.decompress(body)
    data = json.loads(body.decode("utf-8"))
    return list(data) if isinstance(data, list) else []


def build_queue_delta(rq: ReplicatedQueue, since_job_id: str | None = None) -> Dict[str, Any]:
    states = rq.list_states()
    if since_job_id:
        states = [s for s in states if s.job_id > since_job_id]
    return {"jobs": [s.model_dump() for s in states[:200]]}


def apply_queue_delta(rq: ReplicatedQueue, jobs: List[Dict[str, Any]]) -> int:
    n = 0
    for j in jobs:
        try:
            st = ReplicatedJobState(**j)
        except Exception:
            continue
        rq.upsert(st)
        n += 1
    return n
