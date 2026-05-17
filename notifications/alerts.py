from __future__ import annotations

import os
import threading
from queue import Queue
from typing import Any, Dict

from audit.audit_log import append_audit

_q: Queue[Dict[str, Any]] | None = None
_worker_started = False


def _ensure_worker() -> None:
    global _worker_started, _q
    if _worker_started:
        return
    _worker_started = True
    _q = Queue(maxsize=500)

    def run() -> None:
        import requests

        while True:
            item = _q.get()
            try:
                url = item.get("url") or os.getenv("ARSONIST_V12_WEBHOOK_URL", "")
                if not url:
                    continue
                requests.post(url, json=item.get("payload"), timeout=3)
            except Exception:
                append_audit(type="webhook_failure", payload=item)
            finally:
                _q.task_done()

    threading.Thread(target=run, daemon=True, name="v12-webhooks").start()


def enqueue_webhook(payload: Dict[str, Any], url: str | None = None) -> None:
    _ensure_worker()
    assert _q is not None
    try:
        _q.put_nowait({"url": url, "payload": payload})
    except Exception:
        pass


def alert(level: str, title: str, body: str, org_id: str | None = None) -> None:
    append_audit(type="alert", level=level, title=title, body=body, org_id=org_id)
    enqueue_webhook({"level": level, "title": title, "body": body, "org_id": org_id})
