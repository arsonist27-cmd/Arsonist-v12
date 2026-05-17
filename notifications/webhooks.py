from __future__ import annotations

from notifications.alerts import enqueue_webhook


def deliver(org_id: str, event: str, data: dict) -> None:
    enqueue_webhook({"org_id": org_id, "event": event, "data": data})
