from __future__ import annotations

from fastapi import Request

from billing.usage_tracking import TRACKER


def _org(request: Request) -> str | None:
    ctx = getattr(request.state, "v12_tenant", None)
    if not ctx:
        return None
    return ctx.org_id


def maybe_record_v12_chat(request: Request, model: str, latency_ms: float, content: str) -> None:
    org_id = _org(request)
    if not org_id:
        return
    from gateway.quota_manager import consume_tokens

    est_tokens = max(1.0, len(content) / 4.0)
    ok, _ = consume_tokens(org_id, est_tokens)
    if not ok:
        return
    TRACKER.record(
        org_id,
        metric="tokens",
        amount=est_tokens,
        unit="tokens",
        meta={"model": model, "kind": "chat"},
    )
    TRACKER.record(
        org_id,
        metric="gpu_proxy_ms",
        amount=latency_ms,
        unit="ms",
        meta={"model": model},
    )


def maybe_record_v12_embed(request: Request, model: str, latency_ms: float, dim: int) -> None:
    org_id = _org(request)
    if not org_id:
        return
    from gateway.quota_manager import consume_tokens

    est = max(1.0, dim / 8.0)
    ok, _ = consume_tokens(org_id, est)
    if not ok:
        return
    TRACKER.record(org_id, metric="tokens", amount=est, unit="tokens", meta={"model": model, "kind": "embed"})


def maybe_record_v12_generate(request: Request, model: str, latency_ms: float, text: str) -> None:
    org_id = _org(request)
    if not org_id:
        return
    from gateway.quota_manager import consume_tokens

    est = max(1.0, len(text) / 4.0)
    ok, _ = consume_tokens(org_id, est)
    if not ok:
        return
    TRACKER.record(org_id, metric="tokens", amount=est, unit="tokens", meta={"model": model, "kind": "generate"})
