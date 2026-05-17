from __future__ import annotations

import os
import time
from typing import Callable

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from audit.audit_log import append_audit
from audit.security_events import SecurityEvent, log_security_event
from billing.invoices import INVOICES
from billing.subscription_manager import SUBSCRIPTIONS
from gateway.auth import V12AuthResult, authenticate_bearer
from gateway.quota_manager import check_concurrency, decr_concurrency, incr_concurrency
from gateway.rate_limit import allow_request
from identity.api_tokens import (
    create_org_api_token,
    list_org_tokens,
    revoke_org_api_token_for_org,
    verify_org_api_secret,
)
from identity.organizations import create_organization, get_organization
from identity.permissions import BILLING_READ, ORG_MANAGE, has_permission
from identity.roles import Role
from identity.sessions import decode_v12_subject, issue_user_jwt
from identity.users import (
    add_user_to_org,
    create_user,
    get_user_by_email,
    membership,
    verify_password,
)


def v12_enabled() -> bool:
    return os.getenv("ARSONIST_V12_MULTITENANT", "").lower() in ("1", "true", "yes")


def _require_org_caller(request: Request, authorization: str | None, org_id: str) -> dict:
    """User JWT, API-key JWT, or sk_ secret scoped to org_id."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    raw = authorization.removeprefix("Bearer ").strip()
    if raw.startswith("sk_"):
        rec = verify_org_api_secret(raw)
        if not rec or rec["org_id"] != org_id:
            raise HTTPException(status_code=403, detail="invalid org api key")
        return {"kind": "api_key", "org_id": org_id, "sub": rec["token_id"]}
    c = decode_v12_subject(raw)
    if not c or c.get("org_id") != org_id:
        raise HTTPException(status_code=403, detail="org scope mismatch")
    if c.get("scope") == "arsonist-user":
        return {"kind": "user", **c}
    if c.get("scope") == "arsonist-api-key":
        return {"kind": "api_key", "org_id": org_id, "sub": c["sub"]}
    raise HTTPException(status_code=403, detail="unsupported credential")


def _require_user_for_org(request: Request, authorization: str | None, org_id: str) -> dict:
    p = _require_org_caller(request, authorization, org_id)
    if p.get("kind") != "user":
        raise HTTPException(status_code=403, detail="user session required")
    return p


def _router() -> APIRouter:
    r = APIRouter(prefix="/v12", tags=["v12-saas"])

    @r.post("/orgs")
    def bootstrap_org(payload: dict, authorization: str | None = Header(default=None)) -> dict:
        tok = os.getenv("ARSONIST_V12_BOOTSTRAP_TOKEN", "").strip()
        if not tok or authorization != f"Bearer {tok}":
            raise HTTPException(status_code=403, detail="bootstrap forbidden")
        name = str(payload.get("name", "Acme"))
        slug = str(payload.get("slug", "acme"))
        org = create_organization(name, slug)
        email = str(payload.get("owner_email", "owner@example.com"))
        pw = str(payload.get("owner_password", "change-me-now"))
        user = create_user(email, str(payload.get("owner_name", "Owner")), pw)
        add_user_to_org(user.user_id, org.org_id, Role.admin)
        t = create_org_api_token(org.org_id, name="primary")
        append_audit(type="org_bootstrap", org_id=org.org_id, user_id=user.user_id)
        return {"organization": org.model_dump(), "owner_user_id": user.user_id, "api_key": t}

    @r.get("/orgs/{org_id}")
    def get_org(
        org_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict:
        _require_org_caller(request, authorization, org_id)
        org = get_organization(org_id)
        if not org:
            raise HTTPException(status_code=404, detail="unknown org")
        return {"organization": org.model_dump(), "subscription": SUBSCRIPTIONS.get_subscription(org_id)}

    @r.get("/orgs/{org_id}/usage")
    def usage(org_id: str, request: Request, authorization: str | None = Header(default=None)) -> dict:
        _require_org_caller(request, authorization, org_id)
        from billing.usage_tracking import TRACKER

        return {"totals": TRACKER.totals(org_id), "recent": TRACKER.recent(org_id, 50)}

    @r.post("/orgs/{org_id}/invoices")
    def invoice(
        org_id: str,
        payload: dict,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict:
        admin = _require_user_for_org(request, authorization, org_id)
        uid = admin.get("sub")
        role = membership(uid, org_id) if uid else None
        if not role or not has_permission(role, BILLING_READ):
            raise HTTPException(status_code=403, detail="billing role required")
        period = str(payload.get("period", "current"))
        prices = {k: float(v) for k, v in (payload.get("prices") or {}).items()}
        return INVOICES.from_usage_snapshot(org_id, period, prices)

    @r.get("/orgs/{org_id}/api_tokens")
    def tokens_list(org_id: str, request: Request, authorization: str | None = Header(default=None)) -> dict:
        admin = _require_user_for_org(request, authorization, org_id)
        uid = admin.get("sub")
        role = membership(uid, org_id) if uid else None
        if not role or not has_permission(role, ORG_MANAGE):
            raise HTTPException(status_code=403, detail="admin required")
        return {"tokens": list_org_tokens(org_id)}

    @r.post("/orgs/{org_id}/api_tokens")
    def tokens_create(org_id: str, request: Request, authorization: str | None = Header(default=None)) -> dict:
        admin = _require_user_for_org(request, authorization, org_id)
        uid = admin.get("sub")
        role = membership(uid, org_id) if uid else None
        if not role or not has_permission(role, ORG_MANAGE):
            raise HTTPException(status_code=403, detail="admin required")
        return create_org_api_token(org_id, name="manual")

    @r.delete("/orgs/{org_id}/api_tokens/{token_id}")
    def tokens_del(
        org_id: str,
        token_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict:
        admin = _require_user_for_org(request, authorization, org_id)
        uid = admin.get("sub")
        role = membership(uid, org_id) if uid else None
        if not role or not has_permission(role, ORG_MANAGE):
            raise HTTPException(status_code=403, detail="admin required")
        ok = revoke_org_api_token_for_org(org_id, token_id)
        if not ok:
            raise HTTPException(status_code=404, detail="token not found")
        return {"revoked": True}

    @r.post("/session")
    def session(payload: dict) -> dict:
        email = str(payload.get("email", ""))
        password = str(payload.get("password", ""))
        org_id = str(payload.get("org_id", ""))
        user = get_user_by_email(email)
        if not user or not verify_password(user, password):
            log_security_event(SecurityEvent.auth_failure, email=email)
            raise HTTPException(status_code=401, detail="invalid credentials")
        role = membership(user.user_id, org_id)
        if not role:
            raise HTTPException(status_code=403, detail="not a member of org")
        jwt = issue_user_jwt(user.user_id, org_id, role.value)
        append_audit(type="session", org_id=org_id, user_id=user.user_id)
        return {
            "access_token": jwt,
            "token_type": "bearer",
            "expires_in": int(os.getenv("ARSONIST_V12_JWT_TTL_SEC", "3600")),
        }

    @r.get("/orgs/{org_id}/audit")
    def audit_log(
        org_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict:
        _require_org_caller(request, authorization, org_id)
        from audit.audit_log import AUDIT

        return {"events": AUDIT.query(org_id=org_id, limit=200)}

    return r


class V12GatewayMiddleware(BaseHTTPMiddleware):
    """Tenant auth, rate limits, and concurrency for /v1 and /v12 (except bootstrap + session)."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        path = request.url.path
        if not (path.startswith("/v1") or path.startswith("/v12")):
            return await call_next(request)
        if request.method == "POST" and path.rstrip("/") == "/v12/orgs":
            return await call_next(request)
        if path.startswith("/v12/session"):
            return await call_next(request)

        auth_header = request.headers.get("authorization")
        try:
            res: V12AuthResult = authenticate_bearer(auth_header, path)
        except HTTPException as exc:
            log_security_event(SecurityEvent.auth_failure, path=path)
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

        if res.legacy_inference_ok:
            request.state.v12_legacy_inference_ok = True
            append_audit(type="gateway", path=path, legacy=True)
            return await call_next(request)

        assert res.tenant is not None
        tenant = res.tenant
        request.state.v12_tenant = tenant

        sub = SUBSCRIPTIONS.get_subscription(tenant.org_id)
        rps = float(sub["limits"].get("requests_per_sec", 10.0))
        ok_rl, n = allow_request(tenant.org_id, rps)
        if not ok_rl:
            log_security_event(SecurityEvent.rate_limited, org_id=tenant.org_id, n=n)
            return JSONResponse({"detail": "rate limit exceeded"}, status_code=429)

        ok_c, reason_c = check_concurrency(tenant.org_id)
        if not ok_c:
            log_security_event(SecurityEvent.quota_exceeded, org_id=tenant.org_id, reason=reason_c)
            return JSONResponse({"detail": "concurrency limit"}, status_code=429)

        incr_concurrency(tenant.org_id)
        t0 = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            decr_concurrency(tenant.org_id)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        append_audit(
            type="gateway",
            org_id=tenant.org_id,
            path=path,
            latency_ms=round(dt_ms, 3),
            status=getattr(response, "status_code", 0),
        )
        return response


def attach_v12_gateway(app: FastAPI, require_token: Callable[..., object]) -> None:
    if not v12_enabled():
        return
    if not os.getenv("ARSONIST_JWT_SECRET", "").strip():
        raise RuntimeError("ARSONIST_V12_MULTITENANT requires ARSONIST_JWT_SECRET for JWT issuance")
    app.add_middleware(V12GatewayMiddleware)
    app.include_router(_router())
