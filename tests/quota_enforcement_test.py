"""Quota / rate smoke tests for v12 (run directly: python tests/quota_enforcement_test.py)."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _env() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.environ["ARSONIST_V12_MULTITENANT"] = "1"
    os.environ["ARSONIST_JWT_SECRET"] = "unit-test-jwt-secret-key-please-change-32"
    os.environ["ARSONIST_V12_BOOTSTRAP_TOKEN"] = "bootstrap-quota-test"
    os.environ["ARSONIST_API_TOKEN"] = ""


_env()

from fastapi.testclient import TestClient  # noqa: E402

from billing.subscription_manager import SUBSCRIPTIONS  # noqa: E402
from control_plane.app import app  # noqa: E402
from gateway.quota_manager import consume_tokens  # noqa: E402


def main() -> int:
    c = TestClient(app)
    boot = os.environ["ARSONIST_V12_BOOTSTRAP_TOKEN"]
    r = c.post(
        "/v12/orgs",
        headers={"Authorization": f"Bearer {boot}"},
        json={
            "name": "Quota Co",
            "slug": "quota-co",
            "owner_email": "q@quota.local",
            "owner_password": "pw",
            "owner_name": "Q",
        },
    )
    if r.status_code != 200:
        print(r.status_code, r.text)
        return 1
    org_id = r.json()["organization"]["org_id"]
    SUBSCRIPTIONS.set_custom_limits(org_id, {"tokens_per_minute": 100.0})

    ok, _ = consume_tokens(org_id, 99.0)
    assert ok
    ok2, reason = consume_tokens(org_id, 50.0)
    assert not ok2 and reason == "tokens_per_minute"
    print("token quota enforced:", reason)

    # Rate limit smoke (in-process deque): many /v12/orgs/{id} with same key
    sk = r.json()["api_key"]["secret"]
    SUBSCRIPTIONS.set_custom_limits(org_id, {"requests_per_sec": 3.0})
    hits = 0
    blocked = 0
    for _ in range(8):
        resp = c.get(f"/v12/orgs/{org_id}/usage", headers={"Authorization": f"Bearer {sk}"})
        if resp.status_code == 429:
            blocked += 1
        else:
            hits += 1
    print("rate_limit_429s", blocked, "ok", hits)
    return 0 if blocked else 1


if __name__ == "__main__":
    sys.exit(main())
