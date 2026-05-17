"""
Multi-tenant simulation: bootstrap N orgs, issue API keys, exercise /v12 + gateway.

Run (from repo root, with deps installed):
  set ARSONIST_V12_MULTITENANT=1
  set ARSONIST_JWT_SECRET=your-32+-char-secret
  set ARSONIST_V12_BOOTSTRAP_TOKEN=bootstrap-demo
  python tests/multi_tenant_sim.py

Or on Unix:
  ARSONIST_V12_MULTITENANT=1 ARSONIST_JWT_SECRET=... ARSONIST_V12_BOOTSTRAP_TOKEN=... python tests/multi_tenant_sim.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _env() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.environ.setdefault("ARSONIST_V12_MULTITENANT", "1")
    os.environ.setdefault("ARSONIST_JWT_SECRET", "unit-test-jwt-secret-key-please-change-32")
    os.environ.setdefault("ARSONIST_V12_BOOTSTRAP_TOKEN", "bootstrap-sim-token")
    os.environ.setdefault("ARSONIST_API_TOKEN", "")


_env()

from fastapi.testclient import TestClient  # noqa: E402

from control_plane.app import app  # noqa: E402
from identity.registry_state import STATE  # noqa: E402


def main() -> int:
    client = TestClient(app)
    boot = os.environ["ARSONIST_V12_BOOTSTRAP_TOKEN"]
    orgs = []
    for i in range(12):
        r = client.post(
            "/v12/orgs",
            headers={"Authorization": f"Bearer {boot}"},
            json={
                "name": f"Sim Org {i}",
                "slug": f"sim-org-{i}",
                "owner_email": f"owner{i}@sim.local",
                "owner_password": "pw-sim-123",
                "owner_name": f"Owner {i}",
            },
        )
        if r.status_code != 200:
            print("bootstrap failed", i, r.status_code, r.text)
            return 1
        orgs.append(r.json())

    print("bootstrapped", len(orgs), "orgs")

    # Token auth through gateway on /v12 usage
    for rec in orgs[:3]:
        org = rec["organization"]
        sk = rec["api_key"]["secret"]
        u = client.get(f"/v12/orgs/{org['org_id']}/usage", headers={"Authorization": f"Bearer {sk}"})
        print(org["slug"], "usage_status", u.status_code)

    # Session JWT for user-scoped route
    o0 = orgs[0]["organization"]
    r = client.post(
        "/v12/session",
        json={"email": "owner0@sim.local", "password": "pw-sim-123", "org_id": o0["org_id"]},
    )
    if r.status_code != 200:
        print("session", r.status_code, r.text)
        return 1
    jwt = r.json()["access_token"]
    inv = client.post(
        f"/v12/orgs/{o0['org_id']}/invoices",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"period": "sim", "prices": {"tokens": 0.00001}},
    )
    print("invoice", inv.status_code, "keys", list(inv.json().keys()) if inv.status_code < 400 else inv.text)

    aud = client.get(
        f"/v12/orgs/{o0['org_id']}/audit",
        headers={"Authorization": f"Bearer {jwt}"},
    )
    print("audit_events", len(aud.json().get("events", [])) if aud.status_code < 400 else aud.text)

    print("identity rows orgs=", len(STATE.orgs), "users=", len(STATE.users))
    return 0


if __name__ == "__main__":
    sys.exit(main())
