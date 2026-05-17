from __future__ import annotations

from typing import Any, Dict

import requests
from flask import Flask, jsonify, request


def register(app: Flask, control_url: str, control_headers: Dict[str, str]) -> None:
    """Admin-style proxies for v12 audit (requires user JWT in X-Arsonist-Authorization)."""

    base = control_url.rstrip("/")

    @app.get("/api/v12/orgs/<org_id>/audit")
    def audit_tail(org_id: str) -> Any:
        auth = request.headers.get("X-Arsonist-Authorization", "")
        h = dict(control_headers)
        if auth:
            h["Authorization"] = auth
        try:
            r = requests.get(f"{base}/v12/orgs/{org_id}/audit", headers=h, timeout=4)
            return jsonify(r.json() if r.ok else {"error": r.text, "status": r.status_code})
        except requests.RequestException as exc:
            return jsonify({"error": str(exc)})
