from __future__ import annotations

from typing import Any, Dict

import requests
from flask import Flask, jsonify, request


def register(app: Flask, control_url: str, control_headers: Dict[str, str]) -> None:
    base = control_url.rstrip("/")

    @app.post("/api/v12/orgs/<org_id>/invoices")
    def create_invoice(org_id: str) -> Any:
        auth = request.headers.get("X-Arsonist-Authorization", "")
        h = dict(control_headers)
        if auth:
            h["Authorization"] = auth
        try:
            r = requests.post(
                f"{base}/v12/orgs/{org_id}/invoices",
                json=request.get_json(silent=True) or {},
                headers=h,
                timeout=6,
            )
            return jsonify(r.json() if r.ok else {"error": r.text, "status": r.status_code})
        except requests.RequestException as exc:
            return jsonify({"error": str(exc)})
