from __future__ import annotations

from typing import Any, Dict

import requests
from flask import Flask, jsonify, request


def register(app: Flask, control_url: str, control_headers: Dict[str, str]) -> None:
    base = control_url.rstrip("/")

    @app.get("/api/v12/orgs/<org_id>/summary")
    def org_summary(org_id: str) -> Any:
        auth = request.headers.get("X-Arsonist-Authorization", "")
        h = dict(control_headers)
        if auth:
            h["Authorization"] = auth
        out: Dict[str, Any] = {}
        for key, path in (
            ("org", f"/v12/orgs/{org_id}"),
            ("usage", f"/v12/orgs/{org_id}/usage"),
        ):
            try:
                r = requests.get(f"{base}{path}", headers=h, timeout=4)
                out[key] = r.json() if r.ok else {"error": r.text, "status": r.status_code}
            except requests.RequestException as exc:
                out[key] = {"error": str(exc)}
        return jsonify(out)
