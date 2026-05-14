from __future__ import annotations

import os
from typing import Any, Dict

import requests
from flask import Flask, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)
CONTROL_URL = os.getenv("CONTROL_PLANE_URL", "http://127.0.0.1:8000")
CONTROL_API_TOKEN = os.getenv("CONTROL_API_TOKEN", "")
FEDERATION_URL = os.getenv("ARSONIST_FEDERATION_DASHBOARD_URL", os.getenv("FEDERATION_URL", "")).rstrip("/")
FEDERATION_TOKEN = os.getenv("ARSONIST_FEDERATION_DASHBOARD_TOKEN", os.getenv("FEDERATION_API_TOKEN", ""))


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {CONTROL_API_TOKEN}"} if CONTROL_API_TOKEN else {}


def _fed_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {FEDERATION_TOKEN}"} if FEDERATION_TOKEN else {}


def _get(path: str) -> Dict[str, Any]:
    try:
        resp = requests.get(f"{CONTROL_URL}{path}", headers=_headers(), timeout=3)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        return {"error": str(exc)}


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/federation")
def federation_overview() -> Dict[str, Any]:
    if not FEDERATION_URL:
        return {"enabled": False}
    gh = _fed_get("/global_health")
    clusters = _fed_get("/clusters")
    fm = _fed_get("/federation_metrics")
    rm = _fed_get("/routing_metrics")
    cm = _fed_get("/cluster_metrics")
    return {
        "enabled": True,
        "global_health": gh,
        "clusters": clusters,
        "federation_metrics": fm,
        "routing_metrics": rm,
        "cluster_metrics": cm,
    }


def _fed_get(path: str) -> Dict[str, Any]:
    try:
        resp = requests.get(f"{FEDERATION_URL}{path}", headers=_fed_headers(), timeout=4)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        return {"error": str(exc)}


@app.get("/api/mesh")
def mesh_view() -> Any:
    out: Dict[str, Any] = {"control_plane": CONTROL_URL}
    for key, path in (
        ("mesh_health", "/mesh_health"),
        ("mesh_metrics", "/mesh_metrics"),
        ("mesh_routes", "/mesh_routes"),
        ("mesh_peers", "/mesh/peers"),
        ("mesh_events", "/mesh/events?limit=40"),
    ):
        try:
            resp = requests.get(f"{CONTROL_URL}{path}", headers=_headers(), timeout=3)
            out[key] = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text}
        except requests.RequestException as exc:
            out[key] = {"error": str(exc)}
    return jsonify(out)


@app.get("/api/v11")
def v11_view() -> Any:
    out: Dict[str, Any] = {"control_plane": CONTROL_URL}
    for key, path in (
        ("inference_metrics", "/inference_metrics"),
        ("gpu_metrics", "/gpu_metrics"),
        ("deployment_metrics", "/deployment_metrics"),
    ):
        try:
            resp = requests.get(f"{CONTROL_URL}{path}", headers=_headers(), timeout=3)
            out[key] = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text}
        except requests.RequestException as exc:
            out[key] = {"error": str(exc)}
    return jsonify(out)
def cluster() -> Dict[str, Any]:
    nodes = _get("/nodes")
    jobs = _get("/jobs")
    health = _get("/health")
    metrics = _get("/metrics")
    status = _get("/cluster/status")
    return jsonify({"nodes": nodes, "jobs": jobs, "health": health, "metrics": metrics, "status": status})


@app.post("/submit")
def submit() -> Any:
    payload = {
        "type": request.form.get("type", "code"),
        "task": request.form.get("task", "print('hello from arsonist')"),
        "required_nodes": int(request.form.get("required_nodes", "1")),
        "power": request.form.get("power", "low"),
        "gpu_required": request.form.get("gpu_required") == "on",
    }
    try:
        requests.post(f"{CONTROL_URL}/submit_job", json=payload, headers=_headers(), timeout=4).raise_for_status()
    except requests.RequestException:
        pass
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7000, debug=False)
