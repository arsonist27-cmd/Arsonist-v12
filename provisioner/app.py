from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="Arsonist Provisioner Stub")
TOKEN = os.getenv("ARSONIST_PROVISIONER_TOKEN", "")
BASE_PORT = int(os.getenv("PROVISIONER_BASE_PORT", "9200"))
_counter = 0


def _auth(authorization: str | None) -> None:
    if not TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    if authorization.removeprefix("Bearer ").strip() != TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/provision")
def provision(payload: Dict[str, Any], authorization: str | None = Header(default=None)) -> Dict[str, Any]:
    global _counter
    _auth(authorization)
    _counter += 1
    node_type = str(payload.get("node_type", "CPU")).upper()
    return {
        "node_id": f"prov-{_counter}",
        "host": "127.0.0.1",
        "port": BASE_PORT + _counter,
        "node_type": node_type,
        "has_gpu": node_type == "GPU",
    }
