from __future__ import annotations

import os

import requests

from federation.federation_security import build_headers
from shared.models import JobRecord
from shared.utils import setup_logging

logger = setup_logging("control.federation_callbacks")


def maybe_report_global_completion(job: JobRecord) -> None:
    if not job.report_to_federation:
        return
    base = os.getenv("ARSONIST_FEDERATION_URL", "").rstrip("/")
    if not base:
        return
    token = os.getenv("ARSONIST_FEDERATION_TOKEN", "")
    cid = os.getenv("ARSONIST_CLUSTER_ID", "")
    payload = {
        "job_id": job.id,
        "cluster_id": cid,
        "ok": job.status == "completed",
        "result": job.result or {},
    }
    headers = dict(build_headers(payload))
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        requests.post(f"{base}/global_job_complete", json=payload, headers=headers, timeout=5.0)
    except requests.RequestException as exc:
        logger.warning("federation completion notify failed: %s", exc)
