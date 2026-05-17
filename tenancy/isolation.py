from __future__ import annotations

from typing import Any, Dict


def workload_labels(org_id: str, base: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Kubernetes-style isolation labels for schedulers (v11 hooks consume as metadata)."""
    out = dict(base or {})
    out["arsonist.io/org-id"] = org_id
    out["arsonist.io/tenant-isolation"] = "strict"
    return out


def namespaced_model_id(org_id: str, model_id: str) -> str:
    return f"{org_id}::{model_id}"


def strip_namespace(namespaced: str) -> tuple[str, str]:
    if "::" in namespaced:
        a, b = namespaced.split("::", 1)
        return a, b
    return "", namespaced
