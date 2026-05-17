from __future__ import annotations

from typing import Tuple


def namespaced_deployment_id(org_id: str, deployment_id: str) -> str:
    return f"{org_id}__{deployment_id}"


def parse_namespaced_id(namespaced: str) -> Tuple[str, str]:
    if "__" in namespaced:
        org, rest = namespaced.split("__", 1)
        return org, rest
    return "", namespaced
