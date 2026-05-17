from __future__ import annotations

from typing import Any, Dict


class NamespaceManager:
    """Logical namespaces for control-plane registry keys and deployment IDs."""

    @staticmethod
    def registry_prefix(org_id: str) -> str:
        return f"v12:org:{org_id}:"

    @staticmethod
    def deployment_key(org_id: str, name: str) -> str:
        return f"{NamespaceManager.registry_prefix(org_id)}deploy:{name}"

    @staticmethod
    def parse_registry_key(full_key: str) -> Dict[str, Any] | None:
        if not full_key.startswith("v12:org:"):
            return None
        parts = full_key.split(":", 3)
        if len(parts) < 4:
            return None
        return {"org_id": parts[2], "remainder": parts[3] if len(parts) > 3 else ""}


NS = NamespaceManager()
