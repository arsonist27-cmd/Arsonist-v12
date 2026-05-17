"""v12 tenant-scoped deployments (wraps v11 deployment IDs)."""

from deployments.tenant_deployments import namespaced_deployment_id, parse_namespaced_id

__all__ = ["namespaced_deployment_id", "parse_namespaced_id"]
