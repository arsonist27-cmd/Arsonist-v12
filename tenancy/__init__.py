"""v12 tenancy: routing, isolation, namespaces."""

from tenancy.tenant_router import TenantContext, resolve_tenant_header

__all__ = ["TenantContext", "resolve_tenant_header"]
