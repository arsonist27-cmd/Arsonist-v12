from __future__ import annotations

from typing import FrozenSet

from identity.roles import Role

# Fine-grained permissions for SaaS control plane + inference.
INFERENCE_CALL = "inference:call"
ORG_MANAGE = "org:manage"
USER_MANAGE = "user:manage"
BILLING_READ = "billing:read"
BILLING_MANAGE = "billing:manage"
API_KEY_ROTATE = "api_key:rotate"
DEPLOY_READ = "deploy:read"
DEPLOY_MANAGE = "deploy:manage"
AUDIT_READ = "audit:read"

_ROLE_PERMS: dict[Role, FrozenSet[str]] = {
    Role.admin: frozenset(
        {
            INFERENCE_CALL,
            ORG_MANAGE,
            USER_MANAGE,
            BILLING_READ,
            BILLING_MANAGE,
            API_KEY_ROTATE,
            DEPLOY_READ,
            DEPLOY_MANAGE,
            AUDIT_READ,
        }
    ),
    Role.dev: frozenset({INFERENCE_CALL, DEPLOY_READ, DEPLOY_MANAGE}),
    Role.billing: frozenset({BILLING_READ, BILLING_MANAGE, INFERENCE_CALL}),
    Role.viewer: frozenset({DEPLOY_READ, BILLING_READ}),
}


def permissions_for(role: Role) -> FrozenSet[str]:
    return _ROLE_PERMS.get(role, frozenset())


def has_permission(role: Role, permission: str) -> bool:
    return permission in permissions_for(role)
