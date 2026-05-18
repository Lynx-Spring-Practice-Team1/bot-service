"""Internal-token authentication for admin endpoints.

Mirrors the require_internal_token pattern used in auth-service, order-service,
wallet-service, etc. The shared INTERNAL_SERVICE_TOKEN is set as an env var on
every backend service; admin-panel forwards it as an X-Internal-Token header
on each admin REST call.
"""
from __future__ import annotations

from fastapi import Header, HTTPException, status

from app.config import settings


def require_internal_token(
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
) -> None:
    if x_internal_token != settings.INTERNAL_SERVICE_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing internal service token",
        )


def get_admin_user(
    x_admin_user: str | None = Header(default=None, alias="X-Admin-User"),
) -> str | None:
    """Optional audit-trail header set by admin-panel proxy."""
    return x_admin_user
