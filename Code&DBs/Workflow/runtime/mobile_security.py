"""Mobile HTTP security helpers.

These helpers keep cookie flags and cache policy as code-level contracts. The
API can import them without pulling in DB or WebAuthn dependencies.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

AUTH_COOKIE_NAME = "praxis_mobile_session"
NO_STORE_CACHE_CONTROL = "no-store, no-cache, must-revalidate, private"


def set_mobile_session_cookie(
    response: Any,
    *,
    session_token: str,
    expires_at: datetime | None = None,
) -> None:
    """Set the mobile session cookie with the only accepted flag shape."""

    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=session_token,
        httponly=True,
        secure=True,
        samesite="strict",
        expires=None if expires_at is None else expires_at,
        path="/",
    )


def apply_no_store_headers(response: Any) -> Any:
    """Apply no-store headers to sensitive mobile/API responses."""

    response.headers["Cache-Control"] = NO_STORE_CACHE_CONTROL
    response.headers.setdefault("Pragma", "no-cache")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    return response


def no_store_headers() -> dict[str, str]:
    return {
        "Cache-Control": NO_STORE_CACHE_CONTROL,
        "Pragma": "no-cache",
        "X-Content-Type-Options": "nosniff",
    }


__all__ = [
    "AUTH_COOKIE_NAME",
    "NO_STORE_CACHE_CONTROL",
    "apply_no_store_headers",
    "no_store_headers",
    "set_mobile_session_cookie",
]
