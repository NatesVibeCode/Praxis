"""OAuth2 token lifecycle management.

Stores, checks, and refreshes OAuth2 tokens in Postgres.
Extends the credential resolution chain with token-store-first lookup.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


class OAuthTokenError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class OAuthToken:
    integration_id: str
    access_token: str
    refresh_token: str | None
    expires_at: datetime | None
    scopes: tuple[str, ...]
    token_type: str


def resolve_or_refresh(
    conn: Any,
    integration_id: str,
    *,
    auth_shape: dict[str, Any] | None = None,
    buffer_seconds: int = 300,
) -> OAuthToken:
    """Return a valid token, refreshing if expired.

    Raises OAuthTokenError if no token exists or refresh fails.
    """
    if not integration_id:
        raise OAuthTokenError("oauth.no_integration_id", "integration_id is required")

    rows = conn.execute(
        """SELECT access_token, refresh_token, expires_at,
                  scopes, token_type
             FROM credential_tokens
            WHERE integration_id = $1 AND token_kind = 'access'
            LIMIT 1""",
        integration_id,
    )

    if not rows:
        raise OAuthTokenError(
            "oauth.no_token",
            f"no stored token for integration {integration_id}",
        )

    row = rows[0]
    token = OAuthToken(
        integration_id=integration_id,
        access_token=row["access_token"],
        refresh_token=row.get("refresh_token"),
        expires_at=row.get("expires_at"),
        scopes=tuple(row.get("scopes") or []),
        token_type=row.get("token_type") or "Bearer",
    )

    if not _is_expired(token.expires_at, buffer_seconds):
        return token

    # Token is expired — try to refresh
    if not token.refresh_token:
        raise OAuthTokenError(
            "oauth.no_refresh_token",
            f"token expired and no refresh_token for {integration_id}",
        )

    if not auth_shape:
        raise OAuthTokenError(
            "oauth.no_auth_shape",
            f"token expired but no auth_shape provided for refresh of {integration_id}",
        )

    token_url = str(auth_shape.get("token_url", "")).strip()
    if not token_url:
        raise OAuthTokenError(
            "oauth.no_token_url",
            f"token expired but no token_url in auth_shape for {integration_id}",
        )
    if not token_url.startswith("https://"):
        raise OAuthTokenError(
            "oauth.insecure_token_url",
            f"token_url must use HTTPS for {integration_id}: {token_url!r}",
        )

    env_prefix = integration_id.upper().replace("-", "_")
    client_id = os.environ.get(f"{env_prefix}_CLIENT_ID", "")
    client_secret = os.environ.get(f"{env_prefix}_CLIENT_SECRET", "")

    new_tokens = _refresh_token(token_url, client_id, client_secret, token.refresh_token)

    new_access = new_tokens.get("access_token", "")
    if not new_access:
        raise OAuthTokenError(
            "oauth.refresh_failed",
            f"refresh response missing access_token for {integration_id}",
        )

    new_expires_at = token.expires_at  # preserve previous if conversion fails
    expires_in = new_tokens.get("expires_in")
    if expires_in is not None:
        try:
            clamped = max(0, min(int(expires_in), 31_536_000))  # cap at 1 year
            new_expires_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=clamped)
        except (ValueError, TypeError):
            pass

    new_refresh = new_tokens.get("refresh_token", token.refresh_token)
    new_type = new_tokens.get("token_type", token.token_type)

    store_token(
        conn,
        integration_id,
        access_token=new_access,
        refresh_token=new_refresh,
        expires_at=new_expires_at,
        scopes=token.scopes,
        token_type=new_type,
    )

    return OAuthToken(
        integration_id=integration_id,
        access_token=new_access,
        refresh_token=new_refresh,
        expires_at=new_expires_at,
        scopes=token.scopes,
        token_type=new_type,
    )


def store_token(
    conn: Any,
    integration_id: str,
    *,
    access_token: str,
    refresh_token: str | None = None,
    expires_at: datetime | None = None,
    scopes: tuple[str, ...] = (),
    token_type: str = "Bearer",
) -> None:
    """Upsert an access token into the token store."""
    conn.execute(
        """INSERT INTO credential_tokens
               (integration_id, token_kind, access_token, refresh_token,
                expires_at, scopes, token_type, updated_at)
           VALUES ($1, 'access', $2, $3, $4, $5, $6, now())
           ON CONFLICT (integration_id, token_kind) DO UPDATE SET
               access_token = EXCLUDED.access_token,
               refresh_token = EXCLUDED.refresh_token,
               expires_at = EXCLUDED.expires_at,
               scopes = EXCLUDED.scopes,
               token_type = EXCLUDED.token_type,
               updated_at = now()""",
        integration_id,
        access_token,
        refresh_token,
        expires_at,
        list(scopes),
        token_type,
    )


def _refresh_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict[str, Any]:
    """POST to the OAuth2 token endpoint to refresh an access token."""
    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode()

    req = urllib.request.Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    _MAX_RESPONSE_BYTES = 64 * 1024  # 64 KB
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read(_MAX_RESPONSE_BYTES).decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        resp_body = ""
        try:
            resp_body = e.read().decode("utf-8", errors="replace")[:1000]
        except Exception:
            pass
        raise OAuthTokenError(
            "oauth.refresh_http_error",
            f"token refresh failed: HTTP {e.code} — {resp_body}",
        ) from e
    except Exception as exc:
        raise OAuthTokenError(
            "oauth.refresh_exception",
            f"token refresh failed: {exc}",
        ) from exc


def _is_expired(expires_at: datetime | None, buffer_seconds: int) -> bool:
    """Check if a token is expired (or will be within buffer_seconds)."""
    if expires_at is None:
        return False
    now = datetime.now(timezone.utc)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return now >= (expires_at - timedelta(seconds=buffer_seconds))
