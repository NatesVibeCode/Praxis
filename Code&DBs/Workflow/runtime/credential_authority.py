"""Per-sandbox per-provider credential authority.

Standing decision ``architecture-policy::auth::credentials-per-sandbox-per-provider``
(2026-04-27) requires that sandbox provisioning resolve provider credentials
from a Praxis-side authority — not by forwarding ``CLAUDE_CODE_OAUTH_TOKEN``
or other host-shell env vars from whatever process happened to launch the
workflow. The launch context's identity and the runtime's identity are
different concerns.

This module is the resolver. It reads from ``credential_tokens`` (the same
table used by ``adapters.oauth_lifecycle`` for OAuth integrations) keyed by
``integration_id = f"provider:{provider_slug}"`` and ``token_kind`` ∈
``{api_key, access}``. The seed path is the operator-facing
``praxis credential onboard`` command, which writes to Keychain *and* to
``credential_tokens`` so the runtime can resolve credentials without
depending on host-shell env or Keychain access at sandbox-spawn time.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


logger = logging.getLogger(__name__)


_PROVIDER_INTEGRATION_PREFIX = "provider:"
_API_KEY_TOKEN_KIND = "api_key"


def _provider_integration_id(provider_slug: str) -> str:
    return f"{_PROVIDER_INTEGRATION_PREFIX}{provider_slug.strip().lower()}"


def _connection(conn: "SyncPostgresConnection | None" = None) -> "SyncPostgresConnection":
    if conn is not None:
        return conn
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    return SyncPostgresConnection(get_workflow_pool())


@dataclass(frozen=True, slots=True)
class ProviderCredential:
    """Resolved credential for one provider, scoped to one sandbox session."""

    provider_slug: str
    env_var_name: str
    value: str
    sandbox_session_id: str | None
    minted_at_epoch: float


def resolve_provider_credentials(
    *,
    provider_slug: str,
    sandbox_session_id: str | None = None,
    conn: "SyncPostgresConnection | None" = None,
) -> dict[str, str]:
    """Return env-var-name → value mappings for the given provider.

    Reads from ``credential_tokens``. Returns an empty dict when no credential
    is provisioned. Callers must NOT fall back to ``os.environ`` — provisioning
    is the operator's responsibility via ``praxis credential onboard``.

    ``sandbox_session_id`` is recorded for audit only; the credential itself
    is not narrowed (the sandbox lifetime bounds the credential's reach
    because the value is only injected into that sandbox's env / tmpfs file).
    """
    normalized = str(provider_slug or "").strip().lower()
    if not normalized:
        return {}

    db = _connection(conn)
    integration_id = _provider_integration_id(normalized)
    rows = db.execute(
        """SELECT access_token, provider_hint
             FROM credential_tokens
            WHERE integration_id = $1 AND token_kind = $2
            LIMIT 1""",
        integration_id,
        _API_KEY_TOKEN_KIND,
    )
    if not rows:
        logger.debug(
            "credential_authority.no_credential provider=%s integration_id=%s",
            normalized,
            integration_id,
        )
        return {}

    row = rows[0]
    value = str(row.get("access_token") or "")
    if not value:
        return {}
    env_var_name = str(row.get("provider_hint") or "").strip()
    if not env_var_name:
        # Fallback to first registered env var name for the provider.
        try:
            from registry.provider_execution_registry import resolve_api_key_env_vars

            candidates = resolve_api_key_env_vars(normalized)
        except Exception:
            candidates = ()
        env_var_name = candidates[0] if candidates else ""
    if not env_var_name:
        return {}
    if sandbox_session_id:
        logger.info(
            "credential_authority.minted provider=%s sandbox=%s env=%s",
            normalized,
            sandbox_session_id,
            env_var_name,
        )
    return {env_var_name: value}


def store_provider_credential(
    *,
    provider_slug: str,
    env_var_name: str,
    value: str,
    conn: "SyncPostgresConnection | None" = None,
) -> dict[str, Any]:
    """Upsert a provider credential into ``credential_tokens``.

    Used by the operator-facing onboarding command. The value is also
    mirrored into the Keychain (when available) under the env_var_name so
    local tooling that still resolves via Keychain stays in sync.
    """
    normalized_provider = str(provider_slug or "").strip().lower()
    normalized_env = str(env_var_name or "").strip()
    secret = str(value or "")
    if not normalized_provider:
        raise ValueError("provider_slug is required")
    if not normalized_env:
        raise ValueError("env_var_name is required")
    if not secret:
        raise ValueError("value is required")

    db = _connection(conn)
    integration_id = _provider_integration_id(normalized_provider)
    db.execute(
        """INSERT INTO credential_tokens
               (integration_id, token_kind, access_token, refresh_token,
                expires_at, scopes, token_type, provider_hint, updated_at)
           VALUES ($1, $2, $3, NULL, NULL, ARRAY[]::text[], 'Bearer', $4, now())
           ON CONFLICT (integration_id, token_kind) DO UPDATE SET
               access_token = EXCLUDED.access_token,
               provider_hint = EXCLUDED.provider_hint,
               updated_at = now()""",
        integration_id,
        _API_KEY_TOKEN_KIND,
        secret,
        normalized_env,
    )

    keychain_mirrored = False
    try:
        from adapters.keychain import keychain_set

        keychain_mirrored = bool(keychain_set(normalized_env, secret))
    except Exception:
        keychain_mirrored = False

    return {
        "provider_slug": normalized_provider,
        "integration_id": integration_id,
        "env_var_name": normalized_env,
        "keychain_mirrored": keychain_mirrored,
    }


def list_provisioned_providers(
    *,
    conn: "SyncPostgresConnection | None" = None,
) -> list[dict[str, Any]]:
    """Return one row per provider with a stored credential.

    Used by the onboarding/doctor surfaces to show what is and is not
    provisioned. Does NOT include the credential value.
    """
    db = _connection(conn)
    rows = db.execute(
        """SELECT integration_id, provider_hint, updated_at
             FROM credential_tokens
            WHERE integration_id LIKE $1 AND token_kind = $2
            ORDER BY integration_id ASC""",
        f"{_PROVIDER_INTEGRATION_PREFIX}%",
        _API_KEY_TOKEN_KIND,
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        integration_id = str(row.get("integration_id") or "")
        provider_slug = integration_id.removeprefix(_PROVIDER_INTEGRATION_PREFIX)
        out.append(
            {
                "provider_slug": provider_slug,
                "env_var_name": str(row.get("provider_hint") or ""),
                "updated_at": row.get("updated_at"),
            }
        )
    return out


__all__ = [
    "ProviderCredential",
    "list_provisioned_providers",
    "resolve_provider_credentials",
    "store_provider_credential",
]
