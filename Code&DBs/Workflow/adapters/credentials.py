"""Credential resolution from auth_ref strings to API keys.

Maps endpoint authority auth_ref values (e.g. "secret.default-path.openai")
to actual API keys from environment variables. The last dot-separated segment
is treated as the provider hint.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

from .keychain import resolve_secret
from registry.provider_execution_registry import resolve_api_key_env_vars


class CredentialResolutionError(RuntimeError):
    """Raised when an auth_ref cannot be resolved to a usable credential."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class ResolvedCredential:
    """A resolved credential ready for use in an API call."""

    auth_ref: str
    api_key: str
    provider_hint: str


def resolve_credential(
    auth_ref: str,
    *,
    env: dict[str, str] | None = None,
    conn: Any = None,
    integration_id: str | None = None,
) -> ResolvedCredential:
    """Resolve an auth_ref to an API key.

    Checks the OAuth token store first (when conn and integration_id are
    provided), then falls back to environment variables.
    """
    if conn is not None and integration_id:
        try:
            from .oauth_lifecycle import resolve_or_refresh

            token = resolve_or_refresh(conn, integration_id)
            return ResolvedCredential(
                auth_ref=auth_ref,
                api_key=token.access_token,
                provider_hint=token.token_type.lower(),
            )
        except Exception as exc:
            logger.debug("oauth token store miss for %s: %s", integration_id, exc)

    source = env if env is not None else os.environ
    segments = auth_ref.rsplit(".", maxsplit=1)
    provider_hint = segments[-1].lower() if segments else ""

    env_vars = resolve_api_key_env_vars(provider_hint)
    if not env_vars:
        raise CredentialResolutionError(
            "credential.provider_unknown",
            f"no env var mapping for provider hint {provider_hint!r} "
            f"from auth_ref={auth_ref!r}",
        )
    # Resolution order: Keychain first, then env vars.
    for env_var in env_vars:
        api_key = resolve_secret(env_var, env=env)
        if api_key:
            return ResolvedCredential(
                auth_ref=auth_ref,
                api_key=api_key,
                provider_hint=provider_hint,
            )
    primary_env_var = env_vars[0]
    raise CredentialResolutionError(
        "credential.env_var_missing",
        f"{primary_env_var} not found in Keychain or environment "
        f"(auth_ref={auth_ref!r})",
    )
