"""Operator-facing credential onboarding CLI.

Per architecture-policy::auth::credentials-per-sandbox-per-provider, the
runtime resolves provider credentials at sandbox-spawn time from
``credential_tokens`` instead of host-shell env vars. This command seeds
that table from the operator's machine. The secret is also mirrored into
macOS Keychain (under the env_var_name) when available, so local tooling
that still resolves via Keychain stays in sync.

Subcommands:
  list                                List provisioned providers.
  onboard <provider> [...]            Upsert one provider credential.
  remove <provider>                   Delete one provider credential.

Examples:
  workflow credential list
  workflow credential onboard openai --env OPENAI_API_KEY
  workflow credential onboard anthropic --env CLAUDE_CODE_OAUTH_TOKEN --secret-stdin
"""

from __future__ import annotations

import json
import sys
from typing import TextIO


def _extract_flag_value(args: list[str], flag: str) -> str | None:
    prefix = f"{flag}="
    for index, arg in enumerate(args):
        if arg == flag and index + 1 < len(args):
            return args[index + 1]
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return None


def _read_secret_from_stdin() -> str:
    raw = sys.stdin.read()
    return raw.strip()


def _resolve_default_env_var(provider_slug: str) -> str | None:
    try:
        from registry.provider_execution_registry import resolve_api_key_env_vars

        candidates = resolve_api_key_env_vars(provider_slug)
    except Exception:
        return None
    return candidates[0] if candidates else None


def _credential_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"help", "--help", "-h"}:
        stdout.write(
            "\n".join(
                [
                    "usage: workflow credential <list|onboard|remove> [...]",
                    "",
                    "Per-sandbox per-provider credential authority. Seeds",
                    "credential_tokens (DB-backed) so sandbox spawn does not need",
                    "to forward host-shell env vars (CLAUDE_CODE_OAUTH_TOKEN, etc.).",
                    "",
                    "  list                                Show provisioned providers (no secrets).",
                    "  onboard <provider> [--env <NAME>]   Upsert a provider credential.",
                    "                     [--secret <V> | --secret-stdin]",
                    "  remove <provider>                   Delete a provider credential.",
                    "",
                    "If --env is omitted, the env var defaults to the first entry",
                    "in the provider's api_key_env_vars from the provider registry.",
                ]
            )
            + "\n"
        )
        return 0

    mode = args[0]
    rest = list(args[1:])

    if mode == "list":
        return _list_command(stdout=stdout)
    if mode == "onboard":
        return _onboard_command(rest, stdout=stdout)
    if mode == "remove":
        return _remove_command(rest, stdout=stdout)

    stdout.write(f"unknown subcommand: {mode}\n")
    stdout.write("usage: workflow credential <list|onboard|remove> [...]\n")
    return 2


def _list_command(*, stdout: TextIO) -> int:
    from runtime.credential_authority import list_provisioned_providers

    rows = list_provisioned_providers()
    if not rows:
        stdout.write(
            json.dumps(
                {"ok": True, "providers": [], "note": "no provider credentials provisioned"},
                indent=2,
            )
            + "\n"
        )
        return 0
    serializable = [
        {
            "provider_slug": row["provider_slug"],
            "env_var_name": row["env_var_name"],
            "updated_at": str(row["updated_at"]) if row.get("updated_at") else None,
        }
        for row in rows
    ]
    stdout.write(json.dumps({"ok": True, "providers": serializable}, indent=2) + "\n")
    return 0


def _onboard_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0].startswith("-"):
        stdout.write(
            "usage: workflow credential onboard <provider> [--env <NAME>] [--secret <V> | --secret-stdin]\n"
        )
        return 2
    provider_slug = args[0]
    env_var_name = _extract_flag_value(args, "--env") or _resolve_default_env_var(provider_slug)
    if not env_var_name:
        stdout.write(
            f"error: cannot infer env var name for provider {provider_slug!r}; pass --env <NAME>\n"
        )
        return 2

    secret = _extract_flag_value(args, "--secret")
    if secret is None and "--secret-stdin" in args:
        secret = _read_secret_from_stdin()
    if not secret:
        stdout.write(
            "error: secret is required; pass --secret <V> or pipe via --secret-stdin\n"
        )
        return 2

    from runtime.credential_authority import store_provider_credential

    result = store_provider_credential(
        provider_slug=provider_slug,
        env_var_name=env_var_name,
        value=secret,
    )
    stdout.write(
        json.dumps(
            {
                "ok": True,
                "provider_slug": result["provider_slug"],
                "integration_id": result["integration_id"],
                "env_var_name": result["env_var_name"],
                "keychain_mirrored": result["keychain_mirrored"],
            },
            indent=2,
        )
        + "\n"
    )
    return 0


def _remove_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0].startswith("-"):
        stdout.write("usage: workflow credential remove <provider>\n")
        return 2
    provider_slug = args[0].strip().lower()
    if not provider_slug:
        stdout.write("error: provider is required\n")
        return 2

    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    conn = SyncPostgresConnection(get_workflow_pool())
    integration_id = f"provider:{provider_slug}"
    deleted = conn.execute(
        """DELETE FROM credential_tokens
            WHERE integration_id = $1 AND token_kind = 'api_key'
            RETURNING integration_id""",
        integration_id,
    )
    stdout.write(
        json.dumps(
            {
                "ok": True,
                "provider_slug": provider_slug,
                "removed": bool(deleted),
            },
            indent=2,
        )
        + "\n"
    )
    return 0
