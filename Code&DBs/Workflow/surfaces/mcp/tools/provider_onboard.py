"""Tools: praxis_provider_onboard, praxis_cli_auth_doctor."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_provider_onboard(params: dict, _progress_emitter=None) -> dict:
    """Onboard a CLI or API provider through the shared operation catalog."""

    action = str(params.get("action", "probe")).strip().lower()
    provider_slug = str(params.get("provider_slug", "")).strip()
    if not provider_slug:
        return {"error": "provider_slug is required"}

    transport = str(params.get("transport", "")).strip().lower()
    models = params.get("models") or []
    api_key_env_var = params.get("api_key_env_var")
    dry_run = action == "probe"

    payload = {
        "provider_slug": provider_slug,
        "dry_run": dry_run,
    }
    if transport:
        payload["transport"] = transport
    if models:
        payload["models"] = list(models)
    if api_key_env_var:
        payload["api_key_env_var"] = api_key_env_var

    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=2, message=f"Preparing provider onboarding for {provider_slug}")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="operator.provider_onboarding",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=2, total=2, message=f"Done — {provider_slug} {status}")
    return result


# ──────────────────────────────────────────────────────────────────────────
# CLI auth doctor wizard
#
# Probes claude / codex / gemini CLI binaries with a trivial prompt, parses
# the response for auth-failure patterns, returns structured per-provider
# health, and offers a concrete remediation step. The whole point: when
# something says "Not logged in" or 401, this wizard is the ONE place an
# operator goes to find out which CLI is broken AND what to do about it,
# without having to grep three different binaries' output formats.
#
# The Anthropic standing order is "one real auth rail per environment". On
# macOS hosts that means Claude's OAuth token lives in Keychain and must be
# bridged into Linux containers via CLAUDE_CODE_OAUTH_TOKEN. On Linux hosts
# Claude also supports ~/.claude/.credentials.json. The refresh path is:
# re-authenticate on the host (`claude login`), then bounce the api-server +
# worker containers so the renewed token/file is re-read by the sandbox runner.
# ──────────────────────────────────────────────────────────────────────────

# (binary_name, provider_slug, prompt_args). We use `claude -p`, `codex exec`,
# and `gemini -p` style invocations; each emits JSON with status fields.
_CLI_AUTH_PROBES: tuple[tuple[str, str, list[str]], ...] = (
    ("claude", "anthropic", ["-p", "--output-format", "json"]),
    ("codex", "openai", ["exec", "--output-last-message", "/tmp/_codex_authprobe.txt"]),
    ("gemini", "google", ["-p"]),
)

# Patterns that indicate auth failure, regardless of CLI brand
_AUTH_FAILURE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"not\s+logged\s+in",
        r"please\s+(?:run\s+)?[/\\]?login",
        r"authentication\s+(?:error|failed)",
        r"invalid\s+(?:auth|credentials|token)",
        r"\b401\b",
        r"unauthorized",
        r"token\s+expired",
        r"oauth.*(?:expired|invalid)",
        r"sign\s+in",
    )
)


def _which_binary(name: str) -> str | None:
    return shutil.which(name)


def _probe_cli_auth(binary: str, provider_slug: str, args: list[str]) -> dict[str, Any]:
    binary_path = _which_binary(binary)
    if not binary_path:
        return {
            "provider_slug": provider_slug,
            "binary": binary,
            "binary_path": None,
            "auth_state": "not_installed",
            "healthy": False,
            "summary": f"`{binary}` CLI is not installed on PATH inside this container.",
            "remediation": (
                f"Install the {binary} CLI inside the container's image OR "
                f"verify the right thin sandbox image is being used (see "
                f"runtime/docker_image_authority.AGENT_FAMILY_IMAGE_MAP)."
            ),
        }
    try:
        result = subprocess.run(
            [binary_path, *args],
            input="say hi" if provider_slug == "anthropic" else "say hi\n",
            capture_output=True,
            text=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        return {
            "provider_slug": provider_slug,
            "binary": binary,
            "binary_path": binary_path,
            "auth_state": "timeout",
            "healthy": False,
            "summary": f"`{binary}` did not respond within 15s.",
            "remediation": (
                f"The CLI is installed but hung. Try a manual `{binary} -p` from "
                "the host shell to confirm it works, then bounce the relevant "
                "containers (docker compose restart api-server workflow-worker)."
            ),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "provider_slug": provider_slug,
            "binary": binary,
            "binary_path": binary_path,
            "auth_state": "exec_error",
            "healthy": False,
            "summary": f"`{binary}` execution raised {type(exc).__name__}: {exc}",
            "remediation": "Check binary permissions / runtime.",
        }

    blob = (result.stdout or "") + "\n" + (result.stderr or "")
    matched_pattern: str | None = None
    for pattern in _AUTH_FAILURE_PATTERNS:
        m = pattern.search(blob)
        if m:
            matched_pattern = m.group(0)
            break

    if matched_pattern is not None:
        return {
            "provider_slug": provider_slug,
            "binary": binary,
            "binary_path": binary_path,
            "auth_state": "unauthenticated",
            "healthy": False,
            "exit_code": result.returncode,
            "matched_failure_pattern": matched_pattern,
            "stdout_preview": (result.stdout or "")[:240],
            "stderr_preview": (result.stderr or "")[:240],
            "summary": (
                f"`{binary}` reports an auth failure: {matched_pattern!r}. "
                "Token, OAuth grant, or credentials file is missing/expired."
            ),
            "remediation": _provider_auth_remediation(provider_slug),
        }

    return {
        "provider_slug": provider_slug,
        "binary": binary,
        "binary_path": binary_path,
        "auth_state": "authenticated",
        "healthy": True,
        "exit_code": result.returncode,
        "stdout_preview": (result.stdout or "")[:200],
        "summary": f"`{binary}` responded successfully.",
    }


def _provider_auth_remediation(provider_slug: str) -> dict[str, Any]:
    if provider_slug == "anthropic":
        return {
            "provider": "anthropic",
            "host_action": "Run `claude login` on your Mac (host shell), authorize via browser.",
            "rehydrate_action": (
                "From the host repo root run the canonical resolver: "
                "`scripts/praxis-up`"
            ),
            "auth_sources": [
                "macOS Keychain entry Claude Code-credentials -> CLAUDE_CODE_OAUTH_TOKEN (container env path)",
                "~/.claude/.credentials.json on Linux/Windows hosts (file mount path)",
            ],
            "expected_status_after_fix": "claude -p inside api-server container responds without 'Not logged in'",
        }
    if provider_slug == "openai":
        return {
            "provider": "openai",
            "host_action": "Run `codex login` on your Mac (host shell).",
            "rehydrate_action": (
                "From the host repo root run the canonical resolver: "
                "`scripts/praxis-up`"
            ),
            "auth_sources": [
                "~/.codex/auth.json (file mount path)",
                "OPENAI_API_KEY env var (API path; CLI lane prefers file)",
            ],
            "expected_status_after_fix": "codex exec inside api-server container completes without auth error",
        }
    if provider_slug in ("google", "gemini"):
        return {
            "provider": provider_slug,
            "host_action": "Run `gemini` interactively on your Mac, complete OAuth flow.",
            "rehydrate_action": (
                "From the host repo root run the canonical resolver: "
                "`scripts/praxis-up`"
            ),
            "auth_sources": [
                "~/.gemini/oauth_creds.json",
                "~/.gemini/google_accounts.json",
                "~/.gemini/settings.json",
            ],
            "expected_status_after_fix": "gemini -p inside api-server container responds without auth error",
        }
    return {
        "provider": provider_slug,
        "host_action": f"Re-authenticate the {provider_slug} CLI on the host.",
    }


def tool_praxis_cli_auth_doctor(params: dict, _progress_emitter=None) -> dict:
    """Probe claude / codex / gemini CLI auth health and report structured per-provider state.

    Returns a per-provider report with auth_state, healthy bool, summary, and
    concrete remediation steps. When unhealthy, the remediation field names
    the exact host command to run AND the rehydration command for restoring
    the container's view of the auth files.

    The wizard does NOT execute the rehydration itself — Keychain access and
    `docker compose` recreation must run on the host (the macOS security CLI
    isn't reachable from inside a Linux container). It tells the operator
    EXACTLY what to run.
    """
    requested = params.get("providers")
    requested_set: set[str] | None = None
    if isinstance(requested, list) and requested:
        requested_set = {str(p).strip().lower() for p in requested}
    elif isinstance(requested, str) and requested.strip():
        requested_set = {requested.strip().lower()}

    reports: list[dict[str, Any]] = []
    for binary, provider_slug, args in _CLI_AUTH_PROBES:
        if requested_set is not None and provider_slug not in requested_set:
            continue
        if _progress_emitter:
            _progress_emitter.emit(
                progress=len(reports),
                total=3,
                message=f"Probing {binary} ({provider_slug}) auth …",
            )
        reports.append(_probe_cli_auth(binary, provider_slug, args))

    healthy = sum(1 for r in reports if r.get("healthy"))
    unhealthy = [r for r in reports if not r.get("healthy")]
    overall_ok = bool(reports) and not unhealthy

    response: dict[str, Any] = {
        "ok": overall_ok,
        "tool": "praxis_cli_auth_doctor",
        "checked_at_epoch": int(__import__("time").time()),
        "summary": (
            f"All {len(reports)} CLI(s) authenticated."
            if overall_ok
            else f"{len(unhealthy)} of {len(reports)} CLI(s) failed auth probe."
        ),
        "providers_checked": [r["provider_slug"] for r in reports],
        "healthy_count": healthy,
        "unhealthy_count": len(unhealthy),
        "reports": reports,
    }
    if unhealthy:
        response["next_action"] = {
            "kind": "host_remediation",
            "rehydration_command_chain": "scripts/praxis-up",
            "per_provider_actions": [r.get("remediation") for r in unhealthy],
            "hint": (
                "Run host_action(s) below on your Mac shell, then run rehydration_command_chain "
                "from the repo root. Re-run praxis_cli_auth_doctor to confirm green."
            ),
        }
    if _progress_emitter:
        _progress_emitter.emit(
            progress=len(reports),
            total=len(reports),
            message=("OK" if overall_ok else f"{len(unhealthy)} unhealthy"),
        )
    return response


TOOLS: dict[str, tuple[callable, dict[str, object]]] = {
    "praxis_cli_auth_doctor": (
        tool_praxis_cli_auth_doctor,
        {
            "description": (
                "Diagnose CLI auth state for claude / codex / gemini in one call. "
                "Probes each binary with a trivial prompt, parses the output for auth-failure "
                "patterns ('Not logged in', '401', 'authentication error'…), and returns a "
                "structured per-provider report with concrete host-side remediation commands.\n\n"
                "USE WHEN: a workflow run failed with `sandbox_error: Not logged in`, or you "
                "suspect one of the CLI auths drifted (Keychain stale, OAuth expired, "
                "credentials file missing).\n\n"
                "Does NOT itself rehydrate auth — the macOS security CLI / `claude login` "
                "flow runs on the host. The wizard returns the EXACT host commands to run, "
                "scoped to the providers that actually failed.\n\n"
                "EXAMPLES:\n"
                "  All three:    praxis_cli_auth_doctor()\n"
                "  Only claude:  praxis_cli_auth_doctor(providers=['anthropic'])"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "providers": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["anthropic", "openai", "google"]},
                        "description": (
                            "Optional subset to check. Default: all three (anthropic, openai, google)."
                        ),
                    },
                },
            },
        },
    ),
    "praxis_provider_onboard": (
        tool_praxis_provider_onboard,
        {
            "description": (
                "Onboard a CLI or API provider into Praxis Engine through one catalog-backed "
                "operation. Probes transport, discovers models, writes onboarding authority, "
                "and performs the canonical post-onboarding sync.\n\n"
                "USE WHEN: connecting a new provider or adding models to an existing provider.\n\n"
                "EXAMPLES:\n"
                "  Probe first:  praxis_provider_onboard(action='probe', provider_slug='anthropic', transport='cli')\n"
                "  Then onboard: praxis_provider_onboard(action='onboard', provider_slug='anthropic', transport='cli')\n"
                "  API provider: praxis_provider_onboard(action='onboard', provider_slug='openrouter', transport='api', "
                "api_key_env_var='OPENROUTER_API_KEY')\n\n"
                "The 'probe' action is a dry run. The 'onboard' action writes onboarding authority "
                "and applies the canonical post-onboarding sync.\n\n"
                "DO NOT USE: for checking provider health (use praxis_health)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["probe", "onboard"],
                        "description": "'probe' (dry run) or 'onboard' (write to DB authority)",
                    },
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider identifier (e.g., 'anthropic', 'openai', 'google', 'openrouter')",
                    },
                    "transport": {
                        "type": "string",
                        "enum": ["cli", "api"],
                        "description": "Transport type: 'cli' for CLI tools, 'api' for direct API",
                    },
                    "models": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional specific model slugs to onboard (discovers all if omitted)",
                    },
                    "api_key_env_var": {
                        "type": "string",
                        "description": "Env var name for API key (e.g., 'OPENROUTER_API_KEY')",
                    },
                },
                "required": ["action", "provider_slug"],
            },
        },
    ),
}
