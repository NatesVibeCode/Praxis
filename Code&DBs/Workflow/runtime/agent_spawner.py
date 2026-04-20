"""Agent spawner with provider readiness checks.

Validates provider API-key availability before spawning agent
sub-processes, supporting dry-run and batch modes.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from adapters.keychain import resolve_secret
from registry.provider_execution_registry import resolve_api_key_env_vars


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProviderReadiness:
    provider: str
    ready: bool
    reason: str | None
    checked_at: datetime


@dataclass(frozen=True)
class SpawnRequest:
    job_label: str
    agent_slug: str
    prompt: str
    write_scope: tuple[str, ...]
    timeout_seconds: int
    environment: dict


@dataclass(frozen=True)
class SpawnResult:
    job_label: str
    agent_slug: str
    pid: int | None
    status: str  # 'spawned' | 'failed' | 'readiness_failed'
    error: str | None


# ---------------------------------------------------------------------------
# Provider readiness
# ---------------------------------------------------------------------------

# Anthropic is intentionally absent: the `claude` binary authenticates via
# OAuth (subscription), not an API key. Readiness falls through to the CLI
# binary check below. See decision.2026-04-20.anthropic-cli-only-restored.
_PROVIDER_ENV_KEYS_FALLBACK: dict[str, list[str]] = {
    "cursor": ["CURSOR_API_KEY"],
    "openai": ["OPENAI_API_KEY"],
    "google": ["GEMINI_API_KEY", "GOOGLE_API_KEY"],
}

_PROVIDER_CLI_BINARIES: dict[str, str] = {
    "anthropic": "claude",
    "google": "gemini",
    "openai": "codex",
}


class ProviderReadinessChecker:
    """Check whether a provider has either credentials or a local CLI transport."""

    def check(self, provider: str) -> ProviderReadiness:
        now = datetime.now(timezone.utc)
        env_keys = list(resolve_api_key_env_vars(provider)) or _PROVIDER_ENV_KEYS_FALLBACK.get(provider, [])
        cli_binary = _PROVIDER_CLI_BINARIES.get(provider)

        if not env_keys and cli_binary is None:
            return ProviderReadiness(
                provider=provider,
                ready=False,
                reason=f"Unknown provider: {provider}",
                checked_at=now,
            )

        for key in env_keys:
            if resolve_secret(key, env=dict(os.environ)):
                return ProviderReadiness(
                    provider=provider,
                    ready=True,
                    reason=None,
                    checked_at=now,
                )

        if cli_binary and shutil.which(cli_binary):
            return ProviderReadiness(
                provider=provider,
                ready=True,
                reason=None,
                checked_at=now,
            )

        expected = " or ".join(env_keys)
        if cli_binary:
            expected = f"{expected}; or local CLI binary {cli_binary!r}"
        return ProviderReadiness(
            provider=provider,
            ready=False,
            reason=f"Missing credential: {expected}",
            checked_at=now,
        )


# ---------------------------------------------------------------------------
# Slug -> provider extraction
# ---------------------------------------------------------------------------

def _provider_from_slug(slug: str) -> str:
    """Extract the provider prefix from an agent slug.

    Convention: ``<provider>-<model>-<variant>`` or ``<provider>/<model>``.
    Falls back to the full slug if no separator is found.
    """
    # Prefer the explicit provider/model boundary first because model slugs
    # commonly contain hyphens (for example ``openai/gpt-5.4``).
    for sep in ("/", "-"):
        if sep in slug:
            return slug.split(sep, 1)[0]
    return slug


# ---------------------------------------------------------------------------
# Agent spawner
# ---------------------------------------------------------------------------

class AgentSpawner:
    """Spawn agent processes after validating provider readiness."""

    def __init__(
        self,
        readiness_checker: ProviderReadinessChecker | None = None,
        command_resolver: Callable[[SpawnRequest], list[str] | None] | None = None,
        launcher: Callable[..., subprocess.Popen[str]] | None = None,
    ) -> None:
        self._checker = readiness_checker or ProviderReadinessChecker()
        self._command_resolver = command_resolver or self._resolve_command
        self._launcher = launcher or subprocess.Popen

    def preflight(self, agent_slug: str) -> ProviderReadiness:
        provider = _provider_from_slug(agent_slug)
        return self._checker.check(provider)

    def _resolve_command(self, request: SpawnRequest) -> list[str] | None:
        provider = _provider_from_slug(request.agent_slug).upper()
        for env_key in (f"PRAXIS_AGENT_SPAWN_COMMAND_{provider}", "PRAXIS_AGENT_SPAWN_COMMAND"):
            raw = os.environ.get(env_key, "").strip()
            if raw:
                return shlex.split(raw)
        return None

    def spawn(
        self,
        request: SpawnRequest,
        dry_run: bool = False,
    ) -> SpawnResult:
        readiness = self.preflight(request.agent_slug)
        if not readiness.ready:
            return SpawnResult(
                job_label=request.job_label,
                agent_slug=request.agent_slug,
                pid=None,
                status="readiness_failed",
                error=readiness.reason,
            )

        if dry_run:
            return SpawnResult(
                job_label=request.job_label,
                agent_slug=request.agent_slug,
                pid=None,
                status="spawned",
                error=None,
            )

        command = self._command_resolver(request)
        if not command:
            provider = _provider_from_slug(request.agent_slug).upper()
            return SpawnResult(
                job_label=request.job_label,
                agent_slug=request.agent_slug,
                pid=None,
                status="failed",
                error=(
                    "No spawn command configured for "
                    f"{request.agent_slug}; set PRAXIS_AGENT_SPAWN_COMMAND_{provider} "
                    "or PRAXIS_AGENT_SPAWN_COMMAND"
                ),
            )

        env = dict(os.environ)
        env.update({str(key): str(value) for key, value in request.environment.items()})
        env["PRAXIS_AGENT_WRITE_SCOPE"] = json.dumps(list(request.write_scope))

        try:
            process = self._launcher(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                env=env,
                start_new_session=True,
            )
            if process.stdin is not None:
                process.stdin.write(request.prompt)
                process.stdin.close()
        except OSError as exc:
            return SpawnResult(
                job_label=request.job_label,
                agent_slug=request.agent_slug,
                pid=None,
                status="failed",
                error=str(exc),
            )

        return SpawnResult(
            job_label=request.job_label,
            agent_slug=request.agent_slug,
            pid=process.pid,
            status="spawned",
            error=None,
        )

    def spawn_batch(
        self,
        requests: list[SpawnRequest],
        dry_run: bool = False,
    ) -> list[SpawnResult]:
        return [self.spawn(r, dry_run=dry_run) for r in requests]
