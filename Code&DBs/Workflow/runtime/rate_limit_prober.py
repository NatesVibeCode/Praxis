"""Rate-limit prober — lightweight CLI pings to detect provider availability.

Dispatches a trivial prompt to the cheapest model per provider through the
sandbox docker runner (``run_in_docker``) and records the outcome to the
circuit breaker. Running through the sandbox means probes always execute in
a fresh ephemeral container with the canonical image, mounted CLI auth, and
no host session env (``CLAUDECODE``) leakage — regardless of whether the
heartbeat runs on the host or inside the worker container.

Designed to run as a heartbeat module a few times per day.
"""
from __future__ import annotations

import logging
import shlex
import time
from dataclasses import dataclass
from typing import Literal

_log = logging.getLogger(__name__)

# One lightweight model per provider — enough to detect rate limits.
# The probe prompt is tiny so cost is negligible; Anthropic is CLI/subscription.
_PROBE_MODELS: tuple[tuple[str, str], ...] = (
    ("google", "gemini-2.5-flash"),
    ("openai", "gpt-5.4-mini"),
    ("anthropic", "claude-sonnet-4-6"),
)

_PROBE_PROMPT = "Reply with the single word OK."
_PROBE_TIMEOUT_S = 30


@dataclass(frozen=True, slots=True)
class ProbeResult:
    provider_slug: str
    model_slug: str
    status: Literal["ok", "rate_limited", "error"]
    detail: str
    latency_ms: int


def _get_cmd_template(provider_slug: str, model_slug: str) -> list[str] | None:
    """Load CLI command template from provider_model_candidates."""
    from runtime._workflow_database import resolve_runtime_database_url
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    try:
        conn_url = resolve_runtime_database_url(required=True)
        conn = SyncPostgresConnection(get_workflow_pool(env={"WORKFLOW_DATABASE_URL": conn_url}))
        rows = conn.execute(
            """SELECT cli_config FROM provider_model_candidates
               WHERE provider_slug = $1 AND model_slug = $2
                 AND status = 'active'
               LIMIT 1""",
            provider_slug, model_slug,
        )
        if not rows:
            return None
        cfg = rows[0]["cli_config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        template = cfg.get("cmd_template")
        if template:
            return [s.replace("{model}", model_slug) for s in template]
    except Exception as exc:
        _log.debug("probe: failed to load CLI config for %s/%s: %s", provider_slug, model_slug, exc)
    return None


def probe_provider(provider_slug: str, model_slug: str) -> ProbeResult:
    """Send a minimal CLI prompt via the sandbox docker runner and classify."""
    cmd = _get_cmd_template(provider_slug, model_slug)
    if cmd is None:
        return ProbeResult(
            provider_slug=provider_slug, model_slug=model_slug,
            status="error", detail="no CLI config found", latency_ms=0,
        )

    from adapters.docker_runner import normalize_command_parts_for_docker, run_in_docker

    normalized = normalize_command_parts_for_docker(cmd)
    command = shlex.join(normalized)

    start = time.monotonic()
    try:
        result = run_in_docker(
            command=command,
            stdin_text=_PROBE_PROMPT,
            timeout=_PROBE_TIMEOUT_S,
            network=True,
            provider_slug=provider_slug,
            auth_mount_policy="provider_scoped",
        )
    except Exception as exc:
        latency = int((time.monotonic() - start) * 1000)
        return ProbeResult(
            provider_slug=provider_slug, model_slug=model_slug,
            status="error", detail=str(exc)[:200], latency_ms=latency,
        )

    latency = result.latency_ms or int((time.monotonic() - start) * 1000)

    if result.timed_out:
        return ProbeResult(
            provider_slug=provider_slug, model_slug=model_slug,
            status="error", detail="timeout", latency_ms=latency,
        )

    if result.exit_code == 0:
        return ProbeResult(
            provider_slug=provider_slug, model_slug=model_slug,
            status="ok", detail="probe succeeded", latency_ms=latency,
        )

    stderr = result.stderr or ""
    stderr_lower = stderr.lower()
    if "429" in stderr or "rate limit" in stderr_lower or "quota" in stderr_lower:
        return ProbeResult(
            provider_slug=provider_slug, model_slug=model_slug,
            status="rate_limited", detail=stderr[:200], latency_ms=latency,
        )

    return ProbeResult(
        provider_slug=provider_slug, model_slug=model_slug,
        status="error", detail=f"exit {result.exit_code}: {stderr[:200]}", latency_ms=latency,
    )


def probe_all() -> list[ProbeResult]:
    """Probe every provider and record results to the circuit breaker.

    Returns the list of probe results.
    """
    from .circuit_breaker import get_circuit_breakers
    breakers = get_circuit_breakers()
    results: list[ProbeResult] = []

    for provider_slug, model_slug in _PROBE_MODELS:
        result = probe_provider(provider_slug, model_slug)
        results.append(result)

        succeeded = result.status == "ok"
        failure_code = "rate_limited" if result.status == "rate_limited" else None
        breakers.record_outcome(
            provider_slug,
            succeeded=succeeded,
            failure_code=failure_code,
        )

        _log.info(
            "probe %s/%s: %s (%dms) — %s",
            provider_slug, model_slug, result.status,
            result.latency_ms, result.detail,
        )

    return results


# ---------------------------------------------------------------------------
# Heartbeat module adapter
# ---------------------------------------------------------------------------

class RateLimitProbeModule:
    """HeartbeatModule that probes provider rate limits."""

    @property
    def name(self) -> str:
        return "rate_limit_prober"

    def run(self):
        """Run probes and return a HeartbeatModuleResult."""
        from runtime.heartbeat import HeartbeatModuleResult
        t0 = time.monotonic()
        findings: list[str] = []
        errors: list[str] = []

        try:
            results = probe_all()
            for r in results:
                if r.status == "rate_limited":
                    findings.append(f"{r.provider_slug}/{r.model_slug}: rate limited — {r.detail}")
                elif r.status == "error":
                    findings.append(f"{r.provider_slug}/{r.model_slug}: error — {r.detail}")
                else:
                    findings.append(f"{r.provider_slug}/{r.model_slug}: ok ({r.latency_ms}ms)")
        except Exception as exc:
            errors.append(str(exc))

        elapsed = (time.monotonic() - t0) * 1000
        all_issues = list(findings) + list(errors)
        return HeartbeatModuleResult(
            module_name=self.name,
            ok=len(all_issues) == 0,
            error="; ".join(all_issues) if all_issues else None,
            duration_ms=elapsed,
        )
