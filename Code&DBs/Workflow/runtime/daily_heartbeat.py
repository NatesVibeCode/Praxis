"""Daily heartbeat — unified probe across providers, connectors, credentials, MCP servers.

Writes rows into ``heartbeat_runs`` (run-level aggregation) and
``heartbeat_probe_snapshots`` (per-subject observations) so progress over time is
queryable via the per-kind views (``v_provider_usage_snapshots``, etc.).

Complements ``runtime/rate_limit_prober.py``: that module feeds circuit_breaker
state in real-time (pass/fail/rate_limited); this module persists a full
observability timeseries (tokens consumed, rate-limit headers, connector health
stats, credential expiry windows, MCP server handshake latency).

Entry points:
  - scripts/daily_heartbeat.py (launchd + manual invocation)
  - praxis workflow heartbeat (CLI subcommand)
  - praxis_daily_heartbeat MCP tool
  - await run_daily_heartbeat(scope=..., triggered_by=...) directly
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import asyncpg

from storage.postgres import connect_workflow_database
from runtime.workspace_paths import repo_root as workspace_repo_root

logger = logging.getLogger(__name__)

HeartbeatScope = Literal[
    "providers", "connectors", "credentials", "mcp", "model_retirement", "all"
]
TriggeredBy = Literal["launchd", "cli", "mcp", "http", "test"]

_SCOPES: tuple[HeartbeatScope, ...] = (
    "providers",
    "connectors",
    "credentials",
    "mcp",
    "model_retirement",
)

_PROBE_PROMPT = "Reply with exactly HEARTBEAT_OK"
_PROBE_EXPECTED = "HEARTBEAT_OK"

_DEFAULT_TIMEOUTS_S: dict[str, int] = {
    "providers": 60,
    "connectors": 5,
    "credentials": 5,
    "mcp": 30,
    "model_retirement": 60,
}

_CREDENTIAL_EXPIRY_WARNING_DAYS = 7

_REPO_ROOT = workspace_repo_root()


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ProbeSnapshot:
    """One observation of one subject."""

    probe_kind: str
    subject_id: str
    status: str  # ok|degraded|failed|warning|skipped
    summary: str = ""
    subject_sub: str | None = None
    latency_ms: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    estimated_cost_usd: float | None = None
    days_until_expiry: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class HeartbeatRunResult:
    heartbeat_run_id: str
    scope: str
    triggered_by: str
    started_at: datetime
    completed_at: datetime
    status: str
    probes_total: int
    probes_ok: int
    probes_failed: int
    summary: str
    snapshots: list[ProbeSnapshot]

    def to_json(self) -> dict[str, Any]:
        return {
            "heartbeat_run_id": self.heartbeat_run_id,
            "scope": self.scope,
            "triggered_by": self.triggered_by,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "status": self.status,
            "probes_total": self.probes_total,
            "probes_ok": self.probes_ok,
            "probes_failed": self.probes_failed,
            "summary": self.summary,
            "snapshots": [
                {
                    "probe_kind": s.probe_kind,
                    "subject_id": s.subject_id,
                    "subject_sub": s.subject_sub,
                    "status": s.status,
                    "summary": s.summary,
                    "latency_ms": s.latency_ms,
                    "input_tokens": s.input_tokens,
                    "output_tokens": s.output_tokens,
                    "estimated_cost_usd": s.estimated_cost_usd,
                    "days_until_expiry": s.days_until_expiry,
                    "details": s.details,
                }
                for s in self.snapshots
            ],
        }


# ---------------------------------------------------------------------------
# Provider usage probe
# ---------------------------------------------------------------------------

def _pick_usage_tokens(payload: Any) -> tuple[int | None, int | None, dict[str, Any]]:
    """Best-effort extraction of (input_tokens, output_tokens, raw_usage_block) from
    CLI JSON output. Handles Anthropic claude, OpenAI codex, and Gemini shapes.
    """
    if not isinstance(payload, dict):
        return None, None, {}

    # Anthropic claude CLI: top-level "usage" or nested under "message.usage"
    usage: dict[str, Any] | None = None
    if isinstance(payload.get("usage"), dict):
        usage = dict(payload["usage"])
    elif isinstance(payload.get("message"), dict) and isinstance(payload["message"].get("usage"), dict):
        usage = dict(payload["message"]["usage"])
    # OpenAI codex: "response.usage" or "usage"
    elif isinstance(payload.get("response"), dict) and isinstance(payload["response"].get("usage"), dict):
        usage = dict(payload["response"]["usage"])
    # Gemini non-interactive (`-o json`): stats.models[<slug>].tokens.{input,candidates}
    elif isinstance(payload.get("stats"), dict) and isinstance(payload["stats"].get("models"), dict):
        first_model = next(iter(payload["stats"]["models"].values()), None)
        if isinstance(first_model, dict) and isinstance(first_model.get("tokens"), dict):
            tokens = first_model["tokens"]
            usage = {
                "input_tokens": tokens.get("input") or tokens.get("prompt"),
                "output_tokens": tokens.get("candidates"),
                "raw": dict(tokens),
            }
    # Gemini streaming API shape: "usageMetadata"
    elif isinstance(payload.get("usageMetadata"), dict):
        meta = payload["usageMetadata"]
        usage = {
            "input_tokens": meta.get("promptTokenCount"),
            "output_tokens": meta.get("candidatesTokenCount"),
            "raw": dict(meta),
        }

    if not usage:
        return None, None, {}

    input_tokens = usage.get("input_tokens")
    if input_tokens is None:
        input_tokens = usage.get("prompt_tokens")
    output_tokens = usage.get("output_tokens")
    if output_tokens is None:
        output_tokens = usage.get("completion_tokens")

    try:
        i = int(input_tokens) if input_tokens is not None else None
    except (TypeError, ValueError):
        i = None
    try:
        o = int(output_tokens) if output_tokens is not None else None
    except (TypeError, ValueError):
        o = None
    return i, o, usage


def _coerce_json_list(value: Any) -> list[Any]:
    """Normalize a jsonb field asyncpg hands back as either a parsed list or
    a JSON-encoded str. Without this, ``list(value_str)`` iterates the string
    character-by-character and blows argv apart — that silently destroyed the
    provider probes until it was spotted.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return list(parsed) if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _coerce_json_dict(value: Any) -> dict[str, Any]:
    """Dict sibling of :func:`_coerce_json_list` — same asyncpg jsonb drift story."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _pick_model_slug(payload: Any) -> str | None:
    """Extract the model slug the provider actually routed to from a CLI payload.

    Preferred over whatever is pinned in ``provider_cli_profiles.default_model``:
    the CLI tells us what it ran, which stays correct when vendors rotate
    defaults. Falls back silently if the response shape doesn't advertise one.
    """
    if not isinstance(payload, dict):
        return None
    # Anthropic claude CLI: top-level "model", nested under "message.model",
    # or as the first key of the "modelUsage" breakdown (claude 2.x shape).
    for candidate in (
        payload.get("model"),
        (payload.get("message") or {}).get("model") if isinstance(payload.get("message"), dict) else None,
        (payload.get("response") or {}).get("model") if isinstance(payload.get("response"), dict) else None,
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    model_usage = payload.get("modelUsage")
    if isinstance(model_usage, dict) and model_usage:
        first_key = next(iter(model_usage))
        if isinstance(first_key, str) and first_key.strip():
            return first_key.strip()
    # Gemini CLI non-interactive (`-o json`): stats.models keyed by slug
    stats = payload.get("stats")
    if isinstance(stats, dict):
        stats_models = stats.get("models")
        if isinstance(stats_models, dict) and stats_models:
            first_key = next(iter(stats_models))
            if isinstance(first_key, str) and first_key.strip():
                return first_key.strip()
    # Gemini streaming API shape: modelVersion sibling to usageMetadata
    mv = payload.get("modelVersion")
    if isinstance(mv, str) and mv.strip():
        return mv.strip()
    return None


_NDJSON_MERGE_TOP_LEVEL = ("usage", "model", "modelVersion", "usageMetadata", "stats")


def _parse_cli_output(stdout: str, output_format: str) -> dict[str, Any]:
    text = (stdout or "").strip()
    if not text:
        return {}
    if output_format == "json":
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    if output_format == "ndjson":
        # Codex (and similar) emit an event stream: thread.started → turn.started
        # → item.completed(agent_message) → turn.completed(usage). No single line
        # carries both the response text and the usage block, so we merge: take
        # the last observation of each usage-related top-level key, and collect
        # any agent_message texts into a "text" field.
        merged: dict[str, Any] = {}
        text_parts: list[str] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            for key in _NDJSON_MERGE_TOP_LEVEL:
                if key in obj and obj[key] is not None:
                    merged[key] = obj[key]
            # Codex event shape: {"type":"item.completed","item":{"type":"agent_message","text":"..."}}
            if obj.get("type") == "item.completed":
                item = obj.get("item")
                if isinstance(item, dict) and item.get("type") == "agent_message":
                    txt = item.get("text")
                    if isinstance(txt, str) and txt:
                        text_parts.append(txt)
        if text_parts and "text" not in merged:
            merged["text"] = "".join(text_parts)
        return merged
    return {}


# Session env markers that force CLI probes to abort. We strip them in the
# subprocess env so a heartbeat triggered from inside a Claude Code session (or
# similar nested runtime) doesn't refuse to probe. Launchd/cron invocations
# already have a clean env; this just keeps manual invocation usable.
_SESSION_ENV_STRIP = ("CLAUDECODE", "CLAUDE_CODE", "ANTHROPIC_SESSION_ID")


def _probe_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in _SESSION_ENV_STRIP}
    return env


async def _probe_provider(
    row: Mapping[str, Any],
    profile: Mapping[str, Any] | None,
    *,
    timeout_s: int,
) -> ProbeSnapshot:
    provider_slug = row["provider_slug"]
    adapter_type = row["adapter_type"]
    transport_kind = row["transport_kind"]

    if transport_kind != "cli":
        return ProbeSnapshot(
            probe_kind="provider_usage",
            subject_id=provider_slug,
            subject_sub=adapter_type,
            status="skipped",
            summary=f"Transport {transport_kind} not CLI-probable",
            details={"transport_kind": transport_kind},
        )

    if profile is None or not profile["binary_name"]:
        return ProbeSnapshot(
            probe_kind="provider_usage",
            subject_id=provider_slug,
            subject_sub=adapter_type,
            status="skipped",
            summary="No active provider_cli_profile",
        )

    binary_name = profile["binary_name"]
    base_flags = _coerce_json_list(profile["base_flags"])
    output_format = profile["output_format"] or "json"
    envelope_key = profile["output_envelope_key"] or "result"
    prompt_mode = profile["prompt_mode"] or "stdin"

    economics = _coerce_json_dict(profile["adapter_economics"])
    adapter_econ = _coerce_json_dict(economics.get(adapter_type))
    marginal_cost = adapter_econ.get("effective_marginal_cost") or 0.0
    try:
        marginal_cost = float(marginal_cost)
    except (TypeError, ValueError):
        marginal_cost = 0.0

    # Deliberately do NOT pass --model: a liveness probe should track the CLI
    # vendor's own current default (which they keep live), not a slug pinned
    # into provider_cli_profiles.default_model that drifts the moment a model
    # is deprecated. The recorded model_slug below comes from the actual CLI
    # response, not our DB.
    cmd = [binary_name, *base_flags]

    started = time.monotonic()
    stdin_text: str | None = _PROBE_PROMPT if prompt_mode == "stdin" else None
    if prompt_mode == "argv":
        cmd.append(_PROBE_PROMPT)

    # Run in an ephemeral empty directory so CLIs don't ingest the repo context
    # (codex reads AGENTS.md, gemini walks the workspace, claude honors CLAUDE.md).
    # That context is arbitrary and inflates probe cost + latency with no signal.
    # The sandbox is torn down after the call.
    try:
        with tempfile.TemporaryDirectory(prefix="praxis-heartbeat-") as sandbox_cwd:
            proc = await asyncio.to_thread(
                subprocess.run,
                cmd,
                input=stdin_text,
                capture_output=True,
                text=True,
                timeout=timeout_s,
                env=_probe_env(),
                cwd=sandbox_cwd,
            )
    except subprocess.TimeoutExpired:
        latency = int((time.monotonic() - started) * 1000)
        return ProbeSnapshot(
            probe_kind="provider_usage",
            subject_id=provider_slug,
            subject_sub=adapter_type,
            status="failed",
            summary=f"CLI probe timed out after {timeout_s}s",
            latency_ms=latency,
            details={"command": cmd, "error": "timeout"},
        )
    except FileNotFoundError:
        return ProbeSnapshot(
            probe_kind="provider_usage",
            subject_id=provider_slug,
            subject_sub=adapter_type,
            status="failed",
            summary=f"CLI binary {binary_name!r} not found on PATH",
            details={"command": cmd, "error": "binary_not_found"},
        )
    except Exception as exc:  # noqa: BLE001
        latency = int((time.monotonic() - started) * 1000)
        return ProbeSnapshot(
            probe_kind="provider_usage",
            subject_id=provider_slug,
            subject_sub=adapter_type,
            status="failed",
            summary=f"CLI probe errored: {exc}",
            latency_ms=latency,
            details={"command": cmd, "error": str(exc)[:200]},
        )

    latency = int((time.monotonic() - started) * 1000)
    payload = _parse_cli_output(proc.stdout or "", output_format)
    response_text = ""
    if isinstance(payload, dict):
        raw = payload.get(envelope_key)
        if isinstance(raw, str):
            response_text = raw
    input_tokens, output_tokens, raw_usage = _pick_usage_tokens(payload)

    stderr_lower = (proc.stderr or "").lower()
    rate_limited = "429" in (proc.stderr or "") or "rate limit" in stderr_lower or "quota" in stderr_lower
    succeeded = proc.returncode == 0 and _PROBE_EXPECTED in response_text

    status = "ok" if succeeded else ("degraded" if rate_limited else "failed")

    cost: float | None = None
    total = (input_tokens or 0) + (output_tokens or 0)
    if total > 0 and marginal_cost > 0:
        # effective_marginal_cost is a relative weight, not a $/token rate — so
        # estimated_cost_usd is approximate. Consumers treat it as directional.
        cost = round((total / 1_000_000) * marginal_cost, 6)

    details: dict[str, Any] = {
        "model_slug": _pick_model_slug(payload),
        "transport_kind": transport_kind,
        "returncode": proc.returncode,
        "billing_mode": adapter_econ.get("billing_mode"),
        "budget_bucket": adapter_econ.get("budget_bucket"),
        "effective_marginal_cost": marginal_cost,
        "response_excerpt": (response_text or "")[:200],
        "stderr_excerpt": (proc.stderr or "")[:200],
        "raw_usage": raw_usage,
    }

    if rate_limited:
        details["rate_limited"] = True

    return ProbeSnapshot(
        probe_kind="provider_usage",
        subject_id=provider_slug,
        subject_sub=adapter_type,
        status=status,
        summary=(
            f"{provider_slug}/{adapter_type}: {status}"
            + (f" — {input_tokens}+{output_tokens} tokens" if total > 0 else "")
        ),
        latency_ms=latency,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_usd=cost,
        details=details,
    )


def _normalized_text_filter(values: Sequence[str] | None) -> set[str]:
    if values is None:
        return set()
    return {
        text
        for value in values
        if (text := str(value or "").strip().lower())
    }


async def probe_providers(
    conn: asyncpg.Connection,
    *,
    timeout_s: int,
    provider_slugs: Sequence[str] | None = None,
    adapter_types: Sequence[str] | None = None,
    max_concurrency: int = 4,
) -> list[ProbeSnapshot]:
    # Pre-fetch all admissions and profiles in one serial pass; asyncpg
    # connections are not safe for concurrent use, so we load everything first
    # and then fan out the subprocess calls (which don't touch the DB).
    admissions = await conn.fetch(
        """
        SELECT provider_slug, adapter_type, transport_kind, admitted_by_policy
          FROM provider_transport_admissions
         WHERE status = 'active' AND admitted_by_policy = true
         ORDER BY provider_slug, adapter_type
        """
    )
    provider_filter = _normalized_text_filter(provider_slugs)
    adapter_filter = _normalized_text_filter(adapter_types)
    if provider_filter or adapter_filter:
        admissions = [
            row
            for row in admissions
            if (
                not provider_filter
                or str(row["provider_slug"]).strip().lower() in provider_filter
            )
            and (
                not adapter_filter
                or str(row["adapter_type"]).strip().lower() in adapter_filter
            )
        ]

    admitted_provider_slugs = sorted({row["provider_slug"] for row in admissions})
    profile_rows = await conn.fetch(
        """
        SELECT provider_slug, binary_name, base_flags, model_flag, default_model,
               output_format, output_envelope_key, api_key_env_vars,
               adapter_economics, prompt_mode
          FROM provider_cli_profiles
         WHERE status = 'active' AND provider_slug = ANY($1::text[])
        """,
        admitted_provider_slugs,
    )
    profiles_by_slug: dict[str, Mapping[str, Any]] = {
        r["provider_slug"]: r for r in profile_rows
    }

    concurrency = max(1, int(max_concurrency or 1))
    semaphore = asyncio.Semaphore(concurrency)

    async def _guarded_probe(row: Mapping[str, Any]) -> ProbeSnapshot:
        async with semaphore:
            return await _probe_provider(
                row,
                profiles_by_slug.get(row["provider_slug"]),
                timeout_s=timeout_s,
            )

    results = await asyncio.gather(
        *(_guarded_probe(row) for row in admissions),
        return_exceptions=True,
    )
    snapshots: list[ProbeSnapshot] = []
    for row, result in zip(admissions, results, strict=True):
        if isinstance(result, BaseException):
            snapshots.append(
                ProbeSnapshot(
                    probe_kind="provider_usage",
                    subject_id=row["provider_slug"],
                    subject_sub=row["adapter_type"],
                    status="failed",
                    summary=f"Probe raised: {result}",
                    details={"error": str(result)[:200]},
                )
            )
        else:
            snapshots.append(result)
    return snapshots


# ---------------------------------------------------------------------------
# Connector liveness probe (passive: reads connector_registry stats)
# ---------------------------------------------------------------------------

async def probe_connectors(conn: asyncpg.Connection) -> list[ProbeSnapshot]:
    rows = await conn.fetch(
        """
        SELECT slug, display_name, health_status, verification_status,
               error_rate, total_calls, total_errors,
               last_call_at, last_success_at, last_error_at, last_verified_at
          FROM connector_registry
         WHERE status = 'active'
         ORDER BY slug
        """
    )
    snapshots: list[ProbeSnapshot] = []
    for row in rows:
        slug = row["slug"]
        error_rate = float(row["error_rate"] or 0.0)
        health_status = row["health_status"] or "unknown"
        verification_status = row["verification_status"] or "unverified"

        if health_status == "healthy" and error_rate <= 0.1:
            status = "ok"
        elif error_rate > 0.5 or health_status == "unhealthy":
            status = "failed"
        elif error_rate > 0.1 or health_status == "degraded":
            status = "degraded"
        else:
            status = "ok" if verification_status in ("verified", "partial") else "warning"

        snapshots.append(
            ProbeSnapshot(
                probe_kind="connector_liveness",
                subject_id=slug,
                status=status,
                summary=(
                    f"{slug}: health={health_status}, verify={verification_status}, "
                    f"error_rate={error_rate:.2%}"
                ),
                details={
                    "display_name": row["display_name"],
                    "health_status": health_status,
                    "verification_status": verification_status,
                    "error_rate": error_rate,
                    "total_calls": int(row["total_calls"] or 0),
                    "total_errors": int(row["total_errors"] or 0),
                    "last_call_at": row["last_call_at"].isoformat() if row["last_call_at"] else None,
                    "last_success_at": row["last_success_at"].isoformat() if row["last_success_at"] else None,
                    "last_error_at": row["last_error_at"].isoformat() if row["last_error_at"] else None,
                    "last_verified_at": row["last_verified_at"].isoformat() if row["last_verified_at"] else None,
                },
            )
        )
    return snapshots


# ---------------------------------------------------------------------------
# Credential expiry probe
# ---------------------------------------------------------------------------

def _keychain_present(env_var: str) -> bool:
    try:
        proc = subprocess.run(
            ["security", "find-generic-password", "-a", "praxis", "-s", env_var, "-w"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return proc.returncode == 0 and bool((proc.stdout or "").strip())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


async def probe_credentials(conn: asyncpg.Connection) -> list[ProbeSnapshot]:
    snapshots: list[ProbeSnapshot] = []

    # Provider API keys: enumerate from provider_cli_profiles.api_key_env_vars
    profile_rows = await conn.fetch(
        """
        SELECT provider_slug, api_key_env_vars
          FROM provider_cli_profiles
         WHERE status = 'active' AND api_key_env_vars IS NOT NULL
         ORDER BY provider_slug
        """
    )
    for row in profile_rows:
        env_vars = _coerce_json_list(row["api_key_env_vars"])
        for env_var in env_vars:
            env_var = str(env_var).strip()
            if not env_var:
                continue
            keychain_has = await asyncio.to_thread(_keychain_present, env_var)
            env_has = bool(os.environ.get(env_var))
            present = keychain_has or env_has
            source_kind = "keychain" if keychain_has else ("env" if env_has else "missing")
            status = "ok" if present else "failed"
            snapshots.append(
                ProbeSnapshot(
                    probe_kind="credential_expiry",
                    subject_id=env_var,
                    subject_sub="api_key",
                    status=status,
                    summary=f"{env_var}: {source_kind}",
                    details={
                        "present": present,
                        "source_kind": source_kind,
                        "provider_slug": row["provider_slug"],
                    },
                )
            )

    # OAuth tokens: from credential_tokens
    token_rows = await conn.fetch(
        """
        SELECT integration_id, token_kind, expires_at, scopes, updated_at
          FROM credential_tokens
         WHERE token_kind = 'access'
         ORDER BY integration_id
        """
    )
    now = datetime.now(timezone.utc)
    for row in token_rows:
        integration_id = row["integration_id"]
        expires_at = row["expires_at"]
        if expires_at is None:
            snapshots.append(
                ProbeSnapshot(
                    probe_kind="credential_expiry",
                    subject_id=integration_id,
                    subject_sub="oauth_access",
                    status="ok",
                    summary=f"{integration_id}: oauth token, no expiry",
                    details={
                        "present": True,
                        "source_kind": "db",
                        "integration_id": integration_id,
                    },
                )
            )
            continue

        delta = expires_at - now
        days_until = int(delta.total_seconds() // 86400)

        if days_until < 0:
            status = "failed"
            summary = f"{integration_id}: oauth token EXPIRED {-days_until}d ago"
        elif days_until <= _CREDENTIAL_EXPIRY_WARNING_DAYS:
            status = "degraded"
            summary = f"{integration_id}: oauth token expires in {days_until}d"
        else:
            status = "ok"
            summary = f"{integration_id}: oauth token valid ({days_until}d remaining)"

        snapshots.append(
            ProbeSnapshot(
                probe_kind="credential_expiry",
                subject_id=integration_id,
                subject_sub="oauth_access",
                status=status,
                summary=summary,
                days_until_expiry=days_until,
                details={
                    "present": True,
                    "source_kind": "db",
                    "integration_id": integration_id,
                    "expires_at": expires_at.isoformat(),
                    "scopes": row["scopes"],
                },
            )
        )

    return snapshots


# ---------------------------------------------------------------------------
# MCP server liveness probe
# ---------------------------------------------------------------------------

def _read_mcp_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _probe_stdio_mcp_server(name: str, spec: Mapping[str, Any], *, timeout_s: int) -> ProbeSnapshot:
    command = spec.get("command")
    args = list(spec.get("args") or [])
    env_extra = dict(spec.get("env") or {})
    if not command:
        return ProbeSnapshot(
            probe_kind="mcp_liveness",
            subject_id=name,
            status="skipped",
            summary=f"{name}: no command in mcp.json",
            details={"transport": "stdio"},
        )

    cmd = [command, *args]
    env = {**os.environ, **env_extra}
    started = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
        )
    except (FileNotFoundError, OSError) as exc:
        return ProbeSnapshot(
            probe_kind="mcp_liveness",
            subject_id=name,
            status="failed",
            summary=f"{name}: failed to spawn — {exc}",
            details={"transport": "stdio", "reachable": False, "error_message": str(exc)[:200]},
        )

    initialize_req = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "praxis-daily-heartbeat", "version": "1.0"},
        },
    }) + "\n"

    try:
        proc.stdin.write(initialize_req)  # type: ignore[union-attr]
        proc.stdin.flush()  # type: ignore[union-attr]
        deadline = started + timeout_s
        response_line = ""
        while time.monotonic() < deadline:
            line = proc.stdout.readline() if proc.stdout else ""  # type: ignore[union-attr]
            if not line:
                time.sleep(0.05)
                continue
            line = line.strip()
            if not line.startswith("{"):
                continue
            response_line = line
            break
        latency = int((time.monotonic() - started) * 1000)
        if not response_line:
            return ProbeSnapshot(
                probe_kind="mcp_liveness",
                subject_id=name,
                status="failed",
                summary=f"{name}: no initialize response within {timeout_s}s",
                latency_ms=latency,
                details={"transport": "stdio", "reachable": True, "handshake_succeeded": False},
            )
        try:
            payload = json.loads(response_line)
        except json.JSONDecodeError as exc:
            return ProbeSnapshot(
                probe_kind="mcp_liveness",
                subject_id=name,
                status="failed",
                summary=f"{name}: malformed initialize response",
                latency_ms=latency,
                details={
                    "transport": "stdio",
                    "reachable": True,
                    "handshake_succeeded": False,
                    "error_message": str(exc)[:200],
                    "raw_excerpt": response_line[:200],
                },
            )

        result = (payload or {}).get("result") or {}
        server_info = result.get("serverInfo") or {}
        server_version = server_info.get("version")
        return ProbeSnapshot(
            probe_kind="mcp_liveness",
            subject_id=name,
            status="ok",
            summary=f"{name}: stdio handshake ok (v{server_version or 'unknown'}, {latency}ms)",
            latency_ms=latency,
            details={
                "transport": "stdio",
                "reachable": True,
                "handshake_succeeded": True,
                "server_version": server_version,
                "server_name": server_info.get("name"),
                "protocol_version": result.get("protocolVersion"),
            },
        )
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception:  # noqa: BLE001
            pass


async def probe_mcp_servers(*, timeout_s: int, mcp_config_path: Path | None = None) -> list[ProbeSnapshot]:
    path = mcp_config_path or (_REPO_ROOT / ".mcp.json")
    config = _read_mcp_config(path)
    servers = config.get("mcpServers") or {}
    if not isinstance(servers, dict) or not servers:
        return []

    coroutines = []
    for name, spec in servers.items():
        if not isinstance(spec, dict):
            continue
        transport = str(spec.get("type") or "stdio").lower()
        if transport == "stdio":
            coroutines.append(asyncio.to_thread(_probe_stdio_mcp_server, name, spec, timeout_s=timeout_s))
        else:
            coroutines.append(asyncio.to_thread(
                lambda n=name, s=spec, t=transport: ProbeSnapshot(
                    probe_kind="mcp_liveness",
                    subject_id=n,
                    status="skipped",
                    summary=f"{n}: transport {t} not yet probed",
                    details={"transport": t},
                )
            ))
    results = await asyncio.gather(*coroutines, return_exceptions=True)
    snapshots: list[ProbeSnapshot] = []
    for name, result in zip(list(servers.keys()), results, strict=True):
        if isinstance(result, BaseException):
            snapshots.append(ProbeSnapshot(
                probe_kind="mcp_liveness",
                subject_id=name,
                status="failed",
                summary=f"{name}: probe raised {result}",
                details={"error_message": str(result)[:200]},
            ))
        else:
            snapshots.append(result)
    return snapshots


# ---------------------------------------------------------------------------
# Provider model retirement detector
# ---------------------------------------------------------------------------

async def probe_model_retirements(
    conn: asyncpg.Connection,
    *,
    env: Mapping[str, str] | None = None,
    dry_run: bool = False,
) -> list[ProbeSnapshot]:
    """Run the automatic retirement scanner and emit one snapshot per finding,
    plus one rollup snapshot per provider (so providers with no findings still
    show up as ``ok`` and the timeseries reflects every cycle).

    By default the heartbeat applies retirements (``dry_run=False``) — the
    scanner has its own safety guards (skip on discovery error, skip if >50%
    of registered models would be dropped) so the heartbeat doesn't need to.
    """
    from registry.provider_model_retirement import scan_provider_model_retirements

    report = await scan_provider_model_retirements(
        conn,
        env=env or _probe_env(),
        dry_run=dry_run,
    )
    snapshots: list[ProbeSnapshot] = []
    for outcome in report.outcomes:
        # Per-provider rollup: a single row showing what mode was used, how
        # many models we knew about, and how many the live source returned.
        snapshots.append(
            ProbeSnapshot(
                probe_kind="model_retirement",
                subject_id=outcome.provider_slug,
                subject_sub="provider_rollup",
                status=outcome.status,
                summary=outcome.summary,
                details={
                    "discovery_mode": outcome.discovery_mode,
                    "registered_count": outcome.registered_count,
                    "live_count": outcome.live_count,
                    "findings_count": len(outcome.findings),
                    "error": outcome.error,
                    "dry_run": report.dry_run,
                },
            )
        )
        # One row per stale model for queryable history.
        for finding in outcome.findings:
            snapshots.append(
                ProbeSnapshot(
                    probe_kind="model_retirement",
                    subject_id=finding.provider_slug,
                    subject_sub=finding.model_slug,
                    status="failed",
                    summary=(
                        f"{finding.provider_slug}/{finding.model_slug}: "
                        f"retired via {finding.source}"
                    ),
                    details={
                        "source": finding.source,
                        "reason": finding.reason,
                        "ledger_kind": finding.ledger_kind,
                        "effective_date": finding.effective_date,
                        "applied": (
                            not report.dry_run
                            and (
                                finding.source == "api_discovery"
                                or finding.ledger_kind == "retired"
                            )
                        ),
                    },
                )
            )
    return snapshots


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

_INSERT_RUN_SQL = """
INSERT INTO heartbeat_runs (
    heartbeat_run_id, scope, triggered_by, started_at, status, details
) VALUES ($1, $2, $3, $4, 'running', '{}'::jsonb)
"""

_UPDATE_RUN_SQL = """
UPDATE heartbeat_runs
   SET completed_at = $2,
       status = $3,
       probes_total = $4,
       probes_ok = $5,
       probes_failed = $6,
       summary = $7,
       details = $8::jsonb
 WHERE heartbeat_run_id = $1
"""

_INSERT_SNAPSHOT_SQL = """
INSERT INTO heartbeat_probe_snapshots (
    heartbeat_probe_snapshot_id, heartbeat_run_id,
    probe_kind, subject_id, subject_sub,
    status, summary, latency_ms,
    input_tokens, output_tokens, estimated_cost_usd, days_until_expiry,
    details
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
"""


def _run_id(started: datetime, scope: str) -> str:
    suffix = uuid.uuid4().hex[:8]
    return f"heartbeat_run.{scope}.{started.strftime('%Y%m%dT%H%M%SZ')}.{suffix}"


def _snapshot_id(run_id: str, index: int, snap: ProbeSnapshot) -> str:
    return f"heartbeat_snapshot.{run_id}.{snap.probe_kind}.{snap.subject_id}.{index:04d}"


async def _persist_snapshots(
    conn: asyncpg.Connection,
    run_id: str,
    snapshots: Sequence[ProbeSnapshot],
) -> None:
    for index, snap in enumerate(snapshots, start=1):
        cost = Decimal(str(snap.estimated_cost_usd)) if snap.estimated_cost_usd is not None else None
        await conn.execute(
            _INSERT_SNAPSHOT_SQL,
            _snapshot_id(run_id, index, snap),
            run_id,
            snap.probe_kind,
            snap.subject_id,
            snap.subject_sub,
            snap.status,
            snap.summary or "",
            snap.latency_ms,
            snap.input_tokens,
            snap.output_tokens,
            cost,
            snap.days_until_expiry,
            json.dumps(snap.details or {}, default=str),
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _resolved_scopes(scope: HeartbeatScope) -> tuple[HeartbeatScope, ...]:
    if scope == "all":
        return _SCOPES
    return (scope,)


async def run_daily_heartbeat(
    *,
    scope: HeartbeatScope = "all",
    triggered_by: TriggeredBy = "cli",
    env: Mapping[str, str] | None = None,
    timeouts_s: Mapping[str, int] | None = None,
    mcp_config_path: Path | None = None,
    provider_slugs: Sequence[str] | None = None,
    adapter_types: Sequence[str] | None = None,
    provider_concurrency: int = 4,
) -> HeartbeatRunResult:
    """Run one heartbeat cycle across the requested scope(s)."""
    started = datetime.now(timezone.utc)
    run_id = _run_id(started, scope)
    timeouts = dict(_DEFAULT_TIMEOUTS_S)
    if timeouts_s:
        timeouts.update(timeouts_s)

    conn = await connect_workflow_database(env)
    snapshots: list[ProbeSnapshot] = []
    errors: list[str] = []
    try:
        await conn.execute(_INSERT_RUN_SQL, run_id, scope, triggered_by, started)

        scope_tasks: dict[HeartbeatScope, Any] = {}
        for s in _resolved_scopes(scope):
            if s == "providers":
                provider_kwargs: dict[str, Any] = {"timeout_s": timeouts["providers"]}
                if provider_slugs is not None:
                    provider_kwargs["provider_slugs"] = provider_slugs
                if adapter_types is not None:
                    provider_kwargs["adapter_types"] = adapter_types
                if provider_concurrency != 4:
                    provider_kwargs["max_concurrency"] = provider_concurrency
                scope_tasks[s] = probe_providers(
                    conn,
                    **provider_kwargs,
                )
            elif s == "connectors":
                scope_tasks[s] = probe_connectors(conn)
            elif s == "credentials":
                scope_tasks[s] = probe_credentials(conn)
            elif s == "mcp":
                scope_tasks[s] = probe_mcp_servers(timeout_s=timeouts["mcp"], mcp_config_path=mcp_config_path)
            elif s == "model_retirement":
                scope_tasks[s] = probe_model_retirements(conn, env=env)

        for s_name, coro in scope_tasks.items():
            try:
                result = await coro
                snapshots.extend(result)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{s_name}: {exc}")
                logger.exception("heartbeat scope %s failed", s_name)

        await _persist_snapshots(conn, run_id, snapshots)

        probes_total = len(snapshots)
        probes_failed = sum(1 for s in snapshots if s.status == "failed")
        probes_ok = sum(1 for s in snapshots if s.status == "ok")

        if errors:
            overall_status = "failed" if not snapshots else "partial"
        elif probes_failed == 0:
            overall_status = "succeeded"
        elif probes_ok > 0:
            overall_status = "partial"
        else:
            overall_status = "failed"

        completed = datetime.now(timezone.utc)
        summary_parts = [
            f"scope={scope}",
            f"total={probes_total}",
            f"ok={probes_ok}",
            f"failed={probes_failed}",
        ]
        if errors:
            summary_parts.append(f"errors={len(errors)}")
        summary = " ".join(summary_parts)

        await conn.execute(
            _UPDATE_RUN_SQL,
            run_id,
            completed,
            overall_status,
            probes_total,
            probes_ok,
            probes_failed,
            summary,
            json.dumps({"scope_errors": errors}, default=str),
        )

        return HeartbeatRunResult(
            heartbeat_run_id=run_id,
            scope=scope,
            triggered_by=triggered_by,
            started_at=started,
            completed_at=completed,
            status=overall_status,
            probes_total=probes_total,
            probes_ok=probes_ok,
            probes_failed=probes_failed,
            summary=summary,
            snapshots=snapshots,
        )
    finally:
        await conn.close()
