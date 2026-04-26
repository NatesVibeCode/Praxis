"""Shared constants and utilities for the unified workflow runtime."""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import TYPE_CHECKING

from runtime.native_authority import default_native_authority_refs
from runtime.provider_authority import provider_authority_fail
from runtime.circuit_breaker import get_circuit_breakers

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = [
    "STALE_REAPER_QUERY",
    "_ACTIVE_JOB_STATUSES",
    "_BLOCKING_PARENT_STATUSES",
    "_CIRCUIT_BREAKERS",
    "_CIRCUIT_BREAKERS_UNSET",
    "_default_native_runtime_profile_ref",
    "_default_native_workspace_ref",
    "_MAX_INT32",
    "_READ_ONLY_MODE",
    "_TERMINAL_JOB_STATUSES",
    "_WORKFLOW_REPLAYABLE_RUN_STATES",
    "_WORKFLOW_TERMINAL_STATES",
    "_WRITE_MODE",
    "_circuit_breakers",
    "_definition_version_for_hash",
    "_job_artifact_basename",
    "_json_loads_maybe",
    "_json_safe",
    "_normalize_paths",
    "_normalize_string_list",
    "_slugify",
    "_workflow_id_for_spec",
]

# ── Constants ────────────────────────────────────────────────────────────

_TERMINAL_JOB_STATUSES = {"succeeded", "failed", "dead_letter", "cancelled", "blocked"}
_ACTIVE_JOB_STATUSES = {"claimed", "running"}
_BLOCKING_PARENT_STATUSES = {"failed", "dead_letter", "cancelled", "blocked"}
_WORKFLOW_TERMINAL_STATES = {"succeeded", "failed", "dead_letter", "cancelled"}
_WORKFLOW_REPLAYABLE_RUN_STATES = {"queued", "running", "claim_accepted", "succeeded"}
_READ_ONLY_MODE = "read"
_WRITE_MODE = "write"
_MAX_INT32 = 2_147_483_647
_CIRCUIT_BREAKERS_UNSET = object()
_CIRCUIT_BREAKERS = _CIRCUIT_BREAKERS_UNSET

# When True, outcomes are logged but do NOT mutate route health scores
# or circuit breaker state.  Flip to False once routing data is trusted.
ROUTING_METRICS_FROZEN = True

STALE_REAPER_QUERY = """
UPDATE workflow_jobs
SET    status = 'ready', claimed_by = NULL, claimed_at = NULL,
       failure_category = '', failure_zone = '', is_transient = false
WHERE  status IN ('claimed', 'running')
AND    COALESCE(heartbeat_at, claimed_at) < now() - interval '5 minutes'
AND    (next_retry_at IS NULL OR next_retry_at <= now())
RETURNING id, label, run_id;
"""


# ── Utilities ────────────────────────────────────────────────────────────

def _circuit_breakers():
    if _CIRCUIT_BREAKERS is not _CIRCUIT_BREAKERS_UNSET:
        return _CIRCUIT_BREAKERS
    try:
        return get_circuit_breakers()
    except OSError as exc:
        raise provider_authority_fail(
            "provider_authority.circuit_breaker_unavailable",
            f"circuit breaker gate unavailable: {exc}",
        ) from exc
    except RuntimeError as exc:
        message = str(exc)
        if "requires explicit WORKFLOW_DATABASE_URL Postgres authority" not in message:
            raise
        raise provider_authority_fail(
            "provider_authority.circuit_breaker_unavailable",
            f"circuit breaker gate unavailable: {message}",
        ) from exc


def _default_native_workspace_ref(
    conn: "SyncPostgresConnection | None" = None,
) -> str:
    return default_native_authority_refs(conn)[0]


def _default_native_runtime_profile_ref(
    conn: "SyncPostgresConnection | None" = None,
) -> str:
    return default_native_authority_refs(conn)[1]


def _json_loads_maybe(value, default):
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return value


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _workflow_run_envelope(run_row: dict) -> dict:
    return _json_loads_maybe(run_row.get("request_envelope"), {}) or {}


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", ".", value.strip().lower())
    return cleaned.strip(".") or "unnamed"


def _job_artifact_basename(prefix: str, run_id: str, job_id: int, label: str, suffix: str) -> str:
    return f"{prefix}_{run_id}_job_{job_id}_{_slugify(label)}{suffix}"


def _workflow_id_for_spec(spec) -> str:
    raw = (
        getattr(spec, "workflow_id", "")
        or getattr(spec, "name", "")
    )
    if isinstance(raw, str) and raw.startswith("workflow."):
        return raw
    return f"workflow.{_slugify(str(raw or 'run'))}"


def _normalize_paths(raw) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, (list, tuple, set)):
        return [str(item) for item in raw if item]
    return []


def _definition_version_for_hash(definition_hash: str) -> int:
    """Derive a deterministic positive int32-safe definition version."""
    raw_text = str(definition_hash or "").strip()
    digest_text = raw_text
    if digest_text.lower().startswith("sha256:"):
        digest_text = digest_text.split(":", 1)[1]
    hex_text = "".join(re.findall(r"[0-9a-fA-F]+", digest_text))
    if len(hex_text) < 16:
        hex_text = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    seed = int(hex_text[:16], 16)
    return (seed % (_MAX_INT32 - 1)) + 1


def _normalize_string_list(values: object) -> list[str]:
    if not isinstance(values, (list, tuple)):
        return []
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text:
            result.append(text)
    return list(dict.fromkeys(result))
