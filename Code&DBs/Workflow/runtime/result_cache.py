"""Content-addressed result caching for dispatch executions.

The canonical cache authority is Postgres. File-backed cache mode only exists as
an explicit compatibility override via ``cache_dir`` or ``PRAXIS_WORKFLOW_CACHE_DIR``.
Only successful (status="succeeded") workflow results are cached.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from storage.postgres import SyncPostgresConnection, ensure_postgres_available

if TYPE_CHECKING:
    from .workflow import WorkflowResult, WorkflowSpec

_ENV_CACHE_DIR = "PRAXIS_WORKFLOW_CACHE_DIR"


class CacheKey:
    """Compute content-addressed cache keys from workflow specs."""

    _VERSION = "workflow-result-cache-key:v2"

    @staticmethod
    def compute(spec: WorkflowSpec) -> str:
        payload = {
            "version": CacheKey._VERSION,
            "workflow_spec": {
                "acceptance_contract": spec.acceptance_contract,
                "adapter_type": spec.adapter_type,
                "allowed_tools": spec.allowed_tools,
                "authoring_contract": spec.authoring_contract,
                "capabilities": spec.capabilities,
                "context_sections": spec.context_sections,
                "definition_revision": spec.definition_revision,
                "label": spec.label,
                "max_context_tokens": spec.max_context_tokens,
                "max_retries": spec.max_retries,
                "max_tokens": spec.max_tokens,
                "model_slug": spec.model_slug,
                "output_schema": spec.output_schema,
                "packet_provenance": spec.packet_provenance,
                "parent_run_id": spec.parent_run_id,
                "persist": spec.persist,
                "plan_revision": spec.plan_revision,
                "prefer_cost": spec.prefer_cost,
                "prompt": spec.prompt,
                "provider_slug": spec.provider_slug,
                "review_target_modules": spec.review_target_modules,
                "reviews_workflow_id": spec.reviews_workflow_id,
                "runtime_profile_ref": spec.runtime_profile_ref,
                "scope_read": spec.scope_read,
                "scope_write": spec.scope_write,
                "skip_auto_review": spec.skip_auto_review,
                "submission_required": spec.submission_required,
                "system_prompt": spec.system_prompt,
                "task_type": spec.task_type,
                "temperature": spec.temperature,
                "tier": spec.tier,
                "timeout": spec.timeout,
                "use_cache": spec.use_cache,
                "verify_refs": spec.verify_refs,
                "workdir": spec.workdir,
                "workspace_ref": spec.workspace_ref,
            },
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def validate(key: str) -> bool:
        return isinstance(key, str) and len(key) == 64 and all(c in "0123456789abcdef" for c in key)


@dataclass(frozen=True)
class CachedResultEntry:
    key: str
    cached_at: datetime
    expires_at: datetime
    status: str
    reason_code: str
    completion: str | None
    outputs: dict[str, Any]
    evidence_count: int
    latency_ms: int
    provider_slug: str
    model_slug: str | None
    adapter_type: str
    failure_code: str | None

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at

    def to_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "cached_at": self.cached_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "status": self.status,
            "reason_code": self.reason_code,
            "completion": self.completion,
            "outputs": self.outputs,
            "evidence_count": self.evidence_count,
            "latency_ms": self.latency_ms,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "adapter_type": self.adapter_type,
            "failure_code": self.failure_code,
        }


class ResultCache:
    """Content-addressed result cache for dispatch executions."""

    def __init__(
        self,
        *,
        cache_dir: str | None = None,
        conn: SyncPostgresConnection | None = None,
    ) -> None:
        if cache_dir is not None or os.environ.get(_ENV_CACHE_DIR):
            raise RuntimeError(
                "File-backed result cache has been removed; Postgres authority is required."
            )
        self._conn = conn or ensure_postgres_available()

    def _require_conn(self) -> SyncPostgresConnection:
        if self._conn is None:
            raise RuntimeError("ResultCache requires Postgres authority when no explicit cache_dir is configured")
        return self._conn

    @staticmethod
    def _entry_from_row(row: Any) -> CachedResultEntry:
        outputs = row.get("outputs")
        if isinstance(outputs, str):
            outputs = json.loads(outputs)
        if not isinstance(outputs, dict):
            outputs = {}
        return CachedResultEntry(
            key=str(row.get("cache_key") or row.get("key") or ""),
            cached_at=row["cached_at"],
            expires_at=row["expires_at"],
            status=str(row.get("status") or "succeeded"),
            reason_code=str(row.get("reason_code") or ""),
            completion=row.get("completion"),
            outputs=outputs,
            evidence_count=int(row.get("evidence_count") or 0),
            latency_ms=int(row.get("latency_ms") or 0),
            provider_slug=str(row.get("provider_slug") or ""),
            model_slug=row.get("model_slug"),
            adapter_type=str(row.get("adapter_type") or ""),
            failure_code=row.get("failure_code"),
        )

    @staticmethod
    def _result_from_entry(entry: CachedResultEntry) -> WorkflowResult:
        from .workflow import WorkflowResult

        return WorkflowResult(
            run_id=f"cached:{entry.key[:8]}",
            status=entry.status,
            reason_code=entry.reason_code,
            completion=entry.completion,
            outputs=entry.outputs,
            evidence_count=entry.evidence_count,
            started_at=entry.cached_at,
            finished_at=entry.cached_at + timedelta(milliseconds=entry.latency_ms),
            latency_ms=entry.latency_ms,
            provider_slug=entry.provider_slug,
            model_slug=entry.model_slug,
            adapter_type=entry.adapter_type,
            failure_code=entry.failure_code,
        )

    @staticmethod
    def _entry_from_result(key: str, result: WorkflowResult, *, ttl_hours: float) -> CachedResultEntry:
        now = datetime.now(timezone.utc)
        return CachedResultEntry(
            key=key,
            cached_at=now,
            expires_at=now + timedelta(hours=ttl_hours),
            status=result.status,
            reason_code=result.reason_code,
            completion=result.completion,
            outputs=dict(result.outputs),
            evidence_count=result.evidence_count,
            latency_ms=result.latency_ms,
            provider_slug=result.provider_slug,
            model_slug=result.model_slug,
            adapter_type=result.adapter_type,
            failure_code=result.failure_code,
        )

    def get(self, key: str) -> WorkflowResult | None:
        if not CacheKey.validate(key):
            return None

        rows = self._require_conn().execute(
            """
            SELECT cache_key, cached_at, expires_at, status, reason_code, completion,
                   outputs, evidence_count, latency_ms, provider_slug, model_slug,
                   adapter_type, failure_code
              FROM workflow_result_cache
             WHERE cache_key = $1
               AND expires_at > now()
             LIMIT 1
            """,
            key,
        )
        if not rows:
            return None
        entry = self._entry_from_row(rows[0])
        return None if entry.is_expired() else self._result_from_entry(entry)

    def put(self, key: str, result: WorkflowResult, *, ttl_hours: float = 24.0) -> None:
        if result.status != "succeeded":
            return
        if not CacheKey.validate(key):
            raise ValueError(f"invalid cache key: {key}")

        entry = self._entry_from_result(key, result, ttl_hours=ttl_hours)
        payload = entry.to_json()
        payload_bytes = len(json.dumps(payload, sort_keys=True, default=str).encode("utf-8"))

        self._require_conn().execute(
            """
            INSERT INTO workflow_result_cache (
                cache_key, cached_at, expires_at, status, reason_code, completion,
                outputs, evidence_count, latency_ms, provider_slug, model_slug,
                adapter_type, failure_code, payload_bytes
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7::jsonb, $8, $9, $10, $11,
                $12, $13, $14
            )
            ON CONFLICT (cache_key) DO UPDATE SET
                cached_at = EXCLUDED.cached_at,
                expires_at = EXCLUDED.expires_at,
                status = EXCLUDED.status,
                reason_code = EXCLUDED.reason_code,
                completion = EXCLUDED.completion,
                outputs = EXCLUDED.outputs,
                evidence_count = EXCLUDED.evidence_count,
                latency_ms = EXCLUDED.latency_ms,
                provider_slug = EXCLUDED.provider_slug,
                model_slug = EXCLUDED.model_slug,
                adapter_type = EXCLUDED.adapter_type,
                failure_code = EXCLUDED.failure_code,
                payload_bytes = EXCLUDED.payload_bytes
            """,
            entry.key,
            entry.cached_at,
            entry.expires_at,
            entry.status,
            entry.reason_code,
            entry.completion,
            json.dumps(entry.outputs, sort_keys=True, default=str),
            entry.evidence_count,
            entry.latency_ms,
            entry.provider_slug,
            entry.model_slug,
            entry.adapter_type,
            entry.failure_code,
            payload_bytes,
        )

    def compute_key(self, spec: WorkflowSpec) -> str:
        return CacheKey.compute(spec)

    def stats(self) -> dict[str, Any]:
        rows = self._require_conn().execute(
            """
            SELECT COUNT(*) AS cached_entries,
                   COALESCE(SUM(payload_bytes), 0) AS cache_size_bytes,
                   MIN(cached_at) AS oldest_cached,
                   MAX(cached_at) AS newest_cached
              FROM workflow_result_cache
             WHERE expires_at > now()
            """
        )
        row = rows[0] if rows else {}
        entries: list[datetime] = []
        if row.get("oldest_cached") is not None:
            entries.append(row["oldest_cached"])
        if row.get("newest_cached") is not None and row.get("newest_cached") != row.get("oldest_cached"):
            entries.append(row["newest_cached"])
        stats = self._stats_payload(
            cached_entries=int(row.get("cached_entries") or 0),
            entries=entries,
            total_size=int(row.get("cache_size_bytes") or 0),
        )
        return stats

    @staticmethod
    def _stats_payload(
        *,
        cached_entries: int,
        entries: list[datetime],
        total_size: int,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        oldest_age = None
        newest_age = None
        if entries:
            ordered = sorted(entries)
            oldest_age = (now - ordered[0]).total_seconds() / 3600.0
            newest_age = (now - ordered[-1]).total_seconds() / 3600.0
        return {
            "cached_entries": cached_entries,
            "cache_size_bytes": total_size,
            "oldest_entry_age_hours": oldest_age,
            "newest_entry_age_hours": newest_age,
        }

    def clear(self, *, older_than_hours: float | None = None) -> int:
        if older_than_hours is None:
            rows = self._require_conn().execute(
                "DELETE FROM workflow_result_cache RETURNING cache_key"
            )
            return len(rows)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        rows = self._require_conn().execute(
            "DELETE FROM workflow_result_cache WHERE cached_at < $1 RETURNING cache_key",
            cutoff,
        )
        return len(rows)


_CACHE: ResultCache | None = None
_CACHE_LOCK = threading.Lock()


def get_result_cache(*, cache_dir: str | None = None) -> ResultCache:
    global _CACHE
    if cache_dir is not None:
        return ResultCache(cache_dir=cache_dir)
    if _CACHE is None:
        with _CACHE_LOCK:
            if _CACHE is None:
                _CACHE = ResultCache()
    return _CACHE
