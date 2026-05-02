"""Workflow preflight, cache, and retry policy helpers."""

from __future__ import annotations

import os
import sys
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Callable

from runtime.failure_classifier import classify_failure

from . import _capabilities as _workflow_caps

if TYPE_CHECKING:
    from ._workflow_execution import WorkflowExecutionContext
    from .orchestrator import WorkflowResult, WorkflowSpec


_MAX_BACKOFF_S = int(os.environ.get("PRAXIS_MAX_RETRY_BACKOFF", "30"))

_TIER_ESCALATION: dict[str, str] = {
    "economy": "mid",
    "mid": "frontier",
}


def apply_workflow_preflight(
    spec: WorkflowSpec,
    *,
    context: WorkflowExecutionContext,
    run_id_factory: Callable[[], str],
) -> WorkflowResult | None:
    """Return an early workflow result for circuit-open or cache-hit preflight."""

    provider_slug = str(spec.provider_slug or "").strip().lower()
    if provider_slug and provider_slug != "integration":
        if _workflow_caps.CIRCUIT_BREAKERS is None:
            return context.failure_result(
                run_id=run_id_factory(),
                reason_code="circuit_breaker.unavailable",
                failure_code="circuit_breaker.unavailable",
                outputs={
                    "error": (
                        f"Circuit breaker authority unavailable for {provider_slug}"
                    ),
                },
            )
        try:
            allowed = _workflow_caps.CIRCUIT_BREAKERS.allow_request(provider_slug)
        except Exception as exc:
            return context.failure_result(
                run_id=run_id_factory(),
                reason_code="circuit_breaker.unavailable",
                failure_code="circuit_breaker.unavailable",
                outputs={
                    "error": (
                        f"Circuit breaker authority unavailable for {provider_slug}: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                },
            )
        if not allowed:
            return context.failure_result(
                run_id=run_id_factory(),
                reason_code="circuit_breaker.open",
                failure_code="rate_limited",
                outputs={"error": f"Circuit breaker open for {provider_slug}"},
            )

    cached_result = _load_cached_workflow_result(spec)
    if cached_result is not None:
        return cached_result

    return None


def cache_workflow_result(spec: WorkflowSpec, result: WorkflowResult) -> None:
    """Persist a successful cacheable workflow result when the cache is available."""

    if not spec.use_cache or result.status != "succeeded" or not _workflow_caps.WORKFLOW_CAPABILITIES.result_cache:
        return
    cache = _workflow_caps.WORKFLOW_CAPABILITIES.result_cache()
    cache_key = cache.compute_key(spec)
    cache.put(cache_key, result, ttl_hours=24.0)


def run_workflow_with_retry(
    spec: WorkflowSpec,
    *,
    dispatch_once: Callable[[WorkflowSpec], WorkflowResult],
    emit_started: Callable[..., None],
    emit_finished: Callable[..., None],
    record_result: Callable[..., None],
    sleep_fn: Callable[[float], None] = time.sleep,
    stderr=None,
) -> WorkflowResult:
    """Execute a workflow with retry and tier-escalation policy."""

    stderr = stderr or sys.stderr
    max_attempts = 1 + max(spec.max_retries, 0)
    result = dispatch_once(spec)
    attempt = 1

    emit_started(spec=spec, run_id=result.run_id)

    while attempt < max_attempts and is_retryable(result):
        delay = backoff_seconds(attempt - 1)
        print(
            f"[workflow] attempt {attempt}/{max_attempts} failed "
            f"({result.failure_code}), retrying in {delay:.1f}s",
            file=stderr,
        )
        sleep_fn(delay)
        attempt += 1
        result = dispatch_once(spec)

    if result.status == "failed" and is_retryable(result):
        escalated_spec = escalate_tier(spec)
        if escalated_spec is not None:
            print(
                f"[workflow] escalating from {spec.tier} to {escalated_spec.tier} "
                f"after {attempt} failed attempts",
                file=stderr,
            )
            escalated_result = dispatch_once(escalated_spec)
            attempt += 1
            if escalated_result.status == "succeeded":
                result = escalated_result

    result = with_attempts(result, attempt)
    record_result(result, spec=spec)
    emit_finished(spec=spec, result=result)
    return result


def is_retryable(result: WorkflowResult) -> bool:
    """Return True if the failure code indicates a transient error worth retrying."""

    if result.status != "failed":
        return False
    classification = classify_failure(result.failure_code, outputs=dict(result.outputs))
    return classification.is_retryable


def with_attempts(result: WorkflowResult, attempts: int) -> WorkflowResult:
    """Return a copy of result with the attempts count set."""

    if result.attempts == attempts:
        return result
    return replace(result, attempts=attempts)


def escalate_tier(spec: WorkflowSpec) -> WorkflowSpec | None:
    """Return a new spec with the tier escalated one level, or None if already at top."""

    next_tier = _TIER_ESCALATION.get(spec.tier or "")
    if next_tier is None:
        return None
    return replace(spec, tier=next_tier, model_slug=None)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff: 1s * 2^attempt, capped at _MAX_BACKOFF_S."""

    return min(1.0 * (2**attempt), _MAX_BACKOFF_S)


def _load_cached_workflow_result(spec: WorkflowSpec) -> WorkflowResult | None:
    if not spec.use_cache or not _workflow_caps.WORKFLOW_CAPABILITIES.result_cache:
        return None
    cache = _workflow_caps.WORKFLOW_CAPABILITIES.result_cache()
    cache_key = cache.compute_key(spec)
    cached_result = cache.get(cache_key)
    if cached_result is None:
        return None
    outputs = dict(cached_result.outputs)
    outputs["cache_hit"] = True
    return replace(cached_result, outputs=outputs)
