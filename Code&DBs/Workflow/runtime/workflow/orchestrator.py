"""End-to-end workflow executor.

Takes a workflow spec (prompt + routing hints) and runs the full pipeline:
spec → intake → route → context compile → execute → output write → evidence → terminal state.

Architecture: Models are stdin/stdout workers. The graph owns all I/O.
  1. Graph reads files (scope_resolver) → compiles context (context_compiler/prompt_renderer)
  2. Context injected into prompt via stdin
  3. Model produces structured output (code as text, JSON, diffs)
  4. Graph captures output and decides: write file / feed to next node / review / promote

This is the module that makes the platform actually do work.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

from ..prompt_renderer import render_prompt, render_prompt_as_messages
from ..output_writer import apply_structured_output
from registry.provider_execution_registry import default_llm_adapter_type, default_provider_slug
from runtime.domain import RuntimeBoundaryError
from adapters.task_profiles import resolve_profile
from runtime.native_authority import default_native_authority_refs
from ._capabilities import (
    WORKFLOW_CAPABILITIES as _caps,
    get_route_outcomes,
)
from ._workflow_policy import (
    apply_workflow_preflight,
    cache_workflow_result,
    run_workflow_with_retry,
)
from ._recording import (
    emit_workflow_finished,
    emit_workflow_started,
    record_workflow_result,
)
from ._result_shaping import (
    project_single_workflow_result,
    shape_pipeline_workflow_result,
)
from ._workflow_execution import (
    WorkflowExecutionContext,
    execute_workflow_request,
    plan_workflow_request,
)
from .runtime_setup import (
    _build_registry,
    _build_workflow_graph,
    build_workflow_runtime_setup,
)


logger = logging.getLogger(__name__)


def _default_workspace() -> str:
    return default_native_authority_refs()[0]


def _default_runtime_profile() -> str:
    return default_native_authority_refs()[1]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _unique_id() -> str:
    return uuid.uuid4().hex[:10]


# ---------------------------------------------------------------------------
# Workflow spec
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    """Minimal spec for a single workflow run. This is what the operator provides."""

    prompt: str
    provider_slug: str = field(default_factory=default_provider_slug)
    model_slug: str | None = None
    tier: str | None = None  # "frontier", "mid", "economy", "auto" — overrides provider_slug/model_slug
    adapter_type: str = field(default_factory=default_llm_adapter_type)
    timeout: int = 300
    workdir: str | None = None
    max_tokens: int = 4096
    temperature: float = 0.0
    label: str | None = None
    workspace_ref: str | None = None
    runtime_profile_ref: str | None = None
    system_prompt: str | None = None
    context_sections: list[dict[str, str]] | None = None
    max_retries: int = 0
    scope_read: list[str] | None = None
    scope_write: list[str] | None = None
    allowed_tools: list[str] | None = None
    verify_refs: list[str] | None = None
    definition_revision: str | None = None
    plan_revision: str | None = None
    packet_provenance: dict[str, Any] | None = None
    output_schema: dict | None = None
    authoring_contract: dict[str, Any] | None = None
    acceptance_contract: dict[str, Any] | None = None
    max_context_tokens: int | None = None
    persist: bool = True
    capabilities: list[str] | None = None  # capability-based routing (overrides tier)
    use_cache: bool = False  # Enable result caching (content-addressed)
    task_type: str | None = None  # task type for profile-based tool/tier routing
    prefer_cost: bool = False  # explicit routing hint to favor cheaper admitted models
    submission_required: bool | None = None  # explicit override for sealed submission enforcement
    skip_auto_review: bool = False  # If True, skip auto-review even if warranted (prevents review-of-review loops)
    reviews_workflow_id: str | None = None  # run_id of the workflow being reviewed
    review_target_modules: list[str] | None = None  # files/modules being reviewed
    parent_run_id: str | None = None  # direct lineage parent for triggered / child runs


@dataclass(frozen=True, slots=True)
class WorkflowResult:
    """What comes back from a workflow run."""

    run_id: str
    status: str  # "succeeded" or "failed"
    reason_code: str
    completion: str | None
    outputs: Mapping[str, Any]
    evidence_count: int
    started_at: datetime
    finished_at: datetime
    latency_ms: int
    provider_slug: str
    model_slug: str | None
    adapter_type: str
    failure_code: str | None = None
    attempts: int = 1
    label: str | None = None
    task_type: str | None = None
    capabilities: list[str] | None = None
    author_model: str | None = None  # "provider_slug/model_slug" of the executing model
    reviews_workflow_id: str | None = None  # run_id of the workflow being reviewed
    review_target_modules: list[str] | None = None  # files/modules being reviewed
    parent_run_id: str | None = None
    persisted: bool = False
    sync_status: str = "skipped"
    sync_cycle_id: str | None = None
    sync_error_count: int = 0

    def to_json(self) -> dict[str, Any]:
        result = {
            "kind": "workflow_result",
            "run_id": self.run_id,
            "status": self.status,
            "reason_code": self.reason_code,
            "completion": self.completion,
            "outputs": dict(self.outputs),
            "evidence_count": self.evidence_count,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "latency_ms": self.latency_ms,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "adapter_type": self.adapter_type,
            "failure_code": self.failure_code,
            "attempts": self.attempts,
            "author_model": self.author_model,
            "persisted": self.persisted,
            "sync_status": self.sync_status,
            "sync_cycle_id": self.sync_cycle_id,
            "sync_error_count": self.sync_error_count,
        }
        if self.label is not None:
            result["label"] = self.label
        if self.task_type is not None:
            result["task_type"] = self.task_type
        if self.capabilities is not None:
            result["capabilities"] = self.capabilities
        if self.reviews_workflow_id is not None:
            result["reviews_workflow_id"] = self.reviews_workflow_id
        if self.review_target_modules is not None:
            result["review_target_modules"] = self.review_target_modules
        if self.parent_run_id is not None:
            result["parent_run_id"] = self.parent_run_id
        # Add failure classification if this is a failed run
        if self.status == "failed" and _caps.failure_classifier:
            classification = _caps.failure_classifier(
                self.failure_code,
                outputs=dict(self.outputs),
            )
            result["failure_classification"] = classification.to_dict()
            if _caps.friction_ledger:
                ledger = _caps.friction_ledger()
                from runtime.friction_ledger import FrictionType
                friction_type = (
                    FrictionType.HARD_FAILURE
                    if not classification.is_retryable
                    else FrictionType.GUARDRAIL_BOUNCE
                )
                ledger.record(
                    friction_type=friction_type,
                    source=f"{self.provider_slug}/{self.model_slug or ''}",
                    job_label=self.label or "",
                    message=f"{classification.category.value}: {classification.recommended_action}",
                )
        return result


def _result_is_persisted(
    result: WorkflowResult,
    *,
    evidence_writer: Any | None = None,
) -> bool:
    if not result.run_id or result.run_id.startswith(("cached:", "error:")):
        return False
    if evidence_writer is None:
        return False
    from ..persistent_evidence import PostgresEvidenceWriter

    return isinstance(evidence_writer, PostgresEvidenceWriter)


def _finalize_workflow_result(
    result: WorkflowResult,
    *,
    evidence_writer: Any | None = None,
) -> WorkflowResult:
    persisted = _result_is_persisted(result, evidence_writer=evidence_writer)
    if not persisted:
        return replace(
            result,
            persisted=False,
            sync_status="skipped",
            sync_cycle_id=None,
            sync_error_count=0,
        )

    from ..post_workflow_sync import (
        record_workflow_run_sync_status,
        run_post_workflow_sync,
    )

    try:
        sync_result = run_post_workflow_sync(result.run_id)
    except Exception as exc:
        try:
            sync_result = record_workflow_run_sync_status(
                result.run_id,
                sync_status="degraded",
                sync_cycle_id=None,
                sync_error_count=1,
                total_findings=0,
                total_actions=0,
                last_error=str(exc),
            )
        except Exception as record_exc:
            raise RuntimeBoundaryError(
                f"degraded status could not be persisted for run {result.run_id}",
            ) from record_exc
    return replace(
        result,
        persisted=True,
        sync_status=sync_result.sync_status,
        sync_cycle_id=sync_result.sync_cycle_id,
        sync_error_count=sync_result.sync_error_count,
    )


def _attach_spec_metadata(result: WorkflowResult, spec: WorkflowSpec) -> WorkflowResult:
    """Carry spec lineage hints onto the terminal result."""
    updates: dict[str, object] = {}

    parent_run_id = spec.parent_run_id or spec.reviews_workflow_id
    if parent_run_id and result.parent_run_id is None:
        updates["parent_run_id"] = parent_run_id
    if spec.reviews_workflow_id and result.reviews_workflow_id is None:
        updates["reviews_workflow_id"] = spec.reviews_workflow_id
    if spec.review_target_modules and result.review_target_modules is None:
        updates["review_target_modules"] = list(spec.review_target_modules)
    if spec.task_type and result.task_type is None:
        updates["task_type"] = spec.task_type
    if spec.label and result.label is None:
        updates["label"] = spec.label

    if not updates:
        return result
    return replace(result, **updates)


def _resolve_execution_authority(spec: WorkflowSpec) -> WorkflowSpec:
    """Resolve native authority only at the execution boundary."""

    if spec.workspace_ref and spec.runtime_profile_ref:
        return spec

    workspace_ref, runtime_profile_ref = default_native_authority_refs()
    return replace(
        spec,
        workspace_ref=spec.workspace_ref or workspace_ref,
        runtime_profile_ref=spec.runtime_profile_ref or runtime_profile_ref,
    )


def _run_workflow_core(spec: WorkflowSpec) -> WorkflowResult:
    """Execute a single workflow through the graph.

    Each step is a node in the workflow graph. The orchestrator walks
    the graph, records evidence per node, and passes outputs between nodes.

    Graph: context_compiler → llm → output_parser [→ file_writer] [→ verifier] → terminal
    """

    start_ns = time.monotonic_ns()
    started_at = _utc_now()
    context = WorkflowExecutionContext(
        provider_slug=spec.provider_slug,
        model_slug=spec.model_slug,
        adapter_type=spec.adapter_type,
        started_at=started_at,
        start_ns=start_ns,
    )
    preflight_result = apply_workflow_preflight(spec, context=context, run_id_factory=_unique_id)
    if preflight_result is not None:
        return _finalize_workflow_result(_attach_spec_metadata(preflight_result, spec))

    resolved_spec = _resolve_execution_authority(spec)

    # 1. Build the workflow graph — each step is a node
    request = _build_workflow_graph(resolved_spec)

    # 2. Plan intake (validate + admit)
    runtime_setup = build_workflow_runtime_setup(resolved_spec)
    intake_outcome, failure = plan_workflow_request(
        request=request,
        registry=runtime_setup.registry,
        context=context,
    )
    if failure is not None:
        return _finalize_workflow_result(_attach_spec_metadata(failure, spec))

    # 3. Execute the graph — the orchestrator walks nodes and records evidence
    execution_result, failure = execute_workflow_request(
        intake_outcome=intake_outcome,
        adapter_registry=runtime_setup.adapter_registry,
        evidence_writer=runtime_setup.evidence_writer,
        context=context,
        timeout=spec.timeout or 300,
    )
    if failure is not None:
        return _finalize_workflow_result(
            _attach_spec_metadata(failure, spec),
            evidence_writer=runtime_setup.evidence_writer,
        )

    result = project_single_workflow_result(
        spec=resolved_spec,
        intake_outcome=intake_outcome,
        evidence_writer=runtime_setup.evidence_writer,
    )
    result = _attach_spec_metadata(result, resolved_spec)

    cache_workflow_result(resolved_spec, result)

    return _finalize_workflow_result(result, evidence_writer=runtime_setup.evidence_writer)


def run_workflow(spec: WorkflowSpec) -> WorkflowResult:
    """Execute a workflow with optional retry on transient failures.

    When spec.max_retries > 0, retryable failures trigger re-run with
    exponential backoff. If spec.tier is set, each retry re-resolves the route
    (which may pick a different healthy provider). If an explicit provider is
    set, the same provider is retried.

    Returns the first successful result, or the last failure after all
    attempts are exhausted.
    """
    return run_workflow_with_retry(
        spec,
        dispatch_once=_run_workflow_core,
        emit_started=emit_workflow_started,
        emit_finished=emit_workflow_finished,
        record_result=record_workflow_result,
        sleep_fn=time.sleep,
        stderr=sys.stderr,
    )


def run_single_workflow(spec: WorkflowSpec) -> WorkflowResult:
    """Single-spec workflow entrypoint."""
    return run_workflow(spec)


def run_workflow_parallel(
    specs: list[WorkflowSpec],
    *,
    max_workers: int | None = None,
) -> list[WorkflowResult]:
    """Run multiple specs in parallel and return all results."""

    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not specs:
        return []
    if len(specs) == 1:
        return [run_workflow(specs[0])]

    workers = max_workers or min(len(specs), 8)
    results: dict[int, WorkflowResult] = {}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(run_workflow, spec): index
            for index, spec in enumerate(specs)
        }
        for future in as_completed(futures):
            index = futures[future]
            result = future.result()
            results[index] = result

    return [results[i] for i in range(len(specs))]


def run_workflow_pipeline(
    steps: list,
    *,
    timeout: int = 600,
    workspace_ref: str | None = None,
    runtime_profile_ref: str | None = None,
    max_context_tokens: int | None = None,
    parent_run_id: str | None = None,
) -> WorkflowResult:
    """Execute a multi-step pipeline end-to-end.

    Builds a variable-length WorkflowRequest from the steps, runs it
    through intake -> execute -> evidence, and returns a WorkflowResult
    with outputs from the **last** node in the pipeline.

    Parameters
    ----------
    steps:
        List of ``WorkflowStep`` instances describing the pipeline.
    timeout:
        Overall execution timeout in seconds.
    workspace_ref:
        Workspace authority reference.
    runtime_profile_ref:
        Runtime profile authority reference.
    """

    from ..workflow_builder import build_workflow_request

    start_ns = time.monotonic_ns()
    started_at = _utc_now()

    # 1. Build the multi-node workflow request
    request = build_workflow_request(
        steps,
        workspace_ref=workspace_ref or _default_workspace(),
        runtime_profile_ref=runtime_profile_ref or _default_runtime_profile(),
    )

    # Determine provider/model from the first step for result metadata
    first_step = steps[0]
    provider_slug = first_step.provider_slug or "anthropic"
    model_slug = first_step.model_slug
    adapter_type = first_step.adapter_type
    context = WorkflowExecutionContext(
        provider_slug=provider_slug,
        model_slug=model_slug,
        adapter_type=adapter_type,
        started_at=started_at,
        start_ns=start_ns,
    )

    # 2. Plan intake
    pipeline_spec = WorkflowSpec(
        prompt="pipeline",
        provider_slug=provider_slug,
        model_slug=model_slug,
        adapter_type=adapter_type,
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        parent_run_id=parent_run_id,
    )
    runtime_setup = build_workflow_runtime_setup(pipeline_spec)
    intake_outcome, failure = plan_workflow_request(
        request=request,
        registry=runtime_setup.registry,
        context=context,
    )
    if failure is not None:
        return failure

    # 3. Execute the workflow
    execution_result, failure = execute_workflow_request(
        intake_outcome=intake_outcome,
        adapter_registry=runtime_setup.adapter_registry,
        evidence_writer=runtime_setup.evidence_writer,
        context=context,
        timeout=timeout,
        max_context_tokens=max_context_tokens,
        count_evidence_on_failure=True,
    )
    if failure is not None:
        return failure

    result = shape_pipeline_workflow_result(
        steps=steps,
        intake_outcome=intake_outcome,
        execution_result=execution_result,
        evidence_writer=runtime_setup.evidence_writer,
        context=context,
    )
    record_workflow_result(result)
    return result


def run_workflow_from_spec_file(
    path: str,
    *,
    variables: dict[str, Any] | None = None,
) -> WorkflowResult:
    """Load a single spec from a JSON file and run it.

    If *variables* is provided, ``{{key}}`` placeholders in the spec's
    string values are interpolated before execution.
    """

    from ..workflow_spec import load_workflow_spec

    spec = load_workflow_spec(path, variables=variables)
    return run_workflow(spec)


def run_workflow_batch_from_file(
    path: str,
    *,
    variables: dict[str, Any] | None = None,
) -> list[WorkflowResult]:
    """Load a batch spec from a JSON file and run all jobs in parallel.

    If *variables* is provided, ``{{key}}`` placeholders in the spec's
    string values are interpolated before execution.
    """

    from ..workflow_spec import load_workflow_batch

    specs, max_parallel = load_workflow_batch(path, variables=variables)
    return run_workflow_parallel(specs, max_workers=max_parallel)
