"""Shared workflow intake and execution helpers for workflow runs."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from receipts.evidence import AppendOnlyWorkflowEvidenceWriter

from ..domain import RunState
from ..execution import RuntimeOrchestrator, RunExecutionResult
from ..intake import WorkflowIntakePlanner
from . import _capabilities as _workflow_caps

if TYPE_CHECKING:
    from adapters import AdapterRegistry
    from contracts.domain import WorkflowRequest
    from registry.domain import RegistryResolver


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WorkflowExecutionContext:
    provider_slug: str
    model_slug: str | None
    adapter_type: str
    started_at: datetime
    start_ns: int

    def latency_ms(self) -> int:
        return (time.monotonic_ns() - self.start_ns) // 1_000_000

    def failure_result(
        self,
        *,
        run_id: str,
        reason_code: str,
        failure_code: str | None,
        outputs: dict[str, Any] | None = None,
        evidence_count: int = 0,
    ):
        from .orchestrator import WorkflowResult

        return WorkflowResult(
            run_id=run_id,
            status="failed",
            reason_code=reason_code,
            completion=None,
            outputs=outputs or {},
            evidence_count=evidence_count,
            started_at=self.started_at,
            finished_at=_utc_now(),
            latency_ms=self.latency_ms(),
            provider_slug=self.provider_slug,
            model_slug=self.model_slug,
            adapter_type=self.adapter_type,
            failure_code=failure_code,
        )


def plan_workflow_request(
    *,
    request: WorkflowRequest,
    registry: RegistryResolver,
    context: WorkflowExecutionContext,
):
    """Plan a workflow request and return either intake or a failure result."""

    planner = WorkflowIntakePlanner(registry=registry)
    intake_outcome = planner.plan(request=request)
    if intake_outcome.admission_state == RunState.CLAIM_REJECTED:
        return None, context.failure_result(
            run_id=intake_outcome.run_id,
            reason_code=intake_outcome.admission_decision.reason_code,
            failure_code="intake.rejected",
        )
    return intake_outcome, None


def execute_workflow_request(
    *,
    intake_outcome,
    adapter_registry: AdapterRegistry,
    evidence_writer: AppendOnlyWorkflowEvidenceWriter,
    context: WorkflowExecutionContext,
    timeout: int,
    max_context_tokens: int | None = None,
    count_evidence_on_failure: bool = False,
):
    """Execute an admitted workflow and return either execution or failure."""

    orchestrator = RuntimeOrchestrator(
        adapter_registry=adapter_registry,
        evidence_reader=evidence_writer,
    )
    exec_kwargs: dict[str, Any] = {
        "intake_outcome": intake_outcome,
        "evidence_writer": evidence_writer,
    }
    if max_context_tokens is not None:
        exec_kwargs["max_context_tokens"] = max_context_tokens

    try:
        if _workflow_caps.LOAD_BALANCER is not None:
            with _workflow_caps.LOAD_BALANCER.slot(context.provider_slug) as acquired:
                if not acquired:
                    return None, context.failure_result(
                        run_id=intake_outcome.run_id,
                        reason_code="route.unhealthy",
                        failure_code="route.unhealthy",
                        outputs={"error": f"Provider at capacity: {context.provider_slug}"},
                    )
                from .execution_backends import provider_slot_bypass

                with provider_slot_bypass():
                    execution_result = _run_workflow(orchestrator, timeout=timeout, exec_kwargs=exec_kwargs)
        else:
            execution_result = _run_workflow(orchestrator, timeout=timeout, exec_kwargs=exec_kwargs)
    except FuturesTimeoutError:
        evidence_count = 0
        if count_evidence_on_failure:
            evidence_count = len(evidence_writer.evidence_timeline(intake_outcome.run_id))
        return None, context.failure_result(
            run_id=intake_outcome.run_id,
            reason_code="workflow.execution_timeout",
            failure_code="workflow.timeout",
            evidence_count=evidence_count,
        )
    except Exception as exc:
        evidence_count = 0
        if count_evidence_on_failure:
            evidence_count = len(evidence_writer.evidence_timeline(intake_outcome.run_id))
        return None, context.failure_result(
            run_id=intake_outcome.run_id,
            reason_code="workflow.execution_crash",
            failure_code="workflow.crash",
            outputs={"error": str(exc)},
            evidence_count=evidence_count,
        )

    return execution_result, None


def execute_admitted_workflow_request(
    *,
    intake_outcome,
    adapter_registry: AdapterRegistry,
    evidence_writer: AppendOnlyWorkflowEvidenceWriter,
    context: WorkflowExecutionContext,
    timeout: int,
    max_context_tokens: int | None = None,
):
    """Execute a workflow whose submission and admission proofs already exist."""

    orchestrator = RuntimeOrchestrator(
        adapter_registry=adapter_registry,
        evidence_reader=evidence_writer,
    )
    exec_kwargs: dict[str, Any] = {
        "intake_outcome": intake_outcome,
        "evidence_writer": evidence_writer,
    }
    if max_context_tokens is not None:
        exec_kwargs["max_context_tokens"] = max_context_tokens

    try:
        if _workflow_caps.LOAD_BALANCER is not None:
            with _workflow_caps.LOAD_BALANCER.slot(context.provider_slug) as acquired:
                if not acquired:
                    return None, context.failure_result(
                        run_id=intake_outcome.run_id,
                        reason_code="route.unhealthy",
                        failure_code="route.unhealthy",
                        outputs={"error": f"Provider at capacity: {context.provider_slug}"},
                    )
                from .execution_backends import provider_slot_bypass

                with provider_slot_bypass():
                    execution_result = _run_admitted_workflow(
                        orchestrator,
                        timeout=timeout,
                        exec_kwargs=exec_kwargs,
                    )
        else:
            execution_result = _run_admitted_workflow(
                orchestrator,
                timeout=timeout,
                exec_kwargs=exec_kwargs,
            )
    except FuturesTimeoutError:
        return None, context.failure_result(
            run_id=intake_outcome.run_id,
            reason_code="workflow.execution_timeout",
            failure_code="workflow.timeout",
        )
    except Exception as exc:
        return None, context.failure_result(
            run_id=intake_outcome.run_id,
            reason_code="workflow.execution_crash",
            failure_code="workflow.crash",
            outputs={"error": str(exc)},
        )

    return execution_result, None


def _run_workflow(
    orchestrator: RuntimeOrchestrator,
    *,
    timeout: int,
    exec_kwargs: dict[str, Any],
) -> RunExecutionResult:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(orchestrator.execute_deterministic_path, **exec_kwargs)
        return future.result(timeout=timeout)


def _run_admitted_workflow(
    orchestrator: RuntimeOrchestrator,
    *,
    timeout: int,
    exec_kwargs: dict[str, Any],
) -> RunExecutionResult:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(orchestrator.execute_admitted_deterministic_path, **exec_kwargs)
        return future.result(timeout=timeout)
