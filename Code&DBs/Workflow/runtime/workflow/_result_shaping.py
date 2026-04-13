"""Workflow result shaping helpers."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from ..workflow_projection import project_workflow_result

if TYPE_CHECKING:
    from receipts.evidence import AppendOnlyWorkflowEvidenceWriter
    from ._workflow_execution import WorkflowExecutionContext
    from .orchestrator import WorkflowResult, WorkflowSpec


def project_single_workflow_result(
    *,
    spec: WorkflowSpec,
    intake_outcome,
    evidence_writer: AppendOnlyWorkflowEvidenceWriter,
) -> WorkflowResult:
    """Project a single workflow result from the evidence timeline."""

    from .orchestrator import WorkflowResult

    timeline = evidence_writer.evidence_timeline(intake_outcome.run_id)
    projected = project_workflow_result(
        run_id=intake_outcome.run_id,
        timeline=timeline,
        spec_provider_slug=spec.provider_slug,
        spec_model_slug=spec.model_slug,
        spec_adapter_type=spec.adapter_type,
        spec_label=spec.label,
        spec_capabilities=spec.capabilities,
        spec_reviews_dispatch_id=spec.reviews_workflow_id,
        spec_review_target_modules=spec.review_target_modules,
    )
    return WorkflowResult(**projected)


def shape_pipeline_workflow_result(
    *,
    steps: list,
    intake_outcome,
    execution_result,
    evidence_writer: AppendOnlyWorkflowEvidenceWriter,
    context: WorkflowExecutionContext,
) -> WorkflowResult:
    """Shape a pipeline execution result from the last node and optional fan-out."""

    from .orchestrator import WorkflowResult

    finished_at = _utc_now()
    latency_ms = (time.monotonic_ns() - context.start_ns) // 1_000_000
    evidence_count = len(evidence_writer.evidence_timeline(intake_outcome.run_id))

    last_node_id = f"node_{len(steps) - 1}"
    last_node = next(
        (node_record for node_record in execution_result.node_results if node_record.node_id == last_node_id),
        None,
    )
    if last_node is None:
        return context.failure_result(
            run_id=intake_outcome.run_id,
            reason_code="dispatch.node_not_found",
            failure_code="dispatch.node_not_found",
            evidence_count=evidence_count,
        )

    outputs = dict(last_node.outputs)
    completion = outputs.get("completion")
    completion, outputs = _apply_fan_out_if_enabled(steps[-1], completion=completion, outputs=outputs)

    return WorkflowResult(
        run_id=intake_outcome.run_id,
        status=last_node.status,
        reason_code=execution_result.terminal_reason_code,
        completion=completion,
        outputs=outputs,
        evidence_count=evidence_count,
        started_at=context.started_at,
        finished_at=finished_at,
        latency_ms=latency_ms,
        provider_slug=context.provider_slug,
        model_slug=context.model_slug,
        adapter_type=context.adapter_type,
        failure_code=last_node.failure_code,
    )


def _apply_fan_out_if_enabled(last_step, *, completion: str | None, outputs: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    if not getattr(last_step, "fan_out", False) or not completion:
        return completion, outputs

    from ..fan_out import aggregate_fan_out_results, fan_out_from_completion

    fan_prompt = getattr(last_step, "fan_out_prompt", None) or last_step.prompt
    fan_tier = last_step.tier or "mid"
    fan_max = getattr(last_step, "fan_out_max_parallel", 4)

    fan_results = fan_out_from_completion(
        completion,
        prompt_template=fan_prompt,
        tier=fan_tier,
        max_parallel=fan_max,
    )
    fan_summary = aggregate_fan_out_results(fan_results)
    outputs["fan_out"] = fan_summary
    outputs["fan_out_source_completion"] = completion
    return json.dumps(fan_summary["completions"]), outputs


def _utc_now():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)
