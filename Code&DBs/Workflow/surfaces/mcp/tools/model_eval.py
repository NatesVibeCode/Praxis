"""Tool: praxis_model_eval."""

from __future__ import annotations

import time
from typing import Any
import uuid

from runtime.operation_catalog_gateway import CallerContext, execute_operation_from_env, spawn_threaded

from ..subsystems import workflow_database_env


_ACTION_OPERATION = {
    "plan": "model_eval_plan",
    "run": "model_eval_run_matrix",
    "inspect": "model_eval_inspect",
    "compare": "model_eval_compare",
    "promote": "model_eval_promote_proposal",
    "export": "model_eval_export",
    "benchmark_ingest": "model_eval_benchmark_ingest",
}


def _kickoff_model_eval_run(payload: dict[str, Any]) -> dict[str, Any]:
    run_payload = dict(payload)
    kickoff_id, lab_run_id = _ensure_lab_run_id(run_payload)
    kickoff_started_at = time.time()
    correlation_id = str(uuid.uuid4())
    caller_context = CallerContext(
        cause_receipt_id=None,
        correlation_id=correlation_id,
        transport_kind="workflow",
    )

    def _run_in_background() -> None:
        execute_operation_from_env(
            env=workflow_database_env(),
            operation_name="model_eval_run_matrix",
            payload=run_payload,
            caller_context=caller_context,
        )

    spawn_threaded(
        _run_in_background,
        name=f"model_eval_kickoff:{kickoff_id}",
    )
    return {
        "ok": True,
        "operation": "model_eval_run_matrix",
        "kickoff": True,
        "kickoff_id": kickoff_id,
        "lab_run_id": lab_run_id,
        "kickoff_started_at": kickoff_started_at,
        "correlation_id": correlation_id,
        "status": "started",
        "inspect_input": {"action": "inspect", "lab_run_id": lab_run_id},
        "compare_input": {"action": "compare", "lab_run_id": lab_run_id},
        "message": (
            "Model Eval matrix is running in the background. Inspect the trace "
            f"with correlation_id={correlation_id!r} or inspect lab_run_id={lab_run_id!r}."
        ),
    }


def _ensure_lab_run_id(payload: dict[str, Any]) -> tuple[str, str]:
    kickoff_id = f"model_eval_kickoff_{uuid.uuid4().hex[:12]}"
    lab_run_id = str(payload.get("run_label") or "").strip()
    if not lab_run_id:
        lab_run_id = kickoff_id.replace("model_eval_kickoff_", "model-eval-")
        payload["run_label"] = lab_run_id
    return kickoff_id, lab_run_id


def _run_model_eval_inline(payload: dict[str, Any]) -> dict[str, Any]:
    run_payload = dict(payload)
    _kickoff_id, lab_run_id = _ensure_lab_run_id(run_payload)
    correlation_id = str(uuid.uuid4())
    caller_context = CallerContext(
        cause_receipt_id=None,
        correlation_id=correlation_id,
        transport_kind="workflow",
    )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="model_eval_run_matrix",
        payload=run_payload,
        caller_context=caller_context,
    )
    if isinstance(result, dict):
        result.setdefault("lab_run_id", lab_run_id)
        result.setdefault("correlation_id", correlation_id)
        result.setdefault("inspect_input", {"action": "inspect", "lab_run_id": lab_run_id})
        result.setdefault("compare_input", {"action": "compare", "lab_run_id": lab_run_id})
        result.setdefault("execution_mode", "inline")
    return result


def tool_praxis_model_eval(params: dict, _progress_emitter=None) -> dict:
    """Operate the Model Eval Authority through one action selector."""
    payload: dict[str, Any] = {key: value for key, value in dict(params or {}).items() if value is not None}
    action = str(payload.pop("action", "plan")).strip().lower()
    operation_name = _ACTION_OPERATION.get(action)
    if operation_name is None:
        return {
            "ok": False,
            "error_code": "model_eval.unknown_action",
            "error": f"Unknown model eval action {action!r}",
            "allowed_actions": sorted(_ACTION_OPERATION),
        }
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message=f"Model Eval {action}")
    if action == "run":
        wait_for_completion = bool(payload.pop("wait_for_completion", False))
        if bool(payload.get("dry_run")) or wait_for_completion:
            result = _run_model_eval_inline(payload)
        else:
            result = _kickoff_model_eval_run(payload)
        if _progress_emitter:
            message = "Model Eval run completed" if result.get("execution_mode") == "inline" else "Model Eval run kicked off"
            _progress_emitter.emit(progress=1, total=1, message=message)
        return result
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name=operation_name,
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Model Eval {action} {status}")
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_model_eval": (
        tool_praxis_model_eval,
        {
            "kind": "write",
            "operation_names": [
                "model_eval_plan",
                "model_eval_run_matrix",
                "model_eval_inspect",
                "model_eval_compare",
                "model_eval_promote_proposal",
                "model_eval_export",
                "model_eval_benchmark_ingest",
            ],
            "description": (
                "Plan, run, inspect, compare, promote, export, or ingest "
                "public benchmark priors for model/job/prompt evaluation "
                "matrices. Imports canonical Workflow specs as fixed fixtures "
                "and varies model/provider/prompt/effort/tool/swarm "
                "configuration under strict privacy gates."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["plan", "run", "inspect", "compare", "promote", "export", "benchmark_ingest"],
                        "default": "plan",
                    },
                    "suite_slugs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Built-in suites: docs, pptx, csv, tools, swarm.",
                    },
                    "workflow_spec_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Canonical Workflow spec paths to import as fixed fixtures.",
                    },
                    "model_configs": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Optional provider/model/request configs.",
                    },
                    "prompt_variants": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Optional prompt variants.",
                    },
                    "budget_cap_usd": {"type": "number"},
                    "max_runs": {"type": "integer"},
                    "max_workflow_jobs": {"type": "integer"},
                    "timeout_seconds": {"type": "integer"},
                    "trials_per_case": {
                        "type": "integer",
                        "description": "Repeat count per case/model/prompt cell. Use >=3 before promotion decisions.",
                    },
                    "run_mode": {
                        "type": "string",
                        "enum": [
                            "structured_output",
                            "tool_choice_static",
                            "tool_execution_loop",
                            "workflow_import",
                            "swarm",
                        ],
                        "description": "Optional run-mode filter/override for future suite slices.",
                    },
                    "dry_run": {"type": "boolean"},
                    "wait_for_completion": {
                        "type": "boolean",
                        "description": (
                            "Run synchronously through the workflow lane. Use only for tiny capped live tests; "
                            "dry_run=true always runs inline so CLI calls produce inspectable summaries."
                        ),
                    },
                    "run_label": {"type": "string"},
                    "lab_run_id": {"type": "string"},
                    "include_results": {"type": "boolean"},
                    "export_format": {"type": "string", "enum": ["json", "markdown"]},
                    "task_type": {"type": "string"},
                    "winner_config_id": {"type": "string"},
                    "benchmark_slug": {"type": "string"},
                    "source_url": {"type": "string"},
                    "version": {"type": "string"},
                    "rows": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Benchmark prior rows: provider_slug, model_slug, metric_slug, score, rank, task_family.",
                    },
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "model-eval",
                "when_to_use": (
                    "Use for consistent model selection: same Workflow spec, "
                    "same fixtures, same verifier, varied model/prompt/provider "
                    "configuration."
                ),
                "when_not_to_use": (
                    "Do not use as a production route mutation surface. The "
                    "promote action emits a proposal only."
                ),
                "risks": {
                    "default": "write",
                    "actions": {
                        "plan": "read",
                        "inspect": "read",
                        "compare": "read",
                        "export": "read",
                        "benchmark_ingest": "write",
                        "run": "write",
                        "promote": "write",
                    },
                },
                "examples": [
                    {
                        "title": "Preview docs/csv matrix",
                        "input": {"action": "plan", "suite_slugs": ["docs", "csv"]},
                    },
                    {
                        "title": "Dry-run a model eval matrix",
                        "input": {
                            "action": "run",
                            "suite_slugs": ["docs"],
                            "dry_run": True,
                            "max_runs": 4,
                        },
                    },
                ],
            },
            "type_contract": {
                "model_eval": {
                    "consumes": [
                        "workflow_spec.path",
                        "model_eval.config_matrix",
                        "privacy_policy.no_data_share",
                    ],
                    "produces": [
                        "model_eval.plan",
                        "model_eval.run_summary",
                        "model_eval.scorecard",
                        "model_eval.promotion_proposal",
                        "authority_operation_receipt",
                    ],
                }
            },
        },
    ),
}
