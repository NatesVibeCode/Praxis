"""CQRS command operations for Model Eval Authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from runtime.model_eval.runner import (
    persist_model_eval_case_run,
    run_model_eval_case,
    run_model_eval_matrix,
)
from runtime.operations.queries.model_eval import _best_rows, _load_summary


class ModelEvalRunMatrixCommand(BaseModel):
    suite_slugs: list[str] = Field(default_factory=list)
    workflow_spec_paths: list[str] = Field(default_factory=list)
    model_configs: list[dict[str, Any]] = Field(default_factory=list)
    prompt_variants: list[dict[str, Any]] = Field(default_factory=list)
    budget_cap_usd: float = 5.0
    max_runs: int = 30
    max_workflow_jobs: int = 20
    timeout_seconds: int = 90
    dry_run: bool = False
    run_label: str | None = None
    trials_per_case: int = 1
    run_mode: str | None = None

    @field_validator("suite_slugs", "workflow_spec_paths", mode="before")
    @classmethod
    def _normalize_text_list(cls, value: object) -> list[str]:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        raise ValueError("expected a list of strings")

    @field_validator("budget_cap_usd", mode="before")
    @classmethod
    def _normalize_budget(cls, value: object) -> float:
        if value in (None, ""):
            return 5.0
        if isinstance(value, bool):
            raise ValueError("budget_cap_usd must be numeric")
        return max(0.0, min(float(value), 100.0))

    @field_validator("max_runs", "max_workflow_jobs", "timeout_seconds", "trials_per_case", mode="before")
    @classmethod
    def _normalize_int(cls, value: object) -> int:
        if value in (None, ""):
            return 30
        if isinstance(value, bool):
            raise ValueError("integer fields must be integers")
        return max(1, min(int(value), 500))

    @field_validator("run_label", mode="before")
    @classmethod
    def _normalize_run_label(cls, value: object) -> str | None:
        if value in (None, ""):
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("run_label must be a non-empty string")
        cleaned = value.strip()
        allowed = [char if char.isalnum() or char in "-_." else "-" for char in cleaned]
        return "".join(allowed)[:80]

    @field_validator("run_mode", mode="before")
    @classmethod
    def _normalize_run_mode(cls, value: object) -> str | None:
        if value in (None, ""):
            return None
        normalized = str(value).strip()
        allowed = {
            "structured_output",
            "tool_choice_static",
            "tool_execution_loop",
            "workflow_import",
            "swarm",
        }
        if normalized not in allowed:
            raise ValueError("run_mode is not admitted")
        return normalized


class ModelEvalRunCaseCommand(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    task: dict[str, Any]
    model_config_payload: dict[str, Any] = Field(alias="model_config")
    prompt_variant: dict[str, Any]
    output_root: str
    timeout_seconds: int = 90
    dry_run: bool = False
    trial_number: int = 1
    matrix_receipt_id: str | None = None

    @field_validator("timeout_seconds", "trial_number", mode="before")
    @classmethod
    def _normalize_int(cls, value: object) -> int:
        if value in (None, ""):
            return 1
        if isinstance(value, bool):
            raise ValueError("integer fields must be integers")
        return max(1, min(int(value), 500))

    @field_validator("output_root", "matrix_receipt_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value in (None, ""):
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("text fields must be non-empty strings")
        return value.strip()


class ModelEvalPromoteProposalCommand(BaseModel):
    lab_run_id: str
    task_type: str | None = None
    winner_config_id: str | None = None

    @field_validator("lab_run_id", mode="before")
    @classmethod
    def _normalize_run_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("lab_run_id is required")
        return value.strip()

    @field_validator("task_type", "winner_config_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value in (None, ""):
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional text fields must be non-empty strings")
        return value.strip()


def handle_model_eval_run_matrix(command: ModelEvalRunMatrixCommand, _subsystems: Any) -> dict[str, Any]:
    result = run_model_eval_matrix(
        suite_slugs=command.suite_slugs,
        workflow_spec_paths=command.workflow_spec_paths,
        model_configs=command.model_configs or None,
        prompt_variants=command.prompt_variants or None,
        budget_cap_usd=command.budget_cap_usd,
        max_runs=command.max_runs,
        max_workflow_jobs=command.max_workflow_jobs,
        timeout_seconds=command.timeout_seconds,
        dry_run=command.dry_run,
        run_label=command.run_label,
        trials_per_case=command.trials_per_case,
        run_mode=command.run_mode,
        subsystems=_subsystems,
    )
    result["event_payload"] = {
        "lab_run_id": result.get("lab_run_id"),
        "artifact_root": result.get("artifact_root"),
        "dry_run": result.get("dry_run"),
        "total_cost_usd": result.get("total_cost_usd"),
        "executed_count": result.get("executed_count"),
        "passed_count": result.get("passed_count"),
        "failed_count": result.get("failed_count"),
        "stopped_reason": result.get("stopped_reason"),
    }
    return result


def handle_model_eval_run_case(command: ModelEvalRunCaseCommand, _subsystems: Any) -> dict[str, Any]:
    result = run_model_eval_case(
        task=command.task,
        model_config=command.model_config_payload,
        prompt_variant=command.prompt_variant,
        output_root=command.output_root,
        timeout_seconds=command.timeout_seconds,
        dry_run=command.dry_run,
        trial_number=command.trial_number,
    )
    if command.matrix_receipt_id:
        persist_model_eval_case_run(
            _subsystems,
            matrix_receipt_id=command.matrix_receipt_id,
            result=result,
            task=command.task,
            model_config=command.model_config_payload,
            prompt_variant=command.prompt_variant,
        )
    result["event_payload"] = {
        "case_run_id": result.get("case_run_id"),
        "task_id": result.get("task_id"),
        "suite_slug": result.get("suite_slug"),
        "family": result.get("task_family"),
        "config_id": result.get("config_id"),
        "model_slug": result.get("model_slug"),
        "status": result.get("status"),
        "score": result.get("score"),
        "cost_usd": result.get("cost"),
        "latency_ms": result.get("latency_ms"),
    }
    return result


def handle_model_eval_promote_proposal(
    command: ModelEvalPromoteProposalCommand,
    _subsystems: Any,
) -> dict[str, Any]:
    summary = _load_summary(command.lab_run_id)
    if not summary.get("ok") and summary.get("error_code"):
        return summary
    winners = _best_rows([dict(item) for item in summary.get("results") or [] if isinstance(item, dict)])
    selected = None
    for row in winners:
        if command.winner_config_id and row.get("config_id") != command.winner_config_id:
            continue
        selected = row
        break
    if selected is None:
        return {
            "ok": False,
            "error_code": "model_eval.no_winner",
            "error": "No matching winner found in eval summary.",
            "lab_run_id": command.lab_run_id,
        }
    proposal = {
        "proposal_type": "task_type_routing_candidate",
        "lab_run_id": command.lab_run_id,
        "task_type": command.task_type or selected.get("task_family") or selected.get("suite_slug"),
        "candidate": {
            "config_id": selected.get("config_id"),
            "model_slug": selected.get("model_slug"),
            "provider_order": selected.get("provider_order"),
            "prompt_variant_id": selected.get("prompt_variant_id"),
            "score": selected.get("score"),
            "cost": selected.get("cost"),
            "served_provider": selected.get("served_provider"),
            "served_model": selected.get("served_model"),
        },
        "promotion_gate": (
            "Proposal only. Production task_type_routing/request knobs require "
            "explicit operator review and a separate routing mutation."
        ),
    }
    return {
        "ok": True,
        "operation": "model_eval_promote_proposal",
        "proposal": proposal,
        "event_payload": proposal,
    }


__all__ = [
    "ModelEvalPromoteProposalCommand",
    "ModelEvalRunCaseCommand",
    "ModelEvalRunMatrixCommand",
    "handle_model_eval_promote_proposal",
    "handle_model_eval_run_case",
    "handle_model_eval_run_matrix",
]
