"""CQRS query operations for Model Eval Authority."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.model_eval.catalog import build_suite_plan


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".git").exists():
            return parent
    return Path.cwd()


def _summary_path(lab_run_id: str) -> Path:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in lab_run_id)[:120]
    return _repo_root() / "scratch" / "model-eval" / cleaned / "_summary.json"


def _load_summary(lab_run_id: str) -> dict[str, Any]:
    path = _summary_path(lab_run_id)
    if not path.is_file():
        return {
            "ok": False,
            "error_code": "model_eval.summary_not_found",
            "error": f"Model Eval summary not found for {lab_run_id!r}",
            "summary_path": str(path),
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "error_code": "model_eval.summary_unreadable",
            "error": str(exc),
            "summary_path": str(path),
        }
    return payload if isinstance(payload, dict) else {"ok": False, "error": "summary is not an object"}


def _best_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in results:
        key = str(row.get("task_id") or row.get("task_family") or "unknown")
        grouped.setdefault(key, []).append(row)
    winners: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        ranked = sorted(
            rows,
            key=lambda item: (
                not bool(item.get("ok")),
                -float(item.get("score") or 0.0),
                float(item.get("cost") or 0.0),
                float(item.get("duration_s") or 999999.0),
            ),
        )
        if ranked:
            best = dict(ranked[0])
            best["group_key"] = key
            winners.append(best)
    return winners


def _load_scorecards_for_lab_run(subsystems: Any, lab_run_id: str) -> list[dict[str, Any]]:
    get_pg_conn = getattr(subsystems, "get_pg_conn", None)
    if not callable(get_pg_conn):
        return []
    try:
        conn = get_pg_conn()
        rows = conn.fetch(
            """
            WITH matrix_receipts AS (
                SELECT receipt_id
                  FROM authority_operation_receipts
                 WHERE operation_name = 'model_eval_run_matrix'
                   AND result_payload->>'lab_run_id' = $1
            )
            SELECT
                sc.scorecard_id::text,
                sc.matrix_receipt_id::text,
                sc.config_id,
                sc.model_slug,
                MAX(cr.model_config_json->>'agent') AS pinned_agent_slug,
                MAX(COALESCE(
                    cr.model_config_json->>'model_eval_candidate_ref',
                    cr.model_config_json->>'candidate_ref',
                    cr.model_config_json->>'config_id'
                )) AS model_eval_candidate_ref,
                sc.family,
                sc.trials,
                sc.pass_count,
                sc.pass_at_1,
                sc.mean_score,
                sc.score_variance,
                sc.mean_cost_usd,
                sc.mean_latency_ms,
                sc.failure_counts_json,
                sc.created_at
              FROM model_eval_scorecards sc
              LEFT JOIN model_eval_case_runs cr
                ON cr.matrix_receipt_id = sc.matrix_receipt_id
               AND cr.config_id = sc.config_id
               AND cr.family = sc.family
             WHERE sc.matrix_receipt_id IN (SELECT receipt_id FROM matrix_receipts)
             GROUP BY
                sc.scorecard_id,
                sc.matrix_receipt_id,
                sc.config_id,
                sc.model_slug,
                sc.family,
                sc.trials,
                sc.pass_count,
                sc.pass_at_1,
                sc.mean_score,
                sc.score_variance,
                sc.mean_cost_usd,
                sc.mean_latency_ms,
                sc.failure_counts_json,
                sc.created_at
             ORDER BY sc.family, sc.pass_at_1 DESC, sc.mean_score DESC, sc.mean_cost_usd ASC NULLS LAST
            """,
            lab_run_id,
        )
    except Exception:
        return []
    return [dict(row) for row in rows or []]


def _load_case_runs_for_lab_run(subsystems: Any, lab_run_id: str) -> list[dict[str, Any]]:
    get_pg_conn = getattr(subsystems, "get_pg_conn", None)
    if not callable(get_pg_conn):
        return []
    try:
        conn = get_pg_conn()
        rows = conn.fetch(
            """
            WITH matrix_receipts AS (
                SELECT receipt_id
                  FROM authority_operation_receipts
                 WHERE operation_name = 'model_eval_run_matrix'
                   AND result_payload->>'lab_run_id' = $1
            )
            SELECT
                case_run_id::text,
                matrix_receipt_id::text,
                child_receipt_id::text,
                task_id,
                suite_slug,
                family,
                model_config_json,
                model_config_json->>'agent' AS pinned_agent_slug,
                COALESCE(
                    model_config_json->>'model_eval_candidate_ref',
                    model_config_json->>'candidate_ref',
                    model_config_json->>'config_id'
                ) AS model_eval_candidate_ref,
                prompt_variant_json,
                provider_requested,
                provider_served,
                model_served,
                status,
                score,
                cost_usd,
                latency_ms,
                catalog_version_hash,
                trial_number,
                created_at
              FROM model_eval_case_runs
             WHERE matrix_receipt_id IN (SELECT receipt_id FROM matrix_receipts)
             ORDER BY created_at ASC, task_id ASC
            """,
            lab_run_id,
        )
    except Exception:
        return []
    return [dict(row) for row in rows or []]


class ModelEvalPlanQuery(BaseModel):
    suite_slugs: list[str] = Field(default_factory=list)
    workflow_spec_paths: list[str] = Field(default_factory=list)
    model_configs: list[dict[str, Any]] = Field(default_factory=list)
    prompt_variants: list[dict[str, Any]] = Field(default_factory=list)
    max_workflow_jobs: int = 20
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

    @field_validator("max_workflow_jobs", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 20
        if isinstance(value, bool):
            raise ValueError("max_workflow_jobs must be an integer")
        return max(1, min(int(value), 200))

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


class ModelEvalInspectQuery(BaseModel):
    lab_run_id: str
    include_results: bool = True

    @field_validator("lab_run_id", mode="before")
    @classmethod
    def _normalize_run_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("lab_run_id is required")
        return value.strip()


class ModelEvalCompareQuery(ModelEvalInspectQuery):
    pass


class ModelEvalExportQuery(ModelEvalInspectQuery):
    export_format: str = "json"

    @field_validator("export_format", mode="before")
    @classmethod
    def _normalize_format(cls, value: object) -> str:
        normalized = str(value or "json").strip().lower()
        if normalized not in {"json", "markdown"}:
            raise ValueError("export_format must be json or markdown")
        return normalized


def handle_model_eval_plan(query: ModelEvalPlanQuery, _subsystems: Any) -> dict[str, Any]:
    plan = build_suite_plan(
        suite_slugs=query.suite_slugs,
        workflow_spec_paths=query.workflow_spec_paths,
        model_configs=query.model_configs or None,
        prompt_variants=query.prompt_variants or None,
        max_workflow_jobs=query.max_workflow_jobs,
        run_mode=query.run_mode,
    )
    plan["operation"] = "model_eval_plan"
    return plan


def handle_model_eval_inspect(query: ModelEvalInspectQuery, _subsystems: Any) -> dict[str, Any]:
    summary = _load_summary(query.lab_run_id)
    if not summary.get("ok") and summary.get("error_code"):
        case_runs = _load_case_runs_for_lab_run(_subsystems, query.lab_run_id)
        scorecards = _load_scorecards_for_lab_run(_subsystems, query.lab_run_id)
        if not case_runs and not scorecards:
            return summary
        return {
            "ok": True,
            "operation": "model_eval_inspect",
            "lab_run_id": query.lab_run_id,
            "case_runs": case_runs if query.include_results else [],
            "scorecards": scorecards,
            "source": "model_eval_db",
        }
    if not query.include_results:
        summary = dict(summary)
        summary.pop("results", None)
    case_runs = _load_case_runs_for_lab_run(_subsystems, query.lab_run_id)
    scorecards = _load_scorecards_for_lab_run(_subsystems, query.lab_run_id)
    if case_runs or scorecards:
        summary = dict(summary)
        summary["db_case_runs"] = case_runs if query.include_results else []
        summary["db_scorecards"] = scorecards
    summary["operation"] = "model_eval_inspect"
    return summary


def handle_model_eval_compare(query: ModelEvalCompareQuery, _subsystems: Any) -> dict[str, Any]:
    scorecards = _load_scorecards_for_lab_run(_subsystems, query.lab_run_id)
    if scorecards:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in scorecards:
            grouped.setdefault(str(row.get("family") or "unknown"), []).append(row)
        winners = []
        for family, rows in grouped.items():
            ranked = sorted(
                rows,
                key=lambda item: (
                    -float(item.get("pass_at_1") or 0.0),
                    -float(item.get("mean_score") or 0.0),
                    float(item.get("mean_cost_usd") or 999999.0),
                    float(item.get("mean_latency_ms") or 999999.0),
                ),
            )
            if ranked:
                best = dict(ranked[0])
                best["group_key"] = family
                winners.append(best)
        return {
            "ok": True,
            "operation": "model_eval_compare",
            "lab_run_id": query.lab_run_id,
            "source": "model_eval_scorecards",
            "scorecards": scorecards,
            "winners_by_family": winners,
        }
    summary = _load_summary(query.lab_run_id)
    if not summary.get("ok") and summary.get("error_code"):
        return summary
    results = [dict(item) for item in summary.get("results") or [] if isinstance(item, dict)]
    return {
        "ok": True,
        "operation": "model_eval_compare",
        "lab_run_id": query.lab_run_id,
        "artifact_root": summary.get("artifact_root"),
        "total_cost_usd": summary.get("total_cost_usd"),
        "executed_count": summary.get("executed_count"),
        "winners_by_task": _best_rows(results),
    }


def handle_model_eval_export(query: ModelEvalExportQuery, _subsystems: Any) -> dict[str, Any]:
    summary = _load_summary(query.lab_run_id)
    if not summary.get("ok") and summary.get("error_code"):
        return summary
    if query.export_format == "json":
        return {
            "ok": True,
            "operation": "model_eval_export",
            "export_format": "json",
            "content": json.dumps(summary, indent=2, sort_keys=True),
        }
    winners = _best_rows([dict(item) for item in summary.get("results") or [] if isinstance(item, dict)])
    lines = [
        "# Model Eval Export",
        "",
        f"- run: {query.lab_run_id}",
        f"- cost: {summary.get('total_cost_usd')}",
        f"- executed: {summary.get('executed_count')}",
        "",
        "## Winners",
    ]
    for row in winners:
        lines.append(
            f"- {row.get('group_key')}: {row.get('config_id')} "
            f"score={row.get('score')} cost={row.get('cost')}"
        )
    return {
        "ok": True,
        "operation": "model_eval_export",
        "export_format": "markdown",
        "content": "\n".join(lines) + "\n",
    }


__all__ = [
    "ModelEvalCompareQuery",
    "ModelEvalExportQuery",
    "ModelEvalInspectQuery",
    "ModelEvalPlanQuery",
    "handle_model_eval_compare",
    "handle_model_eval_export",
    "handle_model_eval_inspect",
    "handle_model_eval_plan",
]
