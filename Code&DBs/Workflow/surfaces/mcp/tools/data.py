"""Tools: praxis_data."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from runtime.data_plane import (
    DataRuntimeBoundaryError,
    build_data_workflow_spec,
    execute_data_job,
    write_workflow_spec,
)
from ..subsystems import REPO_ROOT


def _tool_workspace_root(params: dict[str, Any]) -> Path:
    repo_root = Path(REPO_ROOT).expanduser().resolve()
    raw = params.get("workspace_root")
    if raw is None and isinstance(params.get("job"), dict):
        raw = dict(params["job"]).get("workspace_root")
    candidate = Path(raw or repo_root).expanduser().resolve()
    try:
        candidate.relative_to(repo_root)
    except ValueError as exc:
        raise DataRuntimeBoundaryError(
            "data.workspace_boundary_violation",
            "workspace_root must stay inside the Praxis repository root",
            details={"repo_root": str(repo_root), "workspace_root": str(candidate)},
        ) from exc
    return candidate


def _tool_error(exc: Exception, *, action: str) -> dict[str, Any]:
    if isinstance(exc, DataRuntimeBoundaryError):
        return {
            "error": str(exc),
            "error_code": exc.reason_code,
            "details": dict(exc.details),
            "action": action,
        }
    reason_code = getattr(exc, "reason_code", f"data.{action}.failed")
    details = getattr(exc, "details", None)
    payload: dict[str, Any] = {
        "error": str(exc),
        "error_code": reason_code,
        "action": action,
    }
    if isinstance(details, dict) and details:
        payload["details"] = details
    return payload


def _job_payload(params: dict[str, Any]) -> dict[str, Any]:
    job = dict(params.get("job") or {})
    for key, value in params.items():
        if key in {"action", "job", "wait", "workflow_spec_path", "fresh", "run_id"}:
            continue
        job[key] = value
    return job


def tool_praxis_data(params: dict[str, Any]) -> dict[str, Any]:
    """Deterministic data cleanup, validation, transformation, and workflow launch."""
    action = str(params.get("action") or "profile").strip().lower() or "profile"
    try:
        workspace_root = _tool_workspace_root(params)
        if action == "workflow_spec":
            job = _job_payload(params)
            spec = build_data_workflow_spec(job, workspace_root=workspace_root)
            spec_path = ""
            requested_path = str(params.get("workflow_spec_path") or "").strip()
            if requested_path:
                spec_path = write_workflow_spec(spec, workspace_root=workspace_root, output_path=requested_path)
            return {
                "ok": True,
                "action": action,
                "workflow_spec": spec,
                "workflow_spec_path": spec_path,
            }

        if action == "launch":
            from .workflow import tool_praxis_workflow

            job = _job_payload(params)
            spec = build_data_workflow_spec(job, workspace_root=workspace_root)
            spec_path = write_workflow_spec(
                spec,
                workspace_root=workspace_root,
                output_path=str(params.get("workflow_spec_path") or "").strip() or None,
            )
            result = tool_praxis_workflow(
                {
                    "action": "run",
                    "spec_path": spec_path,
                    "wait": bool(params.get("wait", False)),
                    "fresh": bool(params.get("fresh", False)),
                    **({"run_id": params["run_id"]} if params.get("run_id") else {}),
                }
            )
            return {
                "ok": not bool(result.get("error")),
                "action": action,
                "workflow_spec_path": spec_path,
                "workflow": result,
            }

        job = _job_payload(params)
        default_operation = None if action == "run" else action
        return execute_data_job(
            job,
            default_operation=default_operation,
            workspace_root=workspace_root,
            dry_run=bool(params.get("dry_run", False)),
        )
    except Exception as exc:
        return _tool_error(exc, action=action)


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data": (
        tool_praxis_data,
        {
            "description": (
                "Run deterministic data cleanup and reconciliation jobs: parse datasets, profile fields, "
                "filter records, sort rows, normalize values, redact sensitive fields, validate contracts, transform records, "
                "join or merge sources, aggregate groups, split partitions, export shaped datasets, dedupe keys, "
                "reconcile source vs target state, sync target state deterministically, "
                "generate workflow specs, and launch those jobs through Praxis.\n\n"
                "USE WHEN: the platform should own exact parsing, mapping, validation, or diff logic instead "
                "of asking an LLM to mutate rows heuristically.\n\n"
                "EXAMPLES:\n"
                "  Profile a file:    praxis_data(action='profile', input_path='artifacts/data/users.csv')\n"
                "  Filter rows:       praxis_data(action='filter', input_path='artifacts/data/users.csv', "
                "predicates=[{'field':'status','op':'equals','value':'active'}])\n"
                "  Join sources:      praxis_data(action='join', input_path='users.json', secondary_input_path='orders.json', "
                "keys=['user_id'], right_prefix='order_')\n"
                "  Aggregate rows:    praxis_data(action='aggregate', input_path='orders.json', group_by=['status'], "
                "aggregations=[{'op':'count','as':'row_count'}])\n"
                "  Normalize data:    praxis_data(action='normalize', input_path='artifacts/data/users.csv', "
                "rules={'email':['trim','lower']})\n"
                "  Redact PII:        praxis_data(action='redact', input_path='users.json', "
                "redactions={'email':'mask_email','ssn':'remove'})\n"
                "  Validate rows:     praxis_data(action='validate', input_path='artifacts/data/users.json', "
                "schema={'email': {'required': true, 'regex': '.+@.+'}})\n"
                "  Merge sources:     praxis_data(action='merge', input_path='crm.json', secondary_input_path='billing.json', "
                "keys=['id'], precedence='right')\n"
                "  Split rows:        praxis_data(action='split', input_path='users.json', split_by_field='status', "
                "output_path='artifacts/data/users_by_status')\n"
                "  Export fields:     praxis_data(action='export', input_path='users.json', "
                "fields=['id','email'], field_map={'email':'user_email'})\n"
                "  Reconcile state:   praxis_data(action='reconcile', input_path='source.json', "
                "secondary_input_path='target.json', keys=['id'])\n"
                "  Sync target:       praxis_data(action='sync', input_path='source.json', secondary_input_path='target.json', "
                "keys=['id'], sync_mode='mirror')\n"
                "  Build workflow:    praxis_data(action='workflow_spec', job={'operation':'normalize', "
                "'input_path':'source.csv', 'rules': {'email':['trim','lower']}})\n"
                "  Launch workflow:   praxis_data(action='launch', job={'operation':'dedupe', "
                "'input_path':'source.csv', 'keys':['email']})\n\n"
                "DO NOT USE: for fuzzy classification or natural-language inference. This tool is deterministic."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "parse",
                            "profile",
                            "filter",
                            "sort",
                            "normalize",
                            "redact",
                            "validate",
                            "transform",
                            "join",
                            "merge",
                            "aggregate",
                            "split",
                            "export",
                            "dedupe",
                            "reconcile",
                            "sync",
                            "run",
                            "workflow_spec",
                            "launch",
                        ],
                        "default": "profile",
                    },
                    "job": {
                        "type": "object",
                        "description": "Optional full deterministic data job. Use for action='run', 'workflow_spec', or 'launch'.",
                    },
                    "input_path": {"type": "string"},
                    "input_format": {"type": "string", "enum": ["json", "jsonl", "csv", "tsv"]},
                    "workspace_root": {"type": "string"},
                    "records": {"type": "array"},
                    "secondary_input_path": {"type": "string"},
                    "secondary_input_format": {"type": "string", "enum": ["json", "jsonl", "csv", "tsv"]},
                    "secondary_records": {"type": "array"},
                    "predicates": {"type": "array"},
                    "predicate_mode": {"type": "string", "enum": ["all", "any"]},
                    "sort": {"type": "array"},
                    "rules": {"type": "object"},
                    "redactions": {"type": "object"},
                    "schema": {"type": "object"},
                    "checks": {"type": "array"},
                    "mapping": {"type": "object"},
                    "field_map": {"type": "object"},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "keys": {"type": "array", "items": {"type": "string"}},
                    "left_keys": {"type": "array", "items": {"type": "string"}},
                    "right_keys": {"type": "array", "items": {"type": "string"}},
                    "compare_fields": {"type": "array", "items": {"type": "string"}},
                    "group_by": {"type": "array", "items": {"type": "string"}},
                    "aggregations": {"type": "array"},
                    "partitions": {"type": "array"},
                    "strategy": {
                        "type": "string",
                        "enum": ["first", "last", "most_complete", "latest_by_field"],
                    },
                    "order_field": {"type": "string"},
                    "split_by_field": {"type": "string"},
                    "join_kind": {"type": "string", "enum": ["inner", "left", "right", "full"]},
                    "merge_mode": {"type": "string", "enum": ["inner", "left", "right", "full"]},
                    "precedence": {"type": "string", "enum": ["left", "right"]},
                    "split_mode": {"type": "string", "enum": ["first_match", "all_matches"]},
                    "include_unmatched": {"type": "boolean"},
                    "left_prefix": {"type": "string"},
                    "right_prefix": {"type": "string"},
                    "sync_mode": {"type": "string", "enum": ["upsert", "mirror"]},
                    "output_path": {"type": "string"},
                    "output_format": {"type": "string", "enum": ["json", "jsonl", "csv", "tsv"]},
                    "receipt_path": {"type": "string"},
                    "dry_run": {"type": "boolean", "default": False},
                    "workflow_spec_path": {"type": "string"},
                    "wait": {"type": "boolean", "default": False},
                    "fresh": {"type": "boolean", "default": False},
                    "run_id": {"type": "string"},
                },
            },
        },
    ),
}
