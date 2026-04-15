"""Tools: praxis_data."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from runtime.control_plane_manifests import (
    CONTROL_MANIFEST_FAMILY as _CONTROL_MANIFEST_FAMILY,
    CONTROL_MANIFEST_KIND as _CONTROL_MANIFEST_KIND,
    DATA_APPROVAL_MANIFEST_TYPE as _CONTROL_APPROVAL_MANIFEST_TYPE,
    DATA_CHECKPOINT_MANIFEST_TYPE as _CONTROL_CHECKPOINT_MANIFEST_TYPE,
    DATA_PLAN_MANIFEST_TYPE as _CONTROL_PLAN_MANIFEST_TYPE,
    load_control_plane_manifest as _load_control_plane_manifest_record,
)
from runtime.data_plane import (
    DataRuntimeBoundaryError,
    build_data_workflow_spec,
    execute_data_job,
    write_workflow_spec,
)
from ..subsystems import REPO_ROOT, _subs


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _manifest_payload(value: Any, *, field_name: str) -> dict[str, Any]:
    payload = value
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DataRuntimeBoundaryError(
                "data.manifest.invalid_json",
                f"{field_name} must be valid JSON",
                details={"field": field_name},
            ) from exc
    if not isinstance(payload, dict):
        raise DataRuntimeBoundaryError(
            "data.manifest.invalid_payload",
            f"{field_name} must be a JSON object",
            details={"field": field_name},
        )
    return dict(payload)


def _manifest_ref(row: dict[str, Any]) -> dict[str, Any]:
    manifest = _manifest_payload(row.get("manifest"), field_name="manifest")
    kind = _text(manifest.get("kind"))
    family = _text(manifest.get("manifest_family"))
    manifest_type = _text(manifest.get("manifest_type"))
    if kind != _CONTROL_MANIFEST_KIND or family != _CONTROL_MANIFEST_FAMILY:
        raise DataRuntimeBoundaryError(
            "data.manifest.invalid_family",
            "manifest_id must reference a control-plane manifest",
            details={
                "manifest_id": str(row.get("id") or ""),
                "kind": kind,
                "manifest_family": family,
            },
        )
    if manifest_type not in {
        _CONTROL_PLAN_MANIFEST_TYPE,
        _CONTROL_APPROVAL_MANIFEST_TYPE,
        _CONTROL_CHECKPOINT_MANIFEST_TYPE,
    }:
        raise DataRuntimeBoundaryError(
            "data.manifest.invalid_type",
            "manifest_id must reference a data plan, data approval, or data checkpoint manifest",
            details={
                "manifest_id": str(row.get("id") or ""),
                "manifest_type": manifest_type,
            },
        )
    return {
        "manifest_id": str(row.get("id") or ""),
        "name": _text(row.get("name")),
        "description": _text(row.get("description")),
        "status": _text(row.get("status")),
        "updated_at": row.get("updated_at"),
        "kind": kind,
        "manifest_family": family,
        "manifest_type": manifest_type,
        "manifest": manifest,
    }


def _load_control_manifest(manifest_id: str) -> dict[str, Any]:
    pg = _subs.get_pg_conn()
    if pg is None:
        raise DataRuntimeBoundaryError(
            "data.manifest.postgres_unavailable",
            "manifest_id requires a Postgres connection",
            details={"manifest_id": manifest_id},
        )
    try:
        row = _load_control_plane_manifest_record(pg, manifest_id=manifest_id)
    except Exception as exc:
        raise DataRuntimeBoundaryError(
            getattr(exc, "reason_code", "data.manifest.not_found"),
            str(exc),
            details=getattr(exc, "details", {"manifest_id": manifest_id}),
        ) from exc
    return _manifest_ref(dict(row))


def _control_manifest_body(manifest_ref: dict[str, Any], *, body_field: str) -> dict[str, Any]:
    manifest = manifest_ref.get("manifest")
    if not isinstance(manifest, dict):
        raise DataRuntimeBoundaryError(
            "data.manifest.invalid_payload",
            f"{manifest_ref.get('manifest_id', 'manifest')} has no manifest payload",
            details={"manifest_id": manifest_ref.get("manifest_id")},
        )
    body = manifest.get(body_field)
    if not isinstance(body, dict):
        raise DataRuntimeBoundaryError(
            "data.manifest.invalid_body",
            f"{manifest_ref.get('manifest_id', 'manifest')} is missing {body_field}",
            details={
                "manifest_id": manifest_ref.get("manifest_id"),
                "body_field": body_field,
            },
        )
    return dict(body)


def _resolve_manifest_backed_inputs(
    job: dict[str, Any],
    *,
    action: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    plan_ref: dict[str, Any] = {}
    approval_ref: dict[str, Any] = {}
    checkpoint_ref: dict[str, Any] = {}
    plan_manifest_id = _text(job.pop("plan_manifest_id", None))
    approval_manifest_id = _text(job.pop("approval_manifest_id", None))
    checkpoint_manifest_id = _text(job.pop("checkpoint_manifest_id", None))

    if action not in {"approve", "apply", "replay", "sync"}:
        if plan_manifest_id or approval_manifest_id or checkpoint_manifest_id:
            raise DataRuntimeBoundaryError(
                "data.manifest.unsupported_action",
                "manifest ids are only supported for approve, apply, replay, and sync",
                details={"action": action},
            )
        return job, plan_ref, approval_ref, checkpoint_ref

    if plan_manifest_id:
        plan_ref = _load_control_manifest(plan_manifest_id)
        if _text(plan_ref.get("manifest_type")) != _CONTROL_PLAN_MANIFEST_TYPE:
            raise DataRuntimeBoundaryError(
                "data.manifest.expected_plan",
                "plan_manifest_id must reference a data plan manifest",
                details={"manifest_id": plan_manifest_id},
            )
        job["plan_manifest_id"] = plan_manifest_id

    if action == "apply" and approval_manifest_id:
        approval_ref = _load_control_manifest(approval_manifest_id)
        if _text(approval_ref.get("manifest_type")) != _CONTROL_APPROVAL_MANIFEST_TYPE:
            raise DataRuntimeBoundaryError(
                "data.manifest.expected_approval",
                "approval_manifest_id must reference a data approval manifest",
                details={"manifest_id": approval_manifest_id},
            )
        job["approval_manifest_id"] = approval_manifest_id

    if action in {"replay", "sync"} and checkpoint_manifest_id:
        checkpoint_ref = _load_control_manifest(checkpoint_manifest_id)
        if _text(checkpoint_ref.get("manifest_type")) != _CONTROL_CHECKPOINT_MANIFEST_TYPE:
            raise DataRuntimeBoundaryError(
                "data.manifest.expected_checkpoint",
                "checkpoint_manifest_id must reference a data checkpoint manifest",
                details={"manifest_id": checkpoint_manifest_id},
            )
        job["checkpoint_manifest_id"] = checkpoint_manifest_id

    return job, plan_ref, approval_ref, checkpoint_ref


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
        job, plan_ref, approval_ref, checkpoint_ref = _resolve_manifest_backed_inputs(job, action=action)
        default_operation = None if action == "run" else action
        result = execute_data_job(
            job,
            default_operation=default_operation,
            workspace_root=workspace_root,
            pg_conn=_subs.get_pg_conn(),
            dry_run=bool(params.get("dry_run", False)),
        )
        if plan_ref:
            result["plan_manifest_id"] = plan_ref.get("manifest_id")
            result["plan_manifest"] = {
                key: value
                for key, value in plan_ref.items()
                if key != "manifest"
            }
        if approval_ref:
            result["approval_manifest_id"] = approval_ref.get("manifest_id")
            result["approval_manifest"] = {
                key: value
                for key, value in approval_ref.items()
                if key != "manifest"
            }
        if checkpoint_ref:
            result["checkpoint_manifest_id"] = checkpoint_ref.get("manifest_id")
            result["checkpoint_manifest"] = {
                key: value
                for key, value in checkpoint_ref.items()
                if key != "manifest"
            }
        return result
    except Exception as exc:
        return _tool_error(exc, action=action)


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data": (
        tool_praxis_data,
        {
            "description": (
                "Run deterministic data cleanup and reconciliation jobs: parse datasets, profile fields, "
                "filter records, sort rows, normalize values, repair rows, run repair loops, backfill missing values, "
                "redact sensitive fields, checkpoint state, replay cursor windows, approve plans, apply approved plans, "
                "validate contracts, transform records, "
                "join or merge sources, aggregate groups, split partitions, export shaped datasets, dedupe keys, "
                "route dead-letter rows, reconcile source vs target state, sync target state deterministically, "
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
                "  Repair rows:       praxis_data(action='repair', input_path='users.json', "
                "predicates=[{'field':'status','op':'equals','value':'pending'}], repairs={'status': {'value':'active'}})\n"
                "  Repair loop:       praxis_data(action='repair_loop', input_path='users.json', "
                "repairs={'status': {'value':'active'}}, schema={'email': {'required': true, 'regex': '.+@.+'}})\n"
                "  Backfill values:   praxis_data(action='backfill', input_path='users.json', "
                "backfill={'country': {'value':'US'}})\n"
                "  Redact PII:        praxis_data(action='redact', input_path='users.json', "
                "redactions={'email':'mask_email','ssn':'remove'})\n"
                "  Checkpoint state:  praxis_data(action='checkpoint', input_path='events.json', "
                "keys=['id'], cursor_field='updated_at')\n"
                "  Replay window:     praxis_data(action='replay', input_path='events.json', "
                "cursor_field='updated_at', checkpoint_manifest_id='checkpoint_xyz789')\n"
                "  Approve plan:      praxis_data(action='approve', plan_manifest_id='plan_abc123', "
                "approved_by='ops', approval_reason='Reviewed diff and counts')\n"
                "  Apply plan:        praxis_data(action='apply', plan_manifest_id='plan_abc123', "
                "approval_manifest_id='approval_def456', secondary_input_path='target.json', keys=['id'])\n"
                "  Validate rows:     praxis_data(action='validate', input_path='artifacts/data/users.json', "
                "schema={'email': {'required': true, 'regex': '.+@.+'}})\n"
                "  Merge sources:     praxis_data(action='merge', input_path='crm.json', secondary_input_path='billing.json', "
                "keys=['id'], precedence='right')\n"
                "  Split rows:        praxis_data(action='split', input_path='users.json', split_by_field='status', "
                "output_path='artifacts/data/users_by_status')\n"
                "  Export fields:     praxis_data(action='export', input_path='users.json', "
                "fields=['id','email'], field_map={'email':'user_email'})\n"
                "  Route dead-letter: praxis_data(action='dead_letter', input_path='users.json', "
                "schema={'email': {'required': true, 'regex': '.+@.+'}}, output_path='artifacts/data/users_dead_letter')\n"
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
                            "repair",
                            "repair_loop",
                            "backfill",
                            "redact",
                            "checkpoint",
                            "replay",
                            "approve",
                            "apply",
                            "validate",
                            "transform",
                            "join",
                            "merge",
                            "aggregate",
                            "split",
                            "export",
                            "dead_letter",
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
                    "repairs": {"type": "object"},
                    "backfill": {"type": "object"},
                    "redactions": {"type": "object"},
                    "checkpoint": {"type": "object"},
                    "checkpoint_path": {"type": "string"},
                    "checkpoint_manifest_id": {"type": "string"},
                    "plan": {"type": "object"},
                    "plan_path": {"type": "string"},
                    "plan_manifest_id": {"type": "string"},
                    "approval": {"type": "object"},
                    "approval_path": {"type": "string"},
                    "approval_manifest_id": {"type": "string"},
                    "schema": {"type": "object"},
                    "checks": {"type": "array"},
                    "mapping": {"type": "object"},
                    "field_map": {"type": "object"},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "drop_fields": {"type": "array", "items": {"type": "string"}},
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
                    "cursor_field": {"type": "string"},
                    "after": {},
                    "before": {},
                    "approved_by": {"type": "string"},
                    "approval_reason": {"type": "string"},
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
                    "max_passes": {"type": "integer", "minimum": 1},
                    "batch_size": {"type": "integer", "minimum": 1},
                    "workflow_spec_path": {"type": "string"},
                    "wait": {"type": "boolean", "default": False},
                    "fresh": {"type": "boolean", "default": False},
                    "run_id": {"type": "string"},
                },
            },
        },
    ),
}
