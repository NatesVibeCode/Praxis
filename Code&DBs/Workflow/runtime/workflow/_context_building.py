"""Execution context, bundle, and packet building for workflow jobs."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from registry.sandbox_profile_authority import (
    load_runtime_sandbox_profile_authority,
    sandbox_profile_execution_payload,
)
from ._shared import (
    _json_loads_maybe,
    _json_safe,
    _normalize_paths,
    _normalize_string_list,
    _slugify,
    _workflow_run_envelope,
)
from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from runtime.receipt_store import proof_metrics
from runtime.scope_resolver import resolve_scope
from runtime.execution_packet_authority import (
    inspect_execution_packets,
    packet_inspection_from_row,
    resolve_execution_packet_revisions,
)
from runtime.workspace_paths import container_workspace_root
from runtime.failure_projection import project_failure_classification
from runtime.repo_policy_onboarding import (
    get_repo_policy_contract,
    repo_policy_runtime_payload,
)
from runtime.workflow.execution_bundle import (
    build_execution_bundle,
    render_execution_bundle,
)
from runtime.workflow.artifact_contracts import infer_artifact_write_scope
from runtime.workflow.decision_context import (
    explicit_authority_domains_for_job,
    resolve_job_decision_pack,
)
from runtime.workspace_paths import workflow_root
from runtime.workflow.submission_capture import (
    capture_submission_baseline_for_job as _submission_capture_baseline_for_job,
    get_submission_for_job_attempt as _submission_get_submission_for_job_attempt,
)
from runtime.workflow.verification_runtime import (
    extract_verification_paths as _verification_runtime_extract_verification_paths,
    get_verify_bindings as _verification_runtime_get_verify_bindings,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

_EXECUTION_MANIFEST_REQUIRED_SOURCE_KINDS = frozenset({"workflow_trigger", "integration_invoke"})

__all__ = [
    "_execution_model_messages",
    "_job_verify_refs",
    "_shadow_packet_inspection_from_rows",
    "_packet_authority_for_run",
    "_resolve_job_prompt_authority",
    "_normalized_job_write_scope",
    "_normalized_job_read_scope",
    "_proof_metrics_snapshot",
    "_job_execution_context_shard",
    "_build_job_execution_context_shards",
    "_build_job_execution_bundles",
    "_runtime_execution_context_shard",
    "_render_execution_context_shard",
    "_spec_snapshot_job",
    "_runtime_execution_bundle",
    "_persist_runtime_context_for_job",
    "_submission_completion_contract",
    "_submission_required_for_bundle",
    "_capture_submission_baseline_if_required",
    "_verification_artifact_refs",
    "_build_execution_packet",
    "_workflow_row_reuse_authority",
    "_terminal_failure_classification",
    "_extract_verification_paths",
]


# ---------------------------------------------------------------------------
# Internal helpers (dependencies defined outside the extraction range but
# needed by the extracted functions).
# ---------------------------------------------------------------------------


def _runtime_profile_sandbox_payload(
    conn: SyncPostgresConnection,
    *,
    runtime_profile_ref: str | None,
) -> dict[str, object] | None:
    normalized_runtime_profile_ref = str(runtime_profile_ref or "").strip()
    if not normalized_runtime_profile_ref:
        return None
    record = load_runtime_sandbox_profile_authority(
        conn,
        runtime_profile_ref=normalized_runtime_profile_ref,
    )
    return sandbox_profile_execution_payload(record)


def _execution_manifest_for_snapshot(
    conn: SyncPostgresConnection,
    *,
    raw_snapshot: dict[str, object] | None,
    workflow_id: str | None,
) -> dict[str, object] | None:
    def _normalized_execution_manifest(payload: dict[str, object]) -> dict[str, object]:
        if "tool_allowlist" in payload and "verify_refs" in payload:
            return json.loads(json.dumps(payload, default=str))
        tool_allowlist = payload.get("tool_allowlist_json")
        verify_refs = payload.get("verify_refs_json")
        approved_bundle_refs = payload.get("approved_bundle_refs_json")
        compiled_spec = payload.get("compiled_spec_json")
        policy_gates = payload.get("policy_gates_json")
        hardening_report = payload.get("hardening_report_json")
        if isinstance(tool_allowlist, dict):
            return {
                "execution_manifest_ref": str(payload.get("execution_manifest_ref") or "").strip() or None,
                "workflow_id": str(payload.get("workflow_id") or "").strip() or None,
                "definition_revision": str(payload.get("definition_revision") or "").strip() or None,
                "manifest_ref": str(payload.get("manifest_ref") or "").strip() or None,
                "review_group_ref": str(payload.get("review_group_ref") or "").strip() or None,
                "approved_bundle_refs": _normalize_string_list(approved_bundle_refs),
                "tool_allowlist": json.loads(json.dumps(tool_allowlist, default=str)),
                "verify_refs": _normalize_string_list(verify_refs),
                "compiled_spec": json.loads(json.dumps(compiled_spec, default=str))
                if isinstance(compiled_spec, dict)
                else {},
                "policy_gates": json.loads(json.dumps(policy_gates, default=str))
                if isinstance(policy_gates, dict)
                else {},
                "hardening_report": json.loads(json.dumps(hardening_report, default=str))
                if isinstance(hardening_report, dict)
                else {},
            }
        return json.loads(json.dumps(payload, default=str))

    snapshot = dict(raw_snapshot or {})
    embedded_manifest = snapshot.get("execution_manifest")
    if isinstance(embedded_manifest, dict):
        return _normalized_execution_manifest(dict(embedded_manifest))

    execution_manifest_ref = str(snapshot.get("execution_manifest_ref") or "").strip()
    definition_revision = str(snapshot.get("definition_revision") or "").strip()
    normalized_workflow_id = str(workflow_id or "").strip()
    if not execution_manifest_ref and (not normalized_workflow_id or not definition_revision):
        return None

    try:
        from storage.postgres.workflow_build_planning_repository import (
            load_latest_workflow_build_execution_manifest,
            load_workflow_build_execution_manifest_by_ref,
        )
    except Exception:
        return None

    try:
        if execution_manifest_ref:
            manifest = load_workflow_build_execution_manifest_by_ref(
                conn,
                execution_manifest_ref=execution_manifest_ref,
            )
            if isinstance(manifest, dict):
                return _normalized_execution_manifest(manifest)
        if normalized_workflow_id and definition_revision:
            manifest = load_latest_workflow_build_execution_manifest(
                conn,
                workflow_id=normalized_workflow_id,
                definition_revision=definition_revision,
            )
            if isinstance(manifest, dict):
                return _normalized_execution_manifest(manifest)
    except Exception:
        return None
    return None


def _execution_source_kind(raw_snapshot: dict[str, object] | None) -> str | None:
    snapshot = dict(raw_snapshot or {})
    packet_provenance = snapshot.get("packet_provenance")
    if isinstance(packet_provenance, dict):
        value = str(packet_provenance.get("source_kind") or "").strip()
        if value:
            return value
    value = str(snapshot.get("source_kind") or "").strip()
    return value or None


def _requires_reviewed_execution_manifest(raw_snapshot: dict[str, object] | None) -> bool:
    return (_execution_source_kind(raw_snapshot) or "") in _EXECUTION_MANIFEST_REQUIRED_SOURCE_KINDS

# ---------------------------------------------------------------------------
# Extracted functions (lines ~260-1114 of unified.py)
# ---------------------------------------------------------------------------

def build_platform_context(repo_root: str) -> str:
    """Platform-context block injected into worker-bound prompts.

    BUG-D3CD86B8: moved from a private function in _execution_core.py so
    preview and execution share one authority. Previously preview built
    messages without this context, but real execution prepended it, making
    preview's rendered_prompt drift from what the backend actually sees.
    """
    try:
        from runtime._workflow_database import resolve_runtime_database_url
        from runtime.primitive_contracts import redact_url

        database_url = str(resolve_runtime_database_url(required=False) or "unavailable")
        database_ref = (
            "unavailable"
            if database_url == "unavailable"
            else (redact_url(database_url) or "<configured>")
        )
    except Exception:
        database_ref = "unavailable"
    return (
        "--- PLATFORM CONTEXT ---\n"
        f"Host repo root (persistence/output authority): {repo_root}\n"
        f"Command workspace: sandboxed workflow execution typically runs inside a hydrated workspace such as {container_workspace_root()}.\n"
        "Use the live command workspace for shell commands and relative paths; do not assume the host repo path exists inside the sandbox.\n"
        f"Database: {database_ref}\n"
        "--- END PLATFORM CONTEXT ---"
    )


def assemble_full_prompt(
    *,
    prompt: str,
    platform_context: str,
    execution_context_shard_text: str,
    execution_bundle_text: str,
) -> str:
    """Single-source prompt assembly matching execution-core's concatenation
    order: prompt + platform_context + execution_context_shard + bundle.

    BUG-D3CD86B8: sharing this helper between preview and execution keeps
    preview's rendered_full_prompt byte-identical to what the backend sees.
    The parity guarantee is: if preview and execution are called with the
    same inputs, both paths produce the same final string.
    """
    parts: list[str] = []
    if prompt:
        parts.append(prompt)
    if platform_context:
        parts.append(platform_context)
    if execution_context_shard_text:
        parts.append(execution_context_shard_text)
    if execution_bundle_text:
        parts.append(execution_bundle_text)
    return "\n\n".join(parts)


def _execution_model_messages(job: dict[str, object]) -> list[dict[str, str]]:
    """Build preview/helper messages WITHOUT platform_context.

    Historical note (BUG-D3CD86B8): this helper is kept for backward
    compatibility with callers that want the pre-platform-context prompt
    shape. The true backend-bound prompt should be assembled via
    ``assemble_full_prompt`` — preview now exposes both so operators can
    see the pre-platform and the final backend-bound forms side-by-side.
    """
    execution_context = job.get("_execution_context")
    execution_context_text = _render_execution_context_shard(execution_context)
    execution_bundle = job.get("_execution_bundle")
    execution_bundle_text = render_execution_bundle(execution_bundle if isinstance(execution_bundle, dict) else None)
    messages: list[dict[str, str]] = []
    system_prompt = str(job.get("system_prompt") or "").strip()
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    prompt = str(job.get("prompt") or "")
    if execution_context_text:
        prompt = f"{prompt}\n\n{execution_context_text}"
    if execution_bundle_text:
        prompt = f"{prompt}\n\n{execution_bundle_text}" if prompt else execution_bundle_text
    messages.append({"role": "user", "content": prompt})
    return messages


def _job_verify_refs(job: dict[str, object]) -> list[str]:
    refs: list[str] = []
    refs.extend(_normalize_string_list(job.get("verify_refs")))
    return list(dict.fromkeys(refs))


def _shadow_packet_inspection_from_rows(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    run_row: dict,
) -> dict | None:
    materialized = packet_inspection_from_row(run_row)
    if materialized is not None:
        return materialized
    try:
        packet_rows = conn.execute(
            """
            SELECT COALESCE(
                jsonb_agg(payload ORDER BY created_at, execution_packet_id),
                '[]'::jsonb
            ) AS packets
            FROM execution_packets
            WHERE run_id = $1
            """,
            run_id,
        )
    except Exception:
        return None

    if not packet_rows:
        return None

    packets = packet_rows[0].get("packets")
    if isinstance(packets, str):
        try:
            packets = json.loads(packets)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None
    if not isinstance(packets, list) or not packets:
        return None

    try:
        return inspect_execution_packets(packets, run_row=run_row)
    except Exception:
        return None


def _packet_authority_for_run(run_row: dict) -> tuple[str | None, str | None]:
    envelope = _workflow_run_envelope(run_row)
    snapshot = _json_loads_maybe(envelope.get("spec_snapshot"), {}) or {}
    definition_revision = str(snapshot.get("definition_revision") or "").strip() or None
    plan_revision = str(snapshot.get("plan_revision") or "").strip() or None
    if definition_revision is None and plan_revision is None:
        return None, None
    if definition_revision and plan_revision:
        return definition_revision, plan_revision

    from runtime.execution_packet_runtime import ExecutionPacketRuntimeError

    raise ExecutionPacketRuntimeError(
        "execution_packet.authority_missing",
        "migrated runtime execution is missing definition or plan revision authority",
    )


def _resolve_job_prompt_authority(
    conn: SyncPostgresConnection,
    *,
    job: dict,
    run_row: dict,
) -> tuple[
    str,           # prompt
    str | None,    # system_prompt (always None in unified path)
    bool,          # packet_only_runtime (always False in unified path)
    dict[str, object] | None,  # execution_bundle
    dict[str, object] | None,  # execution_context_shard
]:
    """Resolve job prompt and runtime context.

    Single path: job.prompt is truth.  Execution bundle and context shard
    are loaded from workflow_job_runtime_context if persisted at submission
    time, otherwise built fresh at execution time by the caller.
    """
    prompt = str(job.get("prompt") or "")
    run_id = str(run_row.get("run_id") or "").strip()
    label = str(job.get("label") or "").strip()

    # Load persisted runtime context (written at submission time)
    execution_bundle: dict[str, object] | None = None
    execution_context_shard: dict[str, object] | None = None
    if run_id and label:
        rows = conn.execute(
            "SELECT execution_context_shard, execution_bundle "
            "FROM workflow_job_runtime_context "
            "WHERE run_id = $1 AND job_label = $2 LIMIT 1",
            run_id, label,
        )
        if rows:
            raw_shard = rows[0].get("execution_context_shard")
            raw_bundle = rows[0].get("execution_bundle")
            execution_context_shard = dict(raw_shard) if isinstance(raw_shard, dict) else None
            execution_bundle = dict(raw_bundle) if isinstance(raw_bundle, dict) else None

    return prompt, None, False, execution_bundle, execution_context_shard


def _normalized_job_write_scope(job: dict[str, object]) -> list[str]:
    # Accept all three authoring keys so raw spec_snapshot (top-level `write`)
    # and normalized spec.jobs (`scope.write` or `write_scope`) resolve identically.
    write_scope = _normalize_paths(job.get("write_scope"))
    if write_scope:
        return write_scope
    scope = job.get("scope") or {}
    if isinstance(scope, dict):
        scoped = _normalize_paths(scope.get("write"))
        if scoped:
            return scoped
    write_scope = _normalize_paths(job.get("write"))
    if write_scope:
        return write_scope
    return infer_artifact_write_scope(job)


def _normalized_job_read_scope(job: dict[str, object]) -> list[str]:
    read_scope = job.get("read_scope")
    if isinstance(read_scope, dict):
        combined: list[str] = []
        for value in read_scope.values():
            combined.extend(_normalize_paths(value))
        return list(dict.fromkeys(combined))
    normalized = _normalize_paths(read_scope)
    if normalized:
        return normalized
    scope = job.get("scope") or {}
    if isinstance(scope, dict):
        scoped = _normalize_paths(scope.get("read"))
        if scoped:
            return scoped
    return _normalize_paths(job.get("read"))


def _proof_metrics_snapshot(conn: SyncPostgresConnection) -> dict[str, object]:
    try:
        metrics = proof_metrics(conn=conn)
    except Exception:
        return {}
    receipts = metrics.get("receipts") if isinstance(metrics, dict) else None
    compile_authority = metrics.get("compile_authority") if isinstance(metrics, dict) else None
    if not isinstance(receipts, dict) and not isinstance(compile_authority, dict):
        return {}
    snapshot: dict[str, object] = {}
    if isinstance(receipts, dict):
        snapshot["receipts"] = {
            "total": int(receipts.get("total") or 0),
            "verification_coverage": float(receipts.get("verification_coverage") or 0.0),
            "fully_proved_verification_coverage": float(
                receipts.get("fully_proved_verification_coverage") or 0.0
            ),
            "write_manifest_coverage": float(receipts.get("write_manifest_coverage") or 0.0),
        }
    if isinstance(compile_authority, dict):
        snapshot["compile_authority"] = {
            "execution_packets_ready": bool(compile_authority.get("execution_packets_ready")),
            "verify_refs_ready": bool(compile_authority.get("verify_refs_ready")),
            "verification_registry_ready": bool(compile_authority.get("verification_registry_ready")),
            "repo_snapshots_ready": bool(compile_authority.get("repo_snapshots_ready")),
        }
    return snapshot


def _scope_resolution_root(
    *,
    repo_root: str | None,
    scope_paths: list[str],
) -> str:
    configured = str(repo_root or "").strip()
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured))
    try:
        container_root = container_workspace_root()
    except Exception:
        container_root = None
    if container_root is not None and container_root not in candidates:
        candidates.append(container_root)

    normalized_scope = [path.strip().lstrip("./") for path in scope_paths if path.strip()]
    for candidate in candidates:
        if any((candidate / path).exists() for path in normalized_scope):
            return str(candidate)
    return configured


def _job_execution_context_shard(
    *,
    conn: SyncPostgresConnection,
    job: dict[str, object],
    spec_verify_refs: list[str],
    repo_root: str | None,
    proof_snapshot: dict[str, object],
) -> dict[str, object]:
    label = str(job.get("label") or "").strip()
    write_scope = _normalized_job_write_scope(job)
    declared_read_scope = _normalized_job_read_scope(job)
    verify_refs = list(dict.fromkeys([*_job_verify_refs(job), *spec_verify_refs]))

    shard: dict[str, object] = {
        "job_label": label,
        "write_scope": write_scope,
        "declared_read_scope": declared_read_scope,
        "verify_refs": verify_refs,
        "metrics": {
            "write_scope_count": len(write_scope),
            "declared_read_scope_count": len(declared_read_scope),
            "verify_ref_count": len(verify_refs),
        },
    }
    if proof_snapshot:
        shard["proof_metrics"] = json.loads(json.dumps(proof_snapshot, default=str))

    normalized_repo_root = _scope_resolution_root(
        repo_root=repo_root,
        scope_paths=[*write_scope, *declared_read_scope],
    )
    if not normalized_repo_root or not write_scope:
        return shard

    try:
        resolution = resolve_scope(write_scope, root_dir=normalized_repo_root)
    except Exception as exc:
        shard["scope_resolution_error"] = str(exc)
        return shard

    shard["resolved_read_scope"] = list(resolution.computed_read_scope)
    shard["test_scope"] = list(resolution.test_scope)
    shard["blast_radius"] = list(resolution.blast_radius)
    shard["context_sections"] = json.loads(json.dumps(list(resolution.context_sections), default=str))

    # Inject test commands from resolved test_scope
    if resolution.test_scope:
        test_commands = []
        for tp in resolution.test_scope:
            rel = tp.replace(normalized_repo_root + "/", "").replace(
                normalized_repo_root, ""
            )
            test_commands.append(
                f"PYTHONPATH='{workflow_root()}' python3 -m pytest --noconftest -q {rel}"
            )
        if test_commands:
            shard["test_commands"] = test_commands

    # Inject valid enum values for tables touched by write_scope
    try:
        import re as _re
        # Find db_tables referenced by modules in write_scope
        table_names: set[str] = set()
        for wp in write_scope:
            rel = wp.replace(normalized_repo_root + "/", "").replace(
                normalized_repo_root, ""
            )
            rows = conn.execute(
                "SELECT behavior FROM module_embeddings WHERE module_path LIKE '%' || $1",
                rel,
            )
            for r in rows or []:
                b = r["behavior"] if isinstance(r["behavior"], dict) else json.loads(r["behavior"])
                table_names.update(b.get("db_tables") or [])
        if table_names:
            ck_rows = conn.execute("""
                SELECT conrelid::regclass::text AS table_name,
                       pg_get_constraintdef(oid) AS check_def
                FROM pg_constraint
                WHERE contype = 'c' AND connamespace = 'public'::regnamespace
            """)
            valid_values: dict[str, dict[str, list[str]]] = {}
            for r in ck_rows or []:
                tname = r["table_name"]
                if tname not in table_names:
                    continue
                defn = r["check_def"] or ""
                array_match = _re.search(r"ARRAY\[(.+?)\]", defn)
                if not array_match:
                    continue
                col_match = _re.search(r"\(+\s*\(?(\w+)\)?", defn)
                if not col_match:
                    continue
                values = _re.findall(r"'([^']+)'", array_match.group(1))
                if values:
                    valid_values.setdefault(tname, {})[col_match.group(1)] = values
            if valid_values:
                shard["valid_values"] = valid_values
    except Exception:
        pass

    shard["metrics"] = {
        **dict(shard["metrics"]),
        "resolved_read_scope_count": len(resolution.computed_read_scope),
        "test_scope_count": len(resolution.test_scope),
        "blast_radius_count": len(resolution.blast_radius),
        "context_section_count": len(resolution.context_sections),
    }
    return shard


def _build_job_execution_context_shards(
    *,
    conn: SyncPostgresConnection,
    spec,
    raw_snapshot: dict[str, object],
    provenance: dict[str, object] | None,
) -> dict[str, dict[str, object]]:
    provenance = dict(provenance or {})
    repo_root = str(provenance.get("repo_root") or "").strip()
    spec_verify_refs = _normalize_string_list(raw_snapshot.get("verify_refs"))
    proof_snapshot = _proof_metrics_snapshot(conn)
    shards: dict[str, dict[str, object]] = {}
    for index, job in enumerate(spec.jobs):
        label = str(job.get("label") or f"job_{index}")
        shards[label] = _job_execution_context_shard(
            conn=conn,
            job=job,
            spec_verify_refs=spec_verify_refs,
            repo_root=repo_root or None,
            proof_snapshot=proof_snapshot,
        )
    return shards


def _build_job_execution_bundles(
    *,
    conn: SyncPostgresConnection,
    spec,
    raw_snapshot: dict[str, object] | None,
    execution_context_shards: dict[str, dict[str, object]],
    run_id: str | None = None,
    workflow_id: str | None = None,
    runtime_profile_ref: str | None = None,
) -> dict[str, dict[str, object]]:
    downstream_by_label: dict[str, list[str]] = {}
    for index, job in enumerate(spec.jobs):
        child_label = str(job.get("label") or f"job_{index}")
        for dependency in _normalize_paths(job.get("depends_on")):
            downstream_by_label.setdefault(dependency, []).append(child_label)
    bundles: dict[str, dict[str, object]] = {}
    execution_manifest = _execution_manifest_for_snapshot(
        conn,
        raw_snapshot=raw_snapshot,
        workflow_id=workflow_id,
    )
    repo_policy_contract = repo_policy_runtime_payload(
        get_repo_policy_contract(conn, repo_root=workflow_root())
    )
    require_manifest_authority = _requires_reviewed_execution_manifest(raw_snapshot)
    if require_manifest_authority and not isinstance(execution_manifest, dict):
        raise RuntimeError(
            "builder-originated workflow execution requires ExecutionManifest authority; "
            "submission/runtime prompt fallback is not permitted",
        )
    for index, job in enumerate(spec.jobs):
        label = str(job.get("label") or f"job_{index}")
        context_shard = execution_context_shards.get(label) or {}
        explicit_authority_domains = explicit_authority_domains_for_job(
            job=job,
            spec_snapshot=raw_snapshot,
        )
        decision_pack = resolve_job_decision_pack(
            conn,
            write_scope=_normalize_paths(context_shard.get("write_scope")),
            declared_read_scope=_normalize_paths(context_shard.get("declared_read_scope")),
            resolved_read_scope=_normalize_paths(context_shard.get("resolved_read_scope")),
            blast_radius=_normalize_paths(context_shard.get("blast_radius")),
            explicit_authority_domains=explicit_authority_domains,
        )
        sandbox_profile = _runtime_profile_sandbox_payload(
            conn,
            runtime_profile_ref=runtime_profile_ref or getattr(spec, "runtime_profile_ref", None),
        )
        bundles[label] = build_execution_bundle(
            run_id=run_id,
            workflow_id=workflow_id,
            sandbox_profile_ref=(
                None
                if not isinstance(sandbox_profile, dict)
                else str(sandbox_profile.get("sandbox_profile_ref") or "").strip() or None
            ),
            sandbox_profile=sandbox_profile,
            job_label=label,
            prompt=str(job.get("prompt") or ""),
            task_type=str(job.get("task_type") or getattr(job.get("_route_plan"), "task_type", "") or job.get("route_task_type") or "").strip() or None,
            capabilities=_normalize_paths(job.get("capabilities")),
            allowed_tools=_normalize_paths(job.get("allowed_tools")),
            explicit_mcp_tools=_normalize_paths(job.get("mcp_tools")),
            explicit_skill_refs=_normalize_paths(job.get("skill_refs")),
            write_scope=_normalize_paths(context_shard.get("write_scope")),
            declared_read_scope=_normalize_paths(context_shard.get("declared_read_scope")),
            resolved_read_scope=_normalize_paths(context_shard.get("resolved_read_scope")),
            blast_radius=_normalize_paths(context_shard.get("blast_radius")),
            test_scope=_normalize_paths(context_shard.get("test_scope")),
            verify_refs=_normalize_paths(context_shard.get("verify_refs")),
            approval_required=job.get("approval_required")
            if isinstance(job.get("approval_required"), bool)
            else None,
            approval_question=str(job.get("approval_question") or "").strip() or None,
            context_sections=context_shard.get("context_sections")
            if isinstance(context_shard.get("context_sections"), list)
            else [],
            submission_required=job.get("submission_required")
            if isinstance(job.get("submission_required"), bool)
            else (
                # Specs express submission requirements nested under
                # completion_contract; flatten them through so the bundle
                # builder sees spec authority, not just task_type defaults.
                bool(job["completion_contract"].get("submission_required"))
                if isinstance(job.get("completion_contract"), dict)
                and isinstance(job["completion_contract"].get("submission_required"), bool)
                else None
            ),
            result_kind=(
                str(job["completion_contract"].get("result_kind") or "").strip() or None
                if isinstance(job.get("completion_contract"), dict)
                else None
            ),
            submit_tool_names=(
                [
                    str(name).strip()
                    for name in job["completion_contract"].get("submit_tool_names") or []
                    if str(name).strip()
                ]
                if isinstance(job.get("completion_contract"), dict)
                and isinstance(job["completion_contract"].get("submit_tool_names"), list)
                else None
            ),
            verification_required=(
                bool(job["completion_contract"].get("verification_required"))
                if isinstance(job.get("completion_contract"), dict)
                and isinstance(job["completion_contract"].get("verification_required"), bool)
                else None
            ),
            downstream_labels=downstream_by_label.get(label) or [],
            output_schema=job.get("output_schema")
            if isinstance(job.get("output_schema"), dict)
            else None,
            authoring_contract=job.get("authoring_contract")
            if isinstance(job.get("authoring_contract"), dict)
            else None,
            acceptance_contract=job.get("acceptance_contract")
            if isinstance(job.get("acceptance_contract"), dict)
            else None,
            decision_pack=decision_pack,
            execution_manifest=execution_manifest,
            repo_policy_contract=repo_policy_contract,
            require_manifest_authority=require_manifest_authority,
        )
    return bundles


def _runtime_execution_context_shard(
    conn: SyncPostgresConnection,
    *,
    job: dict[str, object],
    run_row: dict[str, object],
    repo_root: str,
) -> dict[str, object]:
    spec_job = _spec_snapshot_job(run_row, str(job.get("label") or "").strip())
    source_job = dict(spec_job or job)
    snapshot = _json_loads_maybe(_workflow_run_envelope(run_row).get("spec_snapshot"), {}) or {}
    shard = _job_execution_context_shard(
        conn=conn,
        job=source_job,
        spec_verify_refs=_normalize_string_list(snapshot.get("verify_refs")),
        repo_root=repo_root or None,
        proof_snapshot=_proof_metrics_snapshot(conn),
    )

    # Inject upstream job outputs so downstream agents see predecessor results
    job_id = job.get("id")
    if job_id is not None:
        upstream = _upstream_job_outputs(conn, job_id=int(job_id))
        if upstream:
            shard["upstream_outputs"] = upstream

    return shard


def _upstream_job_outputs(
    conn: SyncPostgresConnection,
    *,
    job_id: int,
) -> list[dict[str, object]]:
    """Collect outputs from successfully completed upstream (parent) jobs."""
    try:
        rows = conn.execute(
            """SELECT parent.label, parent.stdout_preview, parent.output_path
                 FROM workflow_job_edges edge
                 JOIN workflow_jobs parent ON parent.id = edge.parent_id
                WHERE edge.child_id = $1
                  AND parent.status = 'succeeded'
                ORDER BY parent.finished_at NULLS LAST""",
            job_id,
        )
    except Exception:
        return []
    outputs: list[dict[str, object]] = []
    for row in rows or []:
        entry: dict[str, object] = {"job_label": row["label"]}
        preview = str(row.get("stdout_preview") or "").strip()
        if preview:
            entry["summary"] = preview[:2000]
        output_path = str(row.get("output_path") or "").strip()
        if output_path:
            entry["output_path"] = output_path
        outputs.append(entry)
    return outputs


def _render_execution_context_shard(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return ""

    def _render_list(name: str, items: object) -> list[str]:
        normalized = _normalize_paths(items)
        if not normalized:
            return []
        return [f"{name}:\n" + "\n".join(f"- {item}" for item in normalized)]

    parts: list[str] = ["--- EXECUTION CONTEXT SHARD ---"]
    label = str(value.get("job_label") or "").strip()
    if label:
        parts.append(f"job_label: {label}")

    metrics = value.get("metrics")
    if isinstance(metrics, dict):
        # Only include metrics with non-zero values
        non_zero = {k: v for k, v in metrics.items() if v}
        if non_zero:
            parts.append(
                "scope_metrics: "
                + json.dumps(non_zero, sort_keys=True, separators=(",", ":"), default=str)
            )

    proof_snapshot = value.get("proof_metrics")
    if isinstance(proof_snapshot, dict):
        non_zero_proof = {k: v for k, v in proof_snapshot.items() if v}
        if non_zero_proof:
            parts.append(
                "proof_metrics: "
                + json.dumps(non_zero_proof, sort_keys=True, separators=(",", ":"), default=str)
            )

    parts.extend(_render_list("write_scope", value.get("write_scope")))
    parts.extend(_render_list("declared_read_scope", value.get("declared_read_scope")))
    parts.extend(_render_list("resolved_read_scope", value.get("resolved_read_scope")))
    parts.extend(_render_list("blast_radius", value.get("blast_radius")))
    parts.extend(_render_list("test_scope", value.get("test_scope")))
    parts.extend(_render_list("verify_refs", value.get("verify_refs")))

    context_sections = value.get("context_sections")
    if isinstance(context_sections, list) and context_sections:
        rendered_sections: list[str] = []
        for section in context_sections:
            if not isinstance(section, dict):
                continue
            name = str(section.get("name") or "").strip()
            content = str(section.get("content") or "").strip()
            if not name or not content:
                continue
            rendered_sections.append(f"## {name}\n{content}")
        if rendered_sections:
            parts.append("context_sections:\n" + "\n\n".join(rendered_sections))

    upstream = value.get("upstream_outputs")
    if isinstance(upstream, list) and upstream:
        upstream_parts: list[str] = []
        for entry in upstream:
            if not isinstance(entry, dict):
                continue
            label = str(entry.get("job_label") or "unknown").strip()
            summary = str(entry.get("summary") or "").strip()
            path = str(entry.get("output_path") or "").strip()
            lines = [f"### {label}"]
            if summary:
                lines.append(summary)
            if path:
                lines.append(f"full output: {path}")
            upstream_parts.append("\n".join(lines))
        if upstream_parts:
            parts.append("upstream_job_outputs:\n" + "\n\n".join(upstream_parts))

    resolution_error = str(value.get("scope_resolution_error") or "").strip()
    if resolution_error:
        parts.append(f"scope_resolution_error: {resolution_error}")

    parts.append("--- END EXECUTION CONTEXT SHARD ---")
    return "\n".join(parts)


def _spec_snapshot_job(run_row: dict[str, object], job_label: str) -> dict[str, object]:
    envelope = _workflow_run_envelope(run_row)
    snapshot = _json_loads_maybe(envelope.get("spec_snapshot"), {}) or {}
    jobs = snapshot.get("jobs")
    if not isinstance(jobs, list):
        return {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        label = str(job.get("label") or "").strip()
        if label and label == job_label:
            return dict(job)
    return {}


def _runtime_execution_bundle(
    conn: SyncPostgresConnection,
    *,
    job: dict[str, object],
    run_row: dict[str, object],
    repo_root: str,
    execution_context_shard: dict[str, object] | None = None,
) -> dict[str, object] | None:
    spec_job = _spec_snapshot_job(run_row, str(job.get("label") or "").strip())
    source_job = dict(spec_job or job)
    context_shard = dict(execution_context_shard or {})
    write_scope = _normalize_paths(context_shard.get("write_scope")) or _normalized_job_write_scope(source_job)
    declared_read_scope = _normalize_paths(context_shard.get("declared_read_scope")) or _normalized_job_read_scope(source_job)
    resolved_read_scope = _normalize_paths(context_shard.get("resolved_read_scope"))
    test_scope = _normalize_paths(context_shard.get("test_scope"))
    blast_radius = _normalize_paths(context_shard.get("blast_radius"))
    context_sections = (
        json.loads(json.dumps(context_shard.get("context_sections"), default=str))
        if isinstance(context_shard.get("context_sections"), list)
        else []
    )
    snapshot = _json_loads_maybe(_workflow_run_envelope(run_row).get("spec_snapshot"), {}) or {}
    execution_manifest = _execution_manifest_for_snapshot(
        conn,
        raw_snapshot=snapshot,
        workflow_id=str(_workflow_run_envelope(run_row).get("workflow_id") or "").strip() or None,
    )
    repo_policy_contract = repo_policy_runtime_payload(
        get_repo_policy_contract(conn, repo_root=workflow_root())
    )
    require_manifest_authority = _requires_reviewed_execution_manifest(snapshot)
    if require_manifest_authority and not isinstance(execution_manifest, dict):
        raise RuntimeError(
            "builder-originated workflow execution requires ExecutionManifest authority; "
            "runtime prompt fallback is not permitted",
        )
    runtime_profile_ref = str(
        _workflow_run_envelope(run_row).get("runtime_profile_ref")
        or snapshot.get("runtime_profile_ref")
        or ""
    ).strip()
    sandbox_profile = _runtime_profile_sandbox_payload(
        conn,
        runtime_profile_ref=runtime_profile_ref or None,
    )
    snapshot_jobs = snapshot.get("jobs") if isinstance(snapshot.get("jobs"), list) else []
    downstream_labels: list[str] = []
    current_label = str(source_job.get("label") or job.get("label") or "").strip()
    for child_job in snapshot_jobs:
        if not isinstance(child_job, dict):
            continue
        child_label = str(child_job.get("label") or "").strip()
        if not child_label or child_label == current_label:
            continue
        depends_on = _normalize_paths(child_job.get("depends_on"))
        if current_label and current_label in depends_on:
            downstream_labels.append(child_label)
    verify_refs = list(
        dict.fromkeys(
            [
                *_job_verify_refs(source_job),
                *_normalize_string_list(snapshot.get("verify_refs")),
            ]
        )
    )
    explicit_authority_domains = explicit_authority_domains_for_job(
        job=source_job,
        spec_snapshot=snapshot,
    )
    decision_pack = resolve_job_decision_pack(
        conn,
        write_scope=write_scope,
        declared_read_scope=declared_read_scope,
        resolved_read_scope=resolved_read_scope,
        blast_radius=blast_radius,
        explicit_authority_domains=explicit_authority_domains,
    )
    return build_execution_bundle(
        run_id=str(run_row.get("run_id") or "").strip() or None,
        workflow_id=str(_workflow_run_envelope(run_row).get("workflow_id") or "").strip() or None,
        sandbox_profile_ref=(
            None
            if not isinstance(sandbox_profile, dict)
            else str(sandbox_profile.get("sandbox_profile_ref") or "").strip() or None
        ),
        sandbox_profile=sandbox_profile,
        job_label=str(source_job.get("label") or job.get("label") or "").strip() or "job",
        prompt=str(source_job.get("prompt") or job.get("prompt") or ""),
        task_type=str(source_job.get("task_type") or job.get("task_type") or job.get("route_task_type") or "").strip() or None,
        capabilities=_normalize_paths(source_job.get("capabilities")),
        allowed_tools=_normalize_paths(source_job.get("allowed_tools")),
        explicit_mcp_tools=_normalize_paths(source_job.get("mcp_tools")),
        explicit_skill_refs=_normalize_paths(source_job.get("skill_refs")),
        write_scope=write_scope,
        declared_read_scope=declared_read_scope,
        resolved_read_scope=resolved_read_scope,
        blast_radius=blast_radius,
        test_scope=test_scope,
        verify_refs=verify_refs,
        approval_required=source_job.get("approval_required")
        if isinstance(source_job.get("approval_required"), bool)
        else None,
        approval_question=str(source_job.get("approval_question") or "").strip() or None,
        context_sections=context_sections,
        submission_required=source_job.get("submission_required")
        if isinstance(source_job.get("submission_required"), bool)
        else (
            bool(source_job["completion_contract"].get("submission_required"))
            if isinstance(source_job.get("completion_contract"), dict)
            and isinstance(source_job["completion_contract"].get("submission_required"), bool)
            else None
        ),
        result_kind=(
            str(source_job["completion_contract"].get("result_kind") or "").strip() or None
            if isinstance(source_job.get("completion_contract"), dict)
            else None
        ),
        submit_tool_names=(
            [
                str(name).strip()
                for name in source_job["completion_contract"].get("submit_tool_names") or []
                if str(name).strip()
            ]
            if isinstance(source_job.get("completion_contract"), dict)
            and isinstance(source_job["completion_contract"].get("submit_tool_names"), list)
            else None
        ),
        verification_required=(
            bool(source_job["completion_contract"].get("verification_required"))
            if isinstance(source_job.get("completion_contract"), dict)
            and isinstance(source_job["completion_contract"].get("verification_required"), bool)
            else None
        ),
        downstream_labels=downstream_labels,
        output_schema=source_job.get("output_schema")
        if isinstance(source_job.get("output_schema"), dict)
        else None,
        authoring_contract=source_job.get("authoring_contract")
        if isinstance(source_job.get("authoring_contract"), dict)
        else None,
        acceptance_contract=source_job.get("acceptance_contract")
        if isinstance(source_job.get("acceptance_contract"), dict)
        else None,
        decision_pack=decision_pack,
        execution_manifest=execution_manifest,
        repo_policy_contract=repo_policy_contract,
        require_manifest_authority=require_manifest_authority,
    )


def _persist_runtime_context_for_job(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    workflow_id: str | None,
    job_label: str,
    execution_context_shard: dict[str, object] | None,
    execution_bundle: dict[str, object] | None,
) -> None:
    from runtime.workflow.job_runtime_context import persist_workflow_job_runtime_contexts

    normalized_label = str(job_label or "").strip()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not normalized_label:
        return
    persist_workflow_job_runtime_contexts(
        conn,
        run_id=normalized_run_id,
        workflow_id=str(workflow_id or "").strip() or None,
        execution_context_shards={normalized_label: dict(execution_context_shard or {})},
        execution_bundles={normalized_label: dict(execution_bundle or {})},
    )


def _submission_completion_contract(
    execution_bundle: dict[str, object] | None,
) -> dict[str, object]:
    if not isinstance(execution_bundle, dict):
        return {}
    value = execution_bundle.get("completion_contract")
    return dict(value) if isinstance(value, dict) else {}


def _submission_required_for_bundle(
    execution_bundle: dict[str, object] | None,
) -> bool:
    contract = _submission_completion_contract(execution_bundle)
    return bool(contract.get("submission_required"))


def _capture_submission_baseline_if_required(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    workflow_id: str | None,
    job_label: str,
    repo_root: str,
    execution_context_shard: dict[str, object] | None,
    execution_bundle: dict[str, object] | None,
) -> dict[str, object] | None:
    if not _submission_required_for_bundle(execution_bundle):
        return None
    shard = execution_context_shard if isinstance(execution_context_shard, dict) else {}
    bundle_access_policy = (
        execution_bundle.get("access_policy")
        if isinstance(execution_bundle, dict)
        and isinstance(execution_bundle.get("access_policy"), dict)
        else {}
    )
    write_scope = _normalize_paths(shard.get("write_scope")) or _normalize_paths(
        bundle_access_policy.get("write_scope") if isinstance(bundle_access_policy, dict) else []
    )
    return _submission_capture_baseline_for_job(
        conn,
        run_id=run_id,
        workflow_id=workflow_id,
        job_label=job_label,
        workspace_root=repo_root,
        write_scope=write_scope,
        execution_context_shard=execution_context_shard or {},
        execution_bundle=execution_bundle or {},
    )


def _verification_artifact_refs(
    verification_bindings: list[dict[str, object]] | None,
) -> list[str]:
    refs: list[str] = []
    for binding in verification_bindings or []:
        if not isinstance(binding, dict):
            continue
        for key in ("artifact_ref", "artifact_id", "verification_ref", "verify_ref", "ref"):
            value = str(binding.get(key) or "").strip()
            if value:
                refs.append(value)
    return list(dict.fromkeys(refs))


def _ensure_execution_packet_revisions(
    *,
    raw_snapshot: dict,
    spec,
    workflow_id: str,
    run_id: str,
) -> tuple[str, str]:
    """Return (definition_revision, plan_revision), synthesizing deterministic
    fallbacks when the raw snapshot is missing them.

    BUG-D384AB69: queue-spec and chain-submitted runs don't carry
    definition_revision / plan_revision in the raw snapshot (those are
    graph-runtime fields). Rather than silently skipping execution packet
    persistence — which loses diagnostic context for every non-graph run —
    synthesize deterministic fallbacks from the spec itself. Graph runs
    still win because their explicit values are used as-is.

    Side effect: stamps the resolved revisions onto ``raw_snapshot`` so
    downstream reads stay consistent.
    """
    resolved = resolve_execution_packet_revisions(
        raw_snapshot=raw_snapshot,
        spec=spec,
        workflow_id=workflow_id,
        run_id=run_id,
    )
    if resolved["provenance_kind"] != "compiled":
        logger.info(
            "Resolved execution-packet revisions for run %s via %s authority: definition=%s plan=%s",
            run_id,
            resolved["provenance_kind"],
            resolved["definition_revision"],
            resolved["plan_revision"],
        )
    return str(resolved["definition_revision"]), str(resolved["plan_revision"])


def _build_execution_packet(
    *,
    conn: SyncPostgresConnection,
    spec,
    raw_snapshot: dict,
    run_id: str,
    workflow_id: str,
    authority: dict[str, str],
    parent_run_id: str | None,
    trigger_depth: int,
    provenance: dict[str, object] | None,
) -> dict[str, object] | None:
    from runtime.idempotency import canonical_hash

    definition_revision, plan_revision = _ensure_execution_packet_revisions(
        raw_snapshot=raw_snapshot,
        spec=spec,
        workflow_id=workflow_id,
        run_id=run_id,
    )

    provenance = dict(provenance or {})
    spec_file_inputs = provenance.get("file_inputs") if isinstance(provenance.get("file_inputs"), dict) else {}
    spec_authority_inputs = provenance.get("authority_inputs") if isinstance(provenance.get("authority_inputs"), dict) else {}
    execution_context_shards = _build_job_execution_context_shards(
        conn=conn,
        spec=spec,
        raw_snapshot=raw_snapshot,
        provenance=provenance,
    )
    execution_bundles = _build_job_execution_bundles(
        conn=conn,
        spec=spec,
        raw_snapshot=raw_snapshot,
        execution_context_shards=execution_context_shards,
        run_id=run_id,
        workflow_id=workflow_id,
        runtime_profile_ref=str(raw_snapshot.get("runtime_profile_ref") or "").strip() or None,
    )

    model_messages: list[dict[str, object]] = []
    reference_bindings: list[dict[str, object]] = []
    capability_bindings: list[dict[str, object]] = []
    verify_refs: list[str] = []

    for index, job in enumerate(spec.jobs):
        label = str(job.get("label") or f"job_{index}")
        agent_slug = str(job.get("agent") or "unknown")
        route_plan = job.get("_route_plan")
        route_task_type = str(getattr(route_plan, "task_type", "") or job.get("task_type") or "").strip()
        route_origin_slug = str(getattr(route_plan, "original_slug", "") or "").strip()
        if route_plan and not route_task_type:
            route_task_type = str(getattr(route_plan, "task_type", "") or "").strip()
        verify_refs.extend(_job_verify_refs(job))

        job_payload = dict(job)
        context_shard = execution_context_shards.get(label)
        if context_shard:
            job_payload["_execution_context"] = context_shard
        execution_bundle = execution_bundles.get(label)
        if execution_bundle:
            job_payload["_execution_bundle"] = execution_bundle

        model_messages.append(
            {
                "job_label": label,
                "agent_slug": agent_slug,
                "messages": _execution_model_messages(job_payload),
            }
        )
        reference_bindings.append(
            {
                "job_label": label,
                "agent_slug": agent_slug,
                "depends_on": _normalize_paths(job.get("depends_on")),
                "prompt_hash": str(job.get("prompt_hash") or hashlib.sha256(str(job.get("prompt") or "").encode("utf-8")).hexdigest()[:16]),
                "route_task_type": route_task_type,
                "route_origin_slug": route_origin_slug,
                "route_candidates": _normalize_paths(job.get("route_candidates")),
                "integration_id": job.get("integration_id"),
                "integration_action": job.get("integration_action"),
            }
        )
        capability_bindings.append(
            {
                "job_label": label,
                "agent_slug": agent_slug,
                "route_task_type": route_task_type,
                "capabilities": _normalize_paths(job.get("capabilities")),
                "allowed_tools": _normalize_paths((execution_bundle or {}).get("allowed_tools")),
                "mcp_tools": _normalize_paths((execution_bundle or {}).get("mcp_tool_names")),
                "skill_refs": _normalize_paths((execution_bundle or {}).get("skill_refs")),
                "tool_bucket": str((execution_bundle or {}).get("tool_bucket") or "").strip(),
                "route_candidates": _normalize_paths(job.get("route_candidates")),
            }
        )

    spec_verify_refs = _normalize_string_list(raw_snapshot.get("verify_refs"))
    verify_refs.extend(spec_verify_refs)
    verify_refs = list(dict.fromkeys(verify_refs))

    file_inputs = {
        "repo_root": provenance.get("repo_root"),
        "spec_path": provenance.get("spec_path"),
        "context_files": provenance.get("context_files"),
        "write_scope": provenance.get("write_scope") or raw_snapshot.get("write_scope"),
        "read_scope": provenance.get("read_scope") or raw_snapshot.get("read_scope"),
        "execution_context_shards": execution_context_shards,
        "execution_bundles": execution_bundles,
        "output_dir": raw_snapshot.get("output_dir"),
        "spec_file_inputs": spec_file_inputs,
    }
    authority_inputs = {
        "authority": authority,
        "workflow_definition": provenance.get("definition_row"),
        "workflow_plan": provenance.get("compiled_spec_row"),
        "workflow_row": provenance.get("workflow_row"),
        "spec_snapshot": raw_snapshot,
        "parent_run_id": parent_run_id,
        "trigger_depth": trigger_depth,
        "source_authority": spec_authority_inputs,
    }
    json_file_inputs = json.loads(json.dumps(file_inputs, default=str))
    json_authority_inputs = json.loads(json.dumps(authority_inputs, default=str))
    lineage_file_inputs = json.loads(json.dumps(json_file_inputs, default=str))
    execution_bundles_for_lineage = lineage_file_inputs.get("execution_bundles")
    if isinstance(execution_bundles_for_lineage, dict):
        for bundle in execution_bundles_for_lineage.values():
            if isinstance(bundle, dict):
                bundle.pop("run_id", None)
    packet_payload: dict[str, object] = {
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "packet_revision_authority": json.loads(
            json.dumps(raw_snapshot.get("packet_revision_authority") or {}, default=str)
        ),
        "packet_version": 1,
        "workflow_id": workflow_id,
        "run_id": run_id,
        "spec_name": str(spec.name),
        "source_kind": str(provenance.get("source_kind") or "workflow_submit"),
        "authority_refs": [definition_revision, plan_revision],
        "model_messages": json.loads(json.dumps(model_messages, default=str)),
        "reference_bindings": json.loads(json.dumps(reference_bindings, default=str)),
        "capability_bindings": json.loads(json.dumps(capability_bindings, default=str)),
        "verify_refs": json.loads(json.dumps(verify_refs, default=str)),
        "authority_inputs": json_authority_inputs,
        "file_inputs": json_file_inputs,
        "compile_provenance": {
            "artifact_kind": "packet_lineage",
            "input_fingerprint": "",
            "surface_revision": "workflow_runtime.packet_submit",
            "definition_revision": definition_revision,
            "plan_revision": plan_revision,
            "workflow_id": workflow_id,
            "spec_name": str(spec.name),
            "source_kind": str(provenance.get("source_kind") or "workflow_submit"),
            "file_inputs": lineage_file_inputs,
            "authority_inputs": json.loads(
                json.dumps(
                    {
                        "workflow_definition": provenance.get("definition_row"),
                        "workflow_plan": provenance.get("compiled_spec_row"),
                        "workflow_row": _workflow_row_reuse_authority(provenance.get("workflow_row")),
                        "source_authority": provenance.get("authority_inputs")
                        if isinstance(provenance.get("authority_inputs"), dict)
                        else {},
                    },
                    default=str,
                )
            ),
        },
    }
    compile_provenance = dict(packet_payload["compile_provenance"])
    compile_input_payload = {
        "artifact_kind": compile_provenance["artifact_kind"],
        "surface_revision": compile_provenance["surface_revision"],
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "workflow_id": workflow_id,
        "spec_name": str(spec.name),
        "source_kind": packet_payload["source_kind"],
        "model_messages": packet_payload["model_messages"],
        "reference_bindings": packet_payload["reference_bindings"],
        "capability_bindings": packet_payload["capability_bindings"],
        "verify_refs": packet_payload["verify_refs"],
        "file_inputs": compile_provenance["file_inputs"],
        "authority_inputs": compile_provenance["authority_inputs"],
    }
    compile_provenance["input_fingerprint"] = canonical_hash(compile_input_payload)
    packet_payload["compile_provenance"] = compile_provenance
    artifact_store = CompileArtifactStore(conn)
    try:
        return artifact_store.persist_execution_packet_with_reuse(
            packet=packet_payload,
            authority_refs=[definition_revision, plan_revision],
            parent_artifact_ref=plan_revision,
        )
    except CompileArtifactError as exc:
        raise RuntimeError(f"workflow packet lineage reuse failed closed: {exc}") from exc


def _workflow_row_reuse_authority(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    allowed = {}
    for field_name in ("id", "name"):
        if field_name in value:
            allowed[field_name] = value[field_name]
    return allowed


# ---------------------------------------------------------------------------
# Failure-related helpers (extracted from other locations in unified.py)
# ---------------------------------------------------------------------------

def _terminal_failure_classification(
    *,
    error_code: str,
    stderr: str = "",
    exit_code: int | None = None,
):
    """Classify terminal failure metadata once for the completion path."""
    normalized_error_code = str(error_code or "").strip()
    normalized_stderr = str(stderr or "")
    if not normalized_error_code and not normalized_stderr:
        return None

    try:
        from runtime.failure_classifier import classify_failure, classify_failure_from_stderr
    except Exception:
        return None

    if normalized_error_code:
        try:
            classification = classify_failure(
                normalized_error_code,
                outputs={"stderr": normalized_stderr, "exit_code": exit_code},
            )
            category = getattr(getattr(classification, "category", None), "value", "")
            if category != "unknown" or not normalized_stderr:
                return classification
        except Exception:
            pass
    if normalized_stderr:
        try:
            return classify_failure_from_stderr(normalized_stderr, exit_code=exit_code)
        except Exception:
            pass
    return None


def _extract_verification_paths(bindings: list[dict] | None) -> list[str]:
    """Compatibility wrapper for verification path extraction."""
    return _verification_runtime_extract_verification_paths(bindings)
