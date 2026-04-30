"""Gateway command for the trusted preflight pass on a code-change candidate.

Preflight is the runtime-authority view that reviewers (human or LLM) read
instead of the agent-shaped submission payload. It recomputes the patch from
the real base head, runs the temp verifier, scans for runtime-derived
authority impacts, and validates them against the agent-declared impact
contract. The result is one row in `candidate_preflight_records` plus zero
or more `runtime_addition` rows in `candidate_authority_impacts`.

The `code_change_candidate.review` approve gate refuses to take effect
without a passed preflight whose `base_head_ref_at_preflight` still matches
the candidate's `base_head_ref`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.workflow.authority_overlap import (
    DiscoveredImpact,
    discover_authority_overlap,
    is_authority_bearing,
)
from runtime.workflow.candidate_materialization import (
    CandidateMaterializationError,
    _apply_projection_to_worktree,
    _candidate_row,
    _capture_patch_artifact,
    _json_object,
    _run_candidate_verifier,
    _verifier_inputs_for_worktree,
    _worktree_patch_projection,
)
from runtime.workspace_paths import repo_root as default_repo_root


class PreflightCodeChangeCandidate(BaseModel):
    """Input for `code_change_candidate.preflight`."""

    candidate_id: str = Field(..., min_length=1)
    triggered_by: str = Field(default="system:code_change_candidate.preflight", min_length=1)
    repo_root: str | None = None

    @field_validator("candidate_id", "triggered_by", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        return str(value or "").strip()

    @field_validator("repo_root", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


def _read_post_patch_files(worktree: Path, intended_files: list[str]) -> dict[str, str]:
    contents: dict[str, str] = {}
    for path in intended_files:
        try:
            file_path = worktree / path
            if file_path.is_file():
                contents[path] = file_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
    return contents


def _load_declared_impacts(conn: Any, candidate_id: str) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT impact_id::text          AS impact_id,
               intent::text             AS intent,
               unit_kind::text          AS unit_kind,
               unit_ref,
               predecessor_unit_kind::text AS predecessor_unit_kind,
               predecessor_unit_ref,
               dispatch_effect::text    AS dispatch_effect,
               subsumption_evidence_ref,
               rollback_path,
               discovery_source::text   AS discovery_source,
               validation_status::text  AS validation_status
          FROM candidate_authority_impacts
         WHERE candidate_id = $1::uuid
         ORDER BY created_at ASC
        """,
        candidate_id,
    )
    return [dict(row) for row in (rows or [])]


def _impact_match_key(impact: DiscoveredImpact | dict[str, Any]) -> tuple[str, str, str]:
    if isinstance(impact, DiscoveredImpact):
        return (impact.unit_kind, impact.unit_ref, impact.dispatch_effect)
    return (
        str(impact.get("unit_kind") or ""),
        str(impact.get("unit_ref") or ""),
        str(impact.get("dispatch_effect") or ""),
    )


def _validate_impacts(
    declared: list[dict[str, Any]],
    runtime: list[DiscoveredImpact],
    *,
    conn: Any,
    candidate_id: str,
) -> dict[str, Any]:
    declared_by_key: dict[tuple[str, str, str], dict[str, Any]] = {
        _impact_match_key(row): row for row in declared
    }
    runtime_by_key: dict[tuple[str, str, str], DiscoveredImpact] = {
        _impact_match_key(impact): impact for impact in runtime
    }

    validated_keys: list[tuple[str, str, str]] = []
    contested: list[dict[str, Any]] = []
    additions: list[DiscoveredImpact] = []
    findings: list[dict[str, Any]] = []

    for key, declared_row in declared_by_key.items():
        if key in runtime_by_key:
            validated_keys.append(key)
            findings.append(
                {
                    "result": "validated",
                    "unit_kind": key[0],
                    "unit_ref": key[1],
                    "dispatch_effect": key[2],
                    "agent_intent": declared_row.get("intent"),
                }
            )
        else:
            contested.append(declared_row)
            findings.append(
                {
                    "result": "contested",
                    "reason": "agent_declared_impact_not_found_by_runtime",
                    "unit_kind": key[0],
                    "unit_ref": key[1],
                    "dispatch_effect": key[2],
                    "agent_intent": declared_row.get("intent"),
                }
            )

    for key, runtime_impact in runtime_by_key.items():
        if key in declared_by_key:
            continue
        additions.append(runtime_impact)
        findings.append(
            {
                "result": "runtime_addition",
                "reason": runtime_impact.discovery_evidence.get("reason"),
                "unit_kind": key[0],
                "unit_ref": key[1],
                "dispatch_effect": key[2],
                "intent_hint": runtime_impact.intent_hint,
                "evidence": runtime_impact.discovery_evidence,
            }
        )

    if validated_keys:
        conn.execute(
            """
            UPDATE candidate_authority_impacts
               SET validation_status = 'validated',
                   updated_at = now(),
                   validation_evidence = COALESCE(validation_evidence, '{}'::jsonb)
                                          || jsonb_build_object('preflight_validated_at', now()::text)
             WHERE candidate_id = $1::uuid
               AND (unit_kind::text, unit_ref, dispatch_effect::text) IN (
                   SELECT unnest($2::text[]),
                          unnest($3::text[]),
                          unnest($4::text[])
               )
            """,
            candidate_id,
            [key[0] for key in validated_keys],
            [key[1] for key in validated_keys],
            [key[2] for key in validated_keys],
        )

    if contested:
        contested_ids = [row["impact_id"] for row in contested if row.get("impact_id")]
        if contested_ids:
            conn.execute(
                """
                UPDATE candidate_authority_impacts
                   SET validation_status = 'contested',
                       updated_at = now(),
                       validation_evidence = COALESCE(validation_evidence, '{}'::jsonb)
                                              || jsonb_build_object('preflight_contested_at', now()::text)
                 WHERE impact_id = ANY($1::uuid[])
                """,
                contested_ids,
            )

    for runtime_impact in additions:
        conn.execute(
            """
            INSERT INTO candidate_authority_impacts (
                candidate_id,
                intent,
                unit_kind,
                unit_ref,
                predecessor_unit_kind,
                predecessor_unit_ref,
                dispatch_effect,
                discovery_source,
                validation_status,
                validation_evidence,
                notes
            ) VALUES (
                $1::uuid,
                $2::candidate_authority_impact_intent,
                $3::candidate_authority_unit_kind,
                $4,
                $5::candidate_authority_unit_kind,
                $6,
                $7::candidate_authority_dispatch_effect,
                'runtime_derived',
                'runtime_addition',
                $8::jsonb,
                'inserted by code_change_candidate.preflight runtime overlap discovery'
            )
            """,
            candidate_id,
            runtime_impact.intent_hint,
            runtime_impact.unit_kind,
            runtime_impact.unit_ref,
            runtime_impact.predecessor_unit_kind,
            runtime_impact.predecessor_unit_ref,
            runtime_impact.dispatch_effect,
            json.dumps(runtime_impact.discovery_evidence, sort_keys=True, default=str),
        )

    return {
        "validated_count": len(validated_keys),
        "contested_count": len(contested),
        "addition_count": len(additions),
        "findings": findings,
    }


def _insert_preflight_record(
    conn: Any,
    *,
    candidate_id: str,
    base_head_ref: str,
    runtime_patch_sha: str | None,
    runtime_patch_artifact_ref: str | None,
    agent_patch_sha: str | None,
    patch_divergence: dict[str, Any],
    temp_verifier_run_id: str | None,
    temp_verifier_passed: bool | None,
    impact_contract_complete: bool,
    impact_findings: list[dict[str, Any]],
    runtime_count: int,
    declared_count: int,
    contested_count: int,
    addition_count: int,
    gate_findings: dict[str, Any],
    preflight_status: str,
    triggered_by: str,
) -> str:
    row = conn.fetchrow(
        """
        INSERT INTO candidate_preflight_records (
            candidate_id,
            preflight_status,
            base_head_ref_at_preflight,
            runtime_derived_patch_sha256,
            runtime_derived_patch_artifact_ref,
            agent_declared_patch_sha256,
            patch_divergence,
            temp_verifier_run_id,
            temp_verifier_passed,
            impact_contract_complete,
            impact_contract_findings,
            runtime_derived_impact_count,
            agent_declared_impact_count,
            contested_impact_count,
            runtime_addition_impact_count,
            gate_findings,
            created_by,
            completed_at
        ) VALUES (
            $1::uuid,
            $2::candidate_preflight_status,
            $3,
            $4,
            $5,
            $6,
            $7::jsonb,
            $8,
            $9,
            $10,
            $11::jsonb,
            $12,
            $13,
            $14,
            $15,
            $16::jsonb,
            $17,
            now()
        )
        RETURNING preflight_id::text AS preflight_id
        """,
        candidate_id,
        preflight_status,
        base_head_ref,
        runtime_patch_sha,
        runtime_patch_artifact_ref,
        agent_patch_sha,
        json.dumps(patch_divergence, sort_keys=True, default=str),
        temp_verifier_run_id,
        temp_verifier_passed,
        impact_contract_complete,
        json.dumps(impact_findings, sort_keys=True, default=str),
        runtime_count,
        declared_count,
        contested_count,
        addition_count,
        json.dumps(gate_findings, sort_keys=True, default=str),
        triggered_by,
    )
    return str(row["preflight_id"])


def handle_preflight_candidate(
    command: PreflightCodeChangeCandidate,
    subsystems: Any,
) -> dict[str, Any]:
    """Run trusted preflight for one code-change candidate."""

    conn = subsystems.get_pg_conn()
    repo_root = Path(command.repo_root).resolve() if command.repo_root else default_repo_root().resolve()

    try:
        candidate = _candidate_row(conn, candidate_id=command.candidate_id)
    except CandidateMaterializationError as exc:
        return {
            "ok": False,
            "reason_code": exc.reason_code,
            "error": str(exc),
            "details": exc.details,
        }

    intended_files = [str(path) for path in (candidate.get("intended_files") or [])]
    agent_patch_sha = candidate.get("patch_sha256")
    base_head_ref = str(candidate["base_head_ref"])

    declared_impacts = _load_declared_impacts(conn, command.candidate_id)
    requires_contract = is_authority_bearing(intended_files)

    if requires_contract and not declared_impacts:
        preflight_id = _insert_preflight_record(
            conn,
            candidate_id=command.candidate_id,
            base_head_ref=base_head_ref,
            runtime_patch_sha=None,
            runtime_patch_artifact_ref=None,
            agent_patch_sha=agent_patch_sha,
            patch_divergence={},
            temp_verifier_run_id=None,
            temp_verifier_passed=None,
            impact_contract_complete=False,
            impact_findings=[
                {
                    "result": "blocked",
                    "reason": "authority_bearing_candidate_missing_impact_contract",
                    "intended_files": intended_files,
                }
            ],
            runtime_count=0,
            declared_count=0,
            contested_count=0,
            addition_count=0,
            gate_findings={"reason": "missing_impact_contract"},
            preflight_status="failed_impact_contract",
            triggered_by=command.triggered_by,
        )
        return {
            "ok": False,
            "reason_code": "code_change_candidate.preflight_blocked_missing_impact_contract",
            "preflight_id": preflight_id,
            "candidate_id": command.candidate_id,
            "event_payload": {
                "candidate_id": command.candidate_id,
                "preflight_id": preflight_id,
                "preflight_status": "failed_impact_contract",
                "runtime_derived_patch_sha256": None,
                "temp_verifier_passed": None,
                "impact_contract_complete": False,
                "runtime_derived_impact_count": 0,
                "agent_declared_impact_count": 0,
                "contested_impact_count": 0,
                "runtime_addition_impact_count": 0,
            },
        }

    try:
        projection, temp_path, temp = _worktree_patch_projection(
            repo_root=repo_root,
            base_head_ref=base_head_ref,
            candidate=candidate,
        )
    except CandidateMaterializationError as exc:
        preflight_id = _insert_preflight_record(
            conn,
            candidate_id=command.candidate_id,
            base_head_ref=base_head_ref,
            runtime_patch_sha=None,
            runtime_patch_artifact_ref=None,
            agent_patch_sha=agent_patch_sha,
            patch_divergence={"reason": exc.reason_code, "details": exc.details},
            temp_verifier_run_id=None,
            temp_verifier_passed=None,
            impact_contract_complete=False,
            impact_findings=[],
            runtime_count=0,
            declared_count=len(declared_impacts),
            contested_count=0,
            addition_count=0,
            gate_findings={"reason": exc.reason_code, "details": exc.details},
            preflight_status="failed_patch_divergence",
            triggered_by=command.triggered_by,
        )
        return {
            "ok": False,
            "reason_code": exc.reason_code,
            "preflight_id": preflight_id,
            "details": exc.details,
        }

    runtime_patch_sha = projection.patch_sha256
    runtime_patch_artifact_ref: str | None = None
    temp_verifier_run_id: str | None = None
    temp_verifier_passed: bool | None = None
    runtime_impacts: list[DiscoveredImpact] = []
    impact_summary: dict[str, Any] = {
        "validated_count": 0,
        "contested_count": 0,
        "addition_count": 0,
        "findings": [],
    }
    failure_reason: str | None = None
    failure_details: dict[str, Any] = {}

    try:
        try:
            _apply_projection_to_worktree(temp_path, projection)
            runtime_patch_artifact_ref = _capture_patch_artifact(
                conn,
                candidate_id=command.candidate_id,
                projection=projection,
            )
        except Exception as exc:  # noqa: BLE001
            failure_reason = "code_change_candidate.preflight_apply_failed"
            failure_details = {"error": str(exc)}

        if failure_reason is None:
            file_contents = _read_post_patch_files(temp_path, intended_files)
            runtime_impacts = discover_authority_overlap(
                intended_files=intended_files,
                file_contents=file_contents,
            )
            impact_summary = _validate_impacts(
                declared_impacts,
                runtime_impacts,
                conn=conn,
                candidate_id=command.candidate_id,
            )

            try:
                temp_inputs = _verifier_inputs_for_worktree(
                    _json_object(candidate.get("verifier_inputs")),
                    repo_root=repo_root,
                    worktree_root=temp_path,
                )
                temp_verification = _run_candidate_verifier(
                    conn=conn,
                    candidate=candidate,
                    inputs=temp_inputs,
                    fallback_ref=str(command.candidate_id),
                )
                temp_verifier_run_id = str(temp_verification.get("verification_run_id") or "") or None
                temp_verifier_passed = bool(temp_verification.get("ok"))
            except Exception as exc:  # noqa: BLE001
                failure_reason = "code_change_candidate.preflight_temp_verifier_error"
                failure_details = {"error": str(exc)}
    finally:
        try:
            temp.cleanup()
        except Exception:  # noqa: BLE001
            pass

    patch_divergence: dict[str, Any] = {}
    if agent_patch_sha and runtime_patch_sha and agent_patch_sha != runtime_patch_sha:
        patch_divergence = {
            "agent_declared_patch_sha256": agent_patch_sha,
            "runtime_derived_patch_sha256": runtime_patch_sha,
            "reason": "agent_patch_sha_diverges_from_runtime_recompute",
        }

    contested_count = int(impact_summary["contested_count"])
    addition_count = int(impact_summary["addition_count"])
    validated_count = int(impact_summary["validated_count"])
    runtime_count = len(runtime_impacts)
    declared_count = len(declared_impacts)
    impact_contract_complete = (
        contested_count == 0
        and (not requires_contract or declared_count > 0 or runtime_count == 0)
    )

    if failure_reason == "code_change_candidate.preflight_apply_failed":
        preflight_status = "failed_patch_divergence"
    elif patch_divergence:
        preflight_status = "failed_patch_divergence"
        failure_reason = failure_reason or "code_change_candidate.preflight_patch_divergence"
    elif failure_reason == "code_change_candidate.preflight_temp_verifier_error" or (
        temp_verifier_passed is False
    ):
        preflight_status = "failed_temp_verifier"
        failure_reason = failure_reason or "code_change_candidate.preflight_temp_verifier_failed"
    elif not impact_contract_complete:
        preflight_status = "failed_impact_contract"
        failure_reason = "code_change_candidate.preflight_impact_contract_incomplete"
    else:
        preflight_status = "passed"

    gate_findings: dict[str, Any] = {
        "validated_count": validated_count,
        "contested_count": contested_count,
        "addition_count": addition_count,
        "runtime_count": runtime_count,
        "declared_count": declared_count,
        "patch_divergence": bool(patch_divergence),
        "temp_verifier_passed": temp_verifier_passed,
        "requires_contract": requires_contract,
    }
    if failure_reason:
        gate_findings["failure_reason"] = failure_reason
        if failure_details:
            gate_findings["failure_details"] = failure_details

    preflight_id = _insert_preflight_record(
        conn,
        candidate_id=command.candidate_id,
        base_head_ref=base_head_ref,
        runtime_patch_sha=runtime_patch_sha,
        runtime_patch_artifact_ref=runtime_patch_artifact_ref,
        agent_patch_sha=agent_patch_sha,
        patch_divergence=patch_divergence,
        temp_verifier_run_id=temp_verifier_run_id,
        temp_verifier_passed=temp_verifier_passed,
        impact_contract_complete=impact_contract_complete,
        impact_findings=impact_summary["findings"],
        runtime_count=runtime_count,
        declared_count=declared_count,
        contested_count=contested_count,
        addition_count=addition_count,
        gate_findings=gate_findings,
        preflight_status=preflight_status,
        triggered_by=command.triggered_by,
    )

    return {
        "ok": preflight_status == "passed",
        "preflight_id": preflight_id,
        "candidate_id": command.candidate_id,
        "preflight_status": preflight_status,
        "reason_code": failure_reason,
        "runtime_derived_patch_sha256": runtime_patch_sha,
        "agent_declared_patch_sha256": agent_patch_sha,
        "patch_divergence": patch_divergence,
        "temp_verifier_run_id": temp_verifier_run_id,
        "temp_verifier_passed": temp_verifier_passed,
        "impact_contract_complete": impact_contract_complete,
        "impact_summary": impact_summary,
        "event_payload": {
            "candidate_id": command.candidate_id,
            "preflight_id": preflight_id,
            "preflight_status": preflight_status,
            "runtime_derived_patch_sha256": runtime_patch_sha,
            "temp_verifier_passed": temp_verifier_passed,
            "impact_contract_complete": impact_contract_complete,
            "runtime_derived_impact_count": runtime_count,
            "agent_declared_impact_count": declared_count,
            "contested_impact_count": contested_count,
            "runtime_addition_impact_count": addition_count,
        },
    }


__all__ = [
    "PreflightCodeChangeCandidate",
    "handle_preflight_candidate",
]
