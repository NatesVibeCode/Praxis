"""DB-native persistence for workflow build planning artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "workflow_build_planning.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and key.endswith("_json"):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_timestamp(value: object | None, *, field_name: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise PostgresWriteError(
            "workflow_build_planning.invalid_input",
            f"{field_name} must be a datetime when provided",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise PostgresWriteError(
            "workflow_build_planning.invalid_input",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _string_list(value: object | None, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PostgresWriteError(
            "workflow_build_planning.invalid_input",
            f"{field_name} must be a list",
            details={"field": field_name},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise PostgresWriteError(
                "workflow_build_planning.invalid_input",
                f"{field_name}[{index}] must be a string",
                details={"field": f"{field_name}[{index}]"},
            )
        text = item.strip()
        if text:
            normalized.append(text)
    return normalized


def upsert_workflow_build_intent(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
    source_mode: str,
    goal: str,
    desired_outcome: str,
    constraints: list[str] | None = None,
    success_criteria: list[str] | None = None,
    referenced_entities: list[str] | None = None,
    uncertainty_markers: list[str] | None = None,
    bootstrap_state: dict[str, Any] | None = None,
    intent_ref: str | None = None,
) -> dict[str, Any]:
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_definition_revision = _require_text(
        definition_revision,
        field_name="definition_revision",
    )
    normalized_source_mode = _require_text(source_mode, field_name="source_mode")
    normalized_goal = _require_text(goal, field_name="goal")
    normalized_desired_outcome = _require_text(
        desired_outcome,
        field_name="desired_outcome",
    )
    normalized_intent_ref = _optional_text(intent_ref, field_name="intent_ref") or (
        f"intent:{normalized_workflow_id}:{normalized_definition_revision}"
    )
    normalized_constraints = _string_list(constraints, field_name="constraints")
    normalized_success_criteria = _string_list(
        success_criteria,
        field_name="success_criteria",
    )
    normalized_referenced_entities = _string_list(
        referenced_entities,
        field_name="referenced_entities",
    )
    normalized_uncertainty_markers = _string_list(
        uncertainty_markers,
        field_name="uncertainty_markers",
    )
    normalized_bootstrap_state = dict(
        _require_mapping(
            bootstrap_state or {},
            field_name="bootstrap_state",
        )
    )

    row = conn.fetchrow(
        """
        INSERT INTO workflow_build_intents (
            intent_ref,
            workflow_id,
            definition_revision,
            source_mode,
            goal,
            desired_outcome,
            constraints_json,
            success_criteria_json,
            referenced_entities_json,
            uncertainty_markers_json,
            bootstrap_state_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb
        )
        ON CONFLICT (workflow_id, definition_revision) DO UPDATE SET
            source_mode = EXCLUDED.source_mode,
            goal = EXCLUDED.goal,
            desired_outcome = EXCLUDED.desired_outcome,
            constraints_json = EXCLUDED.constraints_json,
            success_criteria_json = EXCLUDED.success_criteria_json,
            referenced_entities_json = EXCLUDED.referenced_entities_json,
            uncertainty_markers_json = EXCLUDED.uncertainty_markers_json,
            bootstrap_state_json = EXCLUDED.bootstrap_state_json,
            updated_at = now()
        RETURNING *
        """,
        normalized_intent_ref,
        normalized_workflow_id,
        normalized_definition_revision,
        normalized_source_mode,
        normalized_goal,
        normalized_desired_outcome,
        _encode_jsonb(normalized_constraints, field_name="constraints"),
        _encode_jsonb(normalized_success_criteria, field_name="success_criteria"),
        _encode_jsonb(normalized_referenced_entities, field_name="referenced_entities"),
        _encode_jsonb(normalized_uncertainty_markers, field_name="uncertainty_markers"),
        _encode_jsonb(normalized_bootstrap_state, field_name="bootstrap_state"),
    )
    return _normalize_row(row, operation="upsert_workflow_build_intent")


def load_workflow_build_intent(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
        FROM workflow_build_intents
        WHERE workflow_id = $1
          AND definition_revision = $2
        """,
        _require_text(workflow_id, field_name="workflow_id"),
        _require_text(definition_revision, field_name="definition_revision"),
    )
    return None if row is None else _normalize_row(row, operation="load_workflow_build_intent")


def replace_workflow_build_candidate_manifest(
    conn: Any,
    *,
    manifest_ref: str,
    workflow_id: str,
    definition_revision: str,
    manifest_revision: str,
    intent_ref: str,
    review_group_ref: str,
    execution_readiness: str,
    projection_status: dict[str, Any] | None,
    blocking_issues: list[dict[str, Any]] | None,
    required_confirmations: list[dict[str, Any]] | None,
    slots: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_manifest_ref = _require_text(manifest_ref, field_name="manifest_ref")
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_definition_revision = _require_text(
        definition_revision,
        field_name="definition_revision",
    )
    normalized_manifest_revision = _require_text(
        manifest_revision,
        field_name="manifest_revision",
    )
    normalized_intent_ref = _require_text(intent_ref, field_name="intent_ref")
    normalized_review_group_ref = _require_text(
        review_group_ref,
        field_name="review_group_ref",
    )
    normalized_execution_readiness = _require_text(
        execution_readiness,
        field_name="execution_readiness",
    )
    normalized_projection_status = dict(
        _require_mapping(projection_status or {}, field_name="projection_status")
    )
    normalized_blocking_issues = list(blocking_issues or [])
    normalized_required_confirmations = list(required_confirmations or [])

    row = conn.fetchrow(
        """
        INSERT INTO workflow_build_candidate_manifests (
            manifest_ref,
            workflow_id,
            definition_revision,
            manifest_revision,
            intent_ref,
            review_group_ref,
            execution_readiness,
            projection_status_json,
            blocking_issues_json,
            required_confirmations_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb
        )
        ON CONFLICT (manifest_ref) DO UPDATE SET
            workflow_id = EXCLUDED.workflow_id,
            definition_revision = EXCLUDED.definition_revision,
            manifest_revision = EXCLUDED.manifest_revision,
            intent_ref = EXCLUDED.intent_ref,
            review_group_ref = EXCLUDED.review_group_ref,
            execution_readiness = EXCLUDED.execution_readiness,
            projection_status_json = EXCLUDED.projection_status_json,
            blocking_issues_json = EXCLUDED.blocking_issues_json,
            required_confirmations_json = EXCLUDED.required_confirmations_json,
            updated_at = now()
        RETURNING *
        """,
        normalized_manifest_ref,
        normalized_workflow_id,
        normalized_definition_revision,
        normalized_manifest_revision,
        normalized_intent_ref,
        normalized_review_group_ref,
        normalized_execution_readiness,
        _encode_jsonb(normalized_projection_status, field_name="projection_status"),
        _encode_jsonb(normalized_blocking_issues, field_name="blocking_issues"),
        _encode_jsonb(normalized_required_confirmations, field_name="required_confirmations"),
    )

    conn.execute(
        "DELETE FROM workflow_build_candidates WHERE manifest_ref = $1",
        normalized_manifest_ref,
    )
    conn.execute(
        "DELETE FROM workflow_build_candidate_slots WHERE manifest_ref = $1",
        normalized_manifest_ref,
    )
    for slot in slots:
        mapping = dict(_require_mapping(slot, field_name="slot"))
        conn.execute(
            """
            INSERT INTO workflow_build_candidate_slots (
                manifest_ref,
                slot_ref,
                workflow_id,
                definition_revision,
                manifest_revision,
                slot_kind,
                required,
                candidate_resolution_state,
                approval_state,
                source_binding_ref,
                source_evidence_ref,
                top_ranked_ref,
                approved_ref,
                resolution_rationale,
                slot_metadata_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb
            )
            """,
            normalized_manifest_ref,
            _require_text(mapping.get("slot_ref"), field_name="slot_ref"),
            normalized_workflow_id,
            normalized_definition_revision,
            normalized_manifest_revision,
            _require_text(mapping.get("slot_kind"), field_name="slot_kind"),
            bool(mapping.get("required", True)),
            _require_text(
                mapping.get("candidate_resolution_state"),
                field_name="candidate_resolution_state",
            ),
            _require_text(mapping.get("approval_state"), field_name="approval_state"),
            _optional_text(mapping.get("source_binding_ref"), field_name="source_binding_ref"),
            _optional_text(mapping.get("source_evidence_ref"), field_name="source_evidence_ref"),
            _optional_text(mapping.get("top_ranked_ref"), field_name="top_ranked_ref"),
            _optional_text(mapping.get("approved_ref"), field_name="approved_ref"),
            _optional_text(mapping.get("resolution_rationale"), field_name="resolution_rationale"),
            _encode_jsonb(
                dict(_require_mapping(mapping.get("slot_metadata") or {}, field_name="slot_metadata")),
                field_name="slot_metadata",
            ),
        )
    for candidate in candidates:
        mapping = dict(_require_mapping(candidate, field_name="candidate"))
        conn.execute(
            """
            INSERT INTO workflow_build_candidates (
                manifest_ref,
                slot_ref,
                candidate_ref,
                workflow_id,
                definition_revision,
                manifest_revision,
                target_kind,
                target_ref,
                rank,
                fit_score,
                confidence,
                source_def_ref,
                payload_json,
                candidate_approval_state,
                candidate_rationale
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $15
            )
            """,
            normalized_manifest_ref,
            _require_text(mapping.get("slot_ref"), field_name="slot_ref"),
            _require_text(mapping.get("candidate_ref"), field_name="candidate_ref"),
            normalized_workflow_id,
            normalized_definition_revision,
            normalized_manifest_revision,
            _require_text(mapping.get("target_kind"), field_name="target_kind"),
            _require_text(mapping.get("target_ref"), field_name="target_ref"),
            int(mapping.get("rank") or 1),
            mapping.get("fit_score"),
            mapping.get("confidence"),
            _optional_text(mapping.get("source_def_ref"), field_name="source_def_ref"),
            _encode_jsonb(
                dict(_require_mapping(mapping.get("payload") or {}, field_name="payload")),
                field_name="payload",
            ),
            _require_text(
                mapping.get("candidate_approval_state"),
                field_name="candidate_approval_state",
            ),
            _optional_text(mapping.get("candidate_rationale"), field_name="candidate_rationale"),
        )
    return _normalize_row(row, operation="replace_workflow_build_candidate_manifest")


def load_latest_workflow_build_candidate_manifest(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
        FROM workflow_build_candidate_manifests
        WHERE workflow_id = $1
          AND definition_revision = $2
        ORDER BY updated_at DESC, created_at DESC, manifest_ref DESC
        LIMIT 1
        """,
        _require_text(workflow_id, field_name="workflow_id"),
        _require_text(definition_revision, field_name="definition_revision"),
    )
    return None if row is None else _normalize_row(
        row,
        operation="load_latest_workflow_build_candidate_manifest",
    )


def upsert_workflow_build_review_session(
    conn: Any,
    *,
    review_group_ref: str,
    workflow_id: str,
    definition_revision: str,
    manifest_ref: str,
    review_policy_ref: str,
    status: str,
    closed_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_review_group_ref = _require_text(
        review_group_ref,
        field_name="review_group_ref",
    )
    row = conn.fetchrow(
        """
        INSERT INTO workflow_build_review_sessions (
            review_group_ref,
            workflow_id,
            definition_revision,
            manifest_ref,
            review_policy_ref,
            status,
            closed_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
        ON CONFLICT (review_group_ref) DO UPDATE SET
            manifest_ref = EXCLUDED.manifest_ref,
            review_policy_ref = EXCLUDED.review_policy_ref,
            status = EXCLUDED.status,
            closed_at = EXCLUDED.closed_at
        RETURNING *
        """,
        normalized_review_group_ref,
        _require_text(workflow_id, field_name="workflow_id"),
        _require_text(definition_revision, field_name="definition_revision"),
        _require_text(manifest_ref, field_name="manifest_ref"),
        _require_text(review_policy_ref, field_name="review_policy_ref"),
        _require_text(status, field_name="status"),
        _normalize_timestamp(closed_at, field_name="closed_at") if closed_at is not None else None,
    )
    return _normalize_row(row, operation="upsert_workflow_build_review_session")


def load_workflow_build_review_session(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
    review_group_ref: str | None = None,
) -> dict[str, Any] | None:
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_definition_revision = _require_text(
        definition_revision,
        field_name="definition_revision",
    )
    normalized_review_group_ref = _optional_text(
        review_group_ref,
        field_name="review_group_ref",
    )
    if normalized_review_group_ref:
        row = conn.fetchrow(
            """
            SELECT *
            FROM workflow_build_review_sessions
            WHERE workflow_id = $1
              AND definition_revision = $2
              AND review_group_ref = $3
            """,
            normalized_workflow_id,
            normalized_definition_revision,
            normalized_review_group_ref,
        )
    else:
        row = conn.fetchrow(
            """
            SELECT *
            FROM workflow_build_review_sessions
            WHERE workflow_id = $1
              AND definition_revision = $2
            ORDER BY opened_at DESC, review_group_ref DESC
            LIMIT 1
            """,
            normalized_workflow_id,
            normalized_definition_revision,
        )
    return None if row is None else _normalize_row(
        row,
        operation="load_workflow_build_review_session",
    )


def load_review_policy_definition(
    conn: Any,
    *,
    review_policy_ref: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
        FROM review_policy_definitions
        WHERE review_policy_ref = $1
        """,
        _require_text(review_policy_ref, field_name="review_policy_ref"),
    )
    return None if row is None else _normalize_row(row, operation="load_review_policy_definition")


def load_default_workflow_build_review_policy(conn: Any) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
        FROM review_policy_definitions
        WHERE policy_scope = 'workflow_build/default'
          AND status = 'active'
        ORDER BY review_policy_ref
        LIMIT 1
        """
    )
    return None if row is None else _normalize_row(
        row,
        operation="load_default_workflow_build_review_policy",
    )


def list_active_capability_bundle_definitions(conn: Any) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
        FROM capability_bundle_definitions
        WHERE status = 'active'
        ORDER BY family, bundle_ref
        """
    )
    return [_normalize_row(row, operation="list_active_capability_bundle_definitions") for row in rows or []]


def load_capability_bundle_definitions(
    conn: Any,
    *,
    bundle_refs: list[str],
) -> list[dict[str, Any]]:
    normalized_bundle_refs = _string_list(bundle_refs, field_name="bundle_refs")
    if not normalized_bundle_refs:
        return []
    rows = conn.fetch(
        """
        SELECT *
        FROM capability_bundle_definitions
        WHERE bundle_ref = ANY($1::text[])
        ORDER BY family, bundle_ref
        """,
        normalized_bundle_refs,
    )
    return [_normalize_row(row, operation="load_capability_bundle_definitions") for row in rows or []]


def list_active_workflow_shape_family_definitions(conn: Any) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
        FROM workflow_shape_family_definitions
        WHERE status = 'active'
        ORDER BY shape_family_ref
        """
    )
    return [_normalize_row(row, operation="list_active_workflow_shape_family_definitions") for row in rows or []]


def upsert_workflow_build_execution_manifest(
    conn: Any,
    *,
    execution_manifest_ref: str,
    workflow_id: str,
    definition_revision: str,
    manifest_ref: str,
    review_group_ref: str,
    compiled_spec: dict[str, Any],
    resolved_bindings: list[dict[str, Any]],
    approved_bundle_refs: list[str],
    tool_allowlist: dict[str, Any],
    verify_refs: list[str],
    policy_gates: dict[str, Any],
    hardening_report: dict[str, Any],
) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        INSERT INTO workflow_build_execution_manifests (
            execution_manifest_ref,
            workflow_id,
            definition_revision,
            manifest_ref,
            review_group_ref,
            compiled_spec_json,
            resolved_bindings_json,
            approved_bundle_refs_json,
            tool_allowlist_json,
            verify_refs_json,
            policy_gates_json,
            hardening_report_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb
        )
        ON CONFLICT (execution_manifest_ref) DO UPDATE SET
            compiled_spec_json = EXCLUDED.compiled_spec_json,
            resolved_bindings_json = EXCLUDED.resolved_bindings_json,
            approved_bundle_refs_json = EXCLUDED.approved_bundle_refs_json,
            tool_allowlist_json = EXCLUDED.tool_allowlist_json,
            verify_refs_json = EXCLUDED.verify_refs_json,
            policy_gates_json = EXCLUDED.policy_gates_json,
            hardening_report_json = EXCLUDED.hardening_report_json,
            updated_at = now()
        RETURNING *
        """,
        _require_text(execution_manifest_ref, field_name="execution_manifest_ref"),
        _require_text(workflow_id, field_name="workflow_id"),
        _require_text(definition_revision, field_name="definition_revision"),
        _require_text(manifest_ref, field_name="manifest_ref"),
        _require_text(review_group_ref, field_name="review_group_ref"),
        _encode_jsonb(dict(_require_mapping(compiled_spec, field_name="compiled_spec")), field_name="compiled_spec"),
        _encode_jsonb(list(resolved_bindings or []), field_name="resolved_bindings"),
        _encode_jsonb(_string_list(approved_bundle_refs, field_name="approved_bundle_refs"), field_name="approved_bundle_refs"),
        _encode_jsonb(dict(_require_mapping(tool_allowlist, field_name="tool_allowlist")), field_name="tool_allowlist"),
        _encode_jsonb(_string_list(verify_refs, field_name="verify_refs"), field_name="verify_refs"),
        _encode_jsonb(dict(_require_mapping(policy_gates, field_name="policy_gates")), field_name="policy_gates"),
        _encode_jsonb(dict(_require_mapping(hardening_report, field_name="hardening_report")), field_name="hardening_report"),
    )
    return _normalize_row(row, operation="upsert_workflow_build_execution_manifest")


def load_latest_workflow_build_execution_manifest(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
        FROM workflow_build_execution_manifests
        WHERE workflow_id = $1
          AND definition_revision = $2
        ORDER BY updated_at DESC, created_at DESC, execution_manifest_ref DESC
        LIMIT 1
        """,
        _require_text(workflow_id, field_name="workflow_id"),
        _require_text(definition_revision, field_name="definition_revision"),
    )
    return None if row is None else _normalize_row(
        row,
        operation="load_latest_workflow_build_execution_manifest",
    )


__all__ = [
    "list_active_capability_bundle_definitions",
    "list_active_workflow_shape_family_definitions",
    "load_capability_bundle_definitions",
    "load_default_workflow_build_review_policy",
    "load_latest_workflow_build_candidate_manifest",
    "load_latest_workflow_build_execution_manifest",
    "load_review_policy_definition",
    "load_workflow_build_intent",
    "load_workflow_build_review_session",
    "replace_workflow_build_candidate_manifest",
    "upsert_workflow_build_execution_manifest",
    "upsert_workflow_build_intent",
    "upsert_workflow_build_review_session",
]
