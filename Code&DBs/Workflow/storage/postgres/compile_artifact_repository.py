"""Explicit sync Postgres repository for compile-artifact persistence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_positive_int,
    _require_text,
)


def _require_text_sequence(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a sequence of non-empty strings",
            details={"field": field_name},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_require_text(item, field_name=f"{field_name}[{index}]"))
    return tuple(normalized)


def _require_mapping_sequence(
    value: object,
    *,
    field_name: str,
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a sequence of mappings",
            details={"field": field_name},
        )
    normalized: list[Mapping[str, Any]] = []
    for index, item in enumerate(value):
        normalized.append(_require_mapping(item, field_name=f"{field_name}[{index}]"))
    return tuple(normalized)


class PostgresCompileArtifactRepository:
    """Owns canonical compile-artifact and execution-packet mutations."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def upsert_compile_artifact(
        self,
        *,
        compile_artifact_id: str,
        artifact_kind: str,
        artifact_ref: str,
        revision_ref: str,
        parent_artifact_ref: str | None,
        input_fingerprint: str,
        content_hash: str,
        authority_refs: Sequence[str],
        payload: Mapping[str, Any],
        decision_ref: str,
    ) -> str:
        normalized_compile_artifact_id = _require_text(
            compile_artifact_id,
            field_name="compile_artifact_id",
        )
        self._conn.execute(
            """
            INSERT INTO compile_artifacts (
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10
            )
            ON CONFLICT (artifact_kind, revision_ref) DO UPDATE SET
                artifact_ref = EXCLUDED.artifact_ref,
                parent_artifact_ref = EXCLUDED.parent_artifact_ref,
                input_fingerprint = EXCLUDED.input_fingerprint,
                content_hash = EXCLUDED.content_hash,
                authority_refs = EXCLUDED.authority_refs,
                payload = EXCLUDED.payload,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            normalized_compile_artifact_id,
            _require_text(artifact_kind, field_name="artifact_kind"),
            _require_text(artifact_ref, field_name="artifact_ref"),
            _require_text(revision_ref, field_name="revision_ref"),
            _optional_text(parent_artifact_ref, field_name="parent_artifact_ref"),
            _require_text(input_fingerprint, field_name="input_fingerprint"),
            _require_text(content_hash, field_name="content_hash"),
            _encode_jsonb(
                list(_require_text_sequence(authority_refs, field_name="authority_refs")),
                field_name="authority_refs",
            ),
            _encode_jsonb(
                dict(_require_mapping(payload, field_name="payload")),
                field_name="payload",
            ),
            _require_text(decision_ref, field_name="decision_ref"),
        )
        return normalized_compile_artifact_id

    def load_compile_artifacts_for_input(
        self,
        *,
        artifact_kind: str,
        input_fingerprint: str,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref
            FROM compile_artifacts
            WHERE artifact_kind = $1
              AND input_fingerprint = $2
            ORDER BY created_at ASC, compile_artifact_id ASC
            """,
            _require_text(artifact_kind, field_name="artifact_kind"),
            _require_text(input_fingerprint, field_name="input_fingerprint"),
        )
        return [dict(row) for row in rows or ()]

    def upsert_execution_packet(
        self,
        *,
        execution_packet_id: str,
        definition_revision: str,
        plan_revision: str,
        packet_revision: str,
        parent_artifact_ref: str | None,
        packet_version: int,
        packet_hash: str,
        workflow_id: str,
        run_id: str,
        spec_name: str,
        source_kind: str,
        authority_refs: Sequence[str],
        model_messages: Sequence[Mapping[str, Any]],
        reference_bindings: Sequence[Mapping[str, Any]],
        capability_bindings: Sequence[Mapping[str, Any]],
        verify_refs: Sequence[str],
        authority_inputs: Mapping[str, Any],
        file_inputs: Mapping[str, Any],
        payload: Mapping[str, Any],
        decision_ref: str,
    ) -> str:
        normalized_execution_packet_id = _require_text(
            execution_packet_id,
            field_name="execution_packet_id",
        )
        self._conn.execute(
            """
            INSERT INTO execution_packets (
                execution_packet_id,
                definition_revision,
                plan_revision,
                packet_revision,
                parent_artifact_ref,
                packet_version,
                packet_hash,
                workflow_id,
                run_id,
                spec_name,
                source_kind,
                authority_refs,
                model_messages,
                reference_bindings,
                capability_bindings,
                verify_refs,
                authority_inputs,
                file_inputs,
                payload,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb,
                $17::jsonb, $18::jsonb, $19::jsonb, $20
            )
            ON CONFLICT (definition_revision, plan_revision, packet_revision) DO UPDATE SET
                parent_artifact_ref = EXCLUDED.parent_artifact_ref,
                packet_version = EXCLUDED.packet_version,
                packet_hash = EXCLUDED.packet_hash,
                workflow_id = EXCLUDED.workflow_id,
                run_id = EXCLUDED.run_id,
                spec_name = EXCLUDED.spec_name,
                source_kind = EXCLUDED.source_kind,
                authority_refs = EXCLUDED.authority_refs,
                model_messages = EXCLUDED.model_messages,
                reference_bindings = EXCLUDED.reference_bindings,
                capability_bindings = EXCLUDED.capability_bindings,
                verify_refs = EXCLUDED.verify_refs,
                authority_inputs = EXCLUDED.authority_inputs,
                file_inputs = EXCLUDED.file_inputs,
                payload = EXCLUDED.payload,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            normalized_execution_packet_id,
            _require_text(definition_revision, field_name="definition_revision"),
            _require_text(plan_revision, field_name="plan_revision"),
            _require_text(packet_revision, field_name="packet_revision"),
            _optional_text(parent_artifact_ref, field_name="parent_artifact_ref"),
            _require_positive_int(packet_version, field_name="packet_version"),
            _require_text(packet_hash, field_name="packet_hash"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(run_id, field_name="run_id"),
            _require_text(spec_name, field_name="spec_name"),
            _require_text(source_kind, field_name="source_kind"),
            _encode_jsonb(
                list(_require_text_sequence(authority_refs, field_name="authority_refs")),
                field_name="authority_refs",
            ),
            _encode_jsonb(
                list(_require_mapping_sequence(model_messages, field_name="model_messages")),
                field_name="model_messages",
            ),
            _encode_jsonb(
                list(
                    _require_mapping_sequence(
                        reference_bindings,
                        field_name="reference_bindings",
                    )
                ),
                field_name="reference_bindings",
            ),
            _encode_jsonb(
                list(
                    _require_mapping_sequence(
                        capability_bindings,
                        field_name="capability_bindings",
                    )
                ),
                field_name="capability_bindings",
            ),
            _encode_jsonb(
                list(_require_text_sequence(verify_refs, field_name="verify_refs")),
                field_name="verify_refs",
            ),
            _encode_jsonb(
                dict(_require_mapping(authority_inputs, field_name="authority_inputs")),
                field_name="authority_inputs",
            ),
            _encode_jsonb(
                dict(_require_mapping(file_inputs, field_name="file_inputs")),
                field_name="file_inputs",
            ),
            _encode_jsonb(
                dict(_require_mapping(payload, field_name="payload")),
                field_name="payload",
            ),
            _require_text(decision_ref, field_name="decision_ref"),
        )
        return normalized_execution_packet_id

    def load_execution_packets_for_run(
        self,
        *,
        run_id: str,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT
                execution_packet_id,
                definition_revision,
                plan_revision,
                packet_revision,
                parent_artifact_ref,
                packet_version,
                packet_hash,
                workflow_id,
                run_id,
                spec_name,
                source_kind,
                authority_refs,
                model_messages,
                reference_bindings,
                capability_bindings,
                verify_refs,
                authority_inputs,
                file_inputs,
                payload,
                decision_ref
            FROM execution_packets
            WHERE run_id = $1
            ORDER BY created_at ASC, execution_packet_id ASC
            """,
            _require_text(run_id, field_name="run_id"),
        )
        return [dict(row) for row in rows or ()]

    def load_execution_packets_for_revision(
        self,
        *,
        packet_revision: str,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT
                execution_packet_id,
                definition_revision,
                plan_revision,
                packet_revision,
                parent_artifact_ref,
                packet_version,
                packet_hash,
                workflow_id,
                run_id,
                spec_name,
                source_kind,
                authority_refs,
                model_messages,
                reference_bindings,
                capability_bindings,
                verify_refs,
                authority_inputs,
                file_inputs,
                payload,
                decision_ref
            FROM execution_packets
            WHERE packet_revision = $1
            ORDER BY created_at DESC, execution_packet_id DESC
            """,
            _require_text(packet_revision, field_name="packet_revision"),
        )
        return [dict(row) for row in rows or ()]

    def load_compile_artifact_by_revision(
        self,
        *,
        artifact_kind: str,
        revision_ref: str,
    ) -> dict[str, Any] | None:
        rows = self._conn.execute(
            """
            SELECT
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref,
                created_at
            FROM compile_artifacts
            WHERE artifact_kind = $1
              AND revision_ref = $2
            ORDER BY created_at DESC, compile_artifact_id DESC
            LIMIT 1
            """,
            _require_text(artifact_kind, field_name="artifact_kind"),
            _require_text(revision_ref, field_name="revision_ref"),
        )
        rows = list(rows or ())
        return dict(rows[0]) if rows else None

    def load_compile_artifact_history(
        self,
        *,
        artifact_kind: str,
        artifact_ref: str | None = None,
        input_fingerprint: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = ["artifact_kind = $1"]
        params: list[Any] = [_require_text(artifact_kind, field_name="artifact_kind")]
        if artifact_ref is not None:
            params.append(_require_text(artifact_ref, field_name="artifact_ref"))
            clauses.append(f"artifact_ref = ${len(params)}")
        if input_fingerprint is not None:
            params.append(_require_text(input_fingerprint, field_name="input_fingerprint"))
            clauses.append(f"input_fingerprint = ${len(params)}")
        params.append(_require_positive_int(limit, field_name="limit"))
        rows = self._conn.execute(
            f"""
            SELECT
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref,
                created_at
            FROM compile_artifacts
            WHERE {" AND ".join(clauses)}
            ORDER BY created_at DESC, compile_artifact_id DESC
            LIMIT ${len(params)}
            """,
            *params,
        )
        return [dict(row) for row in rows or ()]

    def load_compile_artifact_lineage(
        self,
        *,
        artifact_kind: str,
        revision_ref: str,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            WITH RECURSIVE lineage AS (
                SELECT
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref,
                created_at
            FROM compile_artifacts
            WHERE artifact_kind = $1
              AND revision_ref = $2
            UNION ALL
            SELECT
                parent.compile_artifact_id,
                parent.artifact_kind,
                parent.artifact_ref,
                parent.revision_ref,
                parent.parent_artifact_ref,
                parent.input_fingerprint,
                parent.content_hash,
                parent.authority_refs,
                parent.payload,
                parent.decision_ref,
                parent.created_at
            FROM compile_artifacts parent
            JOIN lineage child
              ON parent.artifact_kind = child.artifact_kind
             AND parent.artifact_ref = child.parent_artifact_ref
            )
            SELECT
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref,
                created_at
            FROM lineage
            ORDER BY created_at ASC, compile_artifact_id ASC
            """,
            _require_text(artifact_kind, field_name="artifact_kind"),
            _require_text(revision_ref, field_name="revision_ref"),
        )
        return [dict(row) for row in rows or ()]
