"""Postgres persistence for Virtual Lab state authority."""

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
            "virtual_lab_state.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key.endswith("_ids")):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_optional_row(row: Any, *, operation: str) -> dict[str, Any] | None:
    if row is None:
        return None
    return _normalize_row(row, operation=operation)


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def _timestamp(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "virtual_lab_state.invalid_timestamp",
                f"{field_name} must be an ISO timestamp",
                details={"field_name": field_name, "value": value},
            ) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise PostgresWriteError(
        "virtual_lab_state.invalid_timestamp",
        f"{field_name} must be an ISO timestamp",
        details={"field_name": field_name, "value": value},
    )


def _optional_clean_text(value: object, *, field_name: str) -> str | None:
    if value is None or value == "":
        return None
    return _optional_text(value, field_name=field_name)


def _list_payloads(value: object, *, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise PostgresWriteError(
            "virtual_lab_state.invalid_payload",
            f"{field_name} must be a list of JSON objects",
            details={"field_name": field_name},
        )
    return [dict(item) for item in value]


def persist_virtual_lab_state_packet(
    conn: Any,
    *,
    environment_revision: dict[str, Any],
    object_states: list[dict[str, Any]] | None = None,
    events: list[dict[str, Any]] | None = None,
    command_receipts: list[dict[str, Any]] | None = None,
    typed_gaps: list[dict[str, Any]] | None = None,
    event_chain_digest: str | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    revision = dict(_require_mapping(environment_revision, field_name="environment_revision"))
    state_records = _list_payloads(object_states, field_name="object_states")
    event_records = _list_payloads(events, field_name="events")
    receipt_records = _list_payloads(command_receipts, field_name="command_receipts")
    gap_records = _list_payloads(typed_gaps, field_name="typed_gaps")

    environment_id = _require_text(revision.get("environment_id"), field_name="environment_revision.environment_id")
    revision_id = _require_text(revision.get("revision_id"), field_name="environment_revision.revision_id")
    revision_digest = _require_text(revision.get("revision_digest"), field_name="environment_revision.revision_digest")
    seed_manifest = dict(_require_mapping(revision.get("seed_manifest"), field_name="environment_revision.seed_manifest"))
    seed_entries = _list_payloads(seed_manifest.get("entries"), field_name="environment_revision.seed_manifest.entries")

    revision_row = conn.fetchrow(
        """
        INSERT INTO virtual_lab_environment_revisions (
            environment_id,
            revision_id,
            parent_revision_id,
            revision_reason,
            status,
            seed_digest,
            config_digest,
            policy_digest,
            revision_digest,
            created_at_source,
            created_by,
            seed_manifest_json,
            revision_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12::jsonb, $13::jsonb, $14, $15
        )
        ON CONFLICT (environment_id, revision_id) DO UPDATE SET
            parent_revision_id = EXCLUDED.parent_revision_id,
            revision_reason = EXCLUDED.revision_reason,
            status = EXCLUDED.status,
            seed_digest = EXCLUDED.seed_digest,
            config_digest = EXCLUDED.config_digest,
            policy_digest = EXCLUDED.policy_digest,
            revision_digest = EXCLUDED.revision_digest,
            created_at_source = EXCLUDED.created_at_source,
            created_by = EXCLUDED.created_by,
            seed_manifest_json = EXCLUDED.seed_manifest_json,
            revision_json = EXCLUDED.revision_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref
        RETURNING *
        """,
        environment_id,
        revision_id,
        _optional_clean_text(revision.get("parent_revision_id"), field_name="environment_revision.parent_revision_id"),
        _require_text(revision.get("revision_reason"), field_name="environment_revision.revision_reason"),
        _require_text(revision.get("status"), field_name="environment_revision.status"),
        _require_text(revision.get("seed_digest"), field_name="environment_revision.seed_digest"),
        _require_text(revision.get("config_digest"), field_name="environment_revision.config_digest"),
        _require_text(revision.get("policy_digest"), field_name="environment_revision.policy_digest"),
        revision_digest,
        _timestamp(revision.get("created_at"), field_name="environment_revision.created_at"),
        _require_text(revision.get("created_by"), field_name="environment_revision.created_by"),
        _encode_jsonb(seed_manifest, field_name="seed_manifest"),
        _encode_jsonb(revision, field_name="environment_revision"),
        _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_clean_text(source_ref, field_name="source_ref"),
    )

    head_row = conn.fetchrow(
        """
        INSERT INTO virtual_lab_environment_heads (
            environment_id,
            current_revision_id,
            current_revision_digest,
            status,
            seed_digest,
            object_state_count,
            event_count,
            receipt_count,
            typed_gap_count,
            event_chain_digest,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (environment_id) DO UPDATE SET
            current_revision_id = EXCLUDED.current_revision_id,
            current_revision_digest = EXCLUDED.current_revision_digest,
            status = EXCLUDED.status,
            seed_digest = EXCLUDED.seed_digest,
            object_state_count = EXCLUDED.object_state_count,
            event_count = EXCLUDED.event_count,
            receipt_count = EXCLUDED.receipt_count,
            typed_gap_count = EXCLUDED.typed_gap_count,
            event_chain_digest = EXCLUDED.event_chain_digest,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        environment_id,
        revision_id,
        revision_digest,
        _require_text(revision.get("status"), field_name="environment_revision.status"),
        _require_text(revision.get("seed_digest"), field_name="environment_revision.seed_digest"),
        len(state_records),
        len(event_records),
        len(receipt_records),
        len(gap_records),
        _optional_clean_text(event_chain_digest, field_name="event_chain_digest"),
        _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_clean_text(source_ref, field_name="source_ref"),
    )

    conn.execute(
        "DELETE FROM virtual_lab_seed_entries WHERE environment_id = $1 AND revision_id = $2",
        environment_id,
        revision_id,
    )
    if seed_entries:
        conn.execute_many(
            """
            INSERT INTO virtual_lab_seed_entries (
                environment_id,
                revision_id,
                object_id,
                instance_id,
                object_truth_ref,
                object_truth_version,
                projection_version,
                base_state_digest,
                seed_digest,
                seed_entry_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            ON CONFLICT (environment_id, revision_id, object_id, instance_id) DO UPDATE SET
                object_truth_ref = EXCLUDED.object_truth_ref,
                object_truth_version = EXCLUDED.object_truth_version,
                projection_version = EXCLUDED.projection_version,
                base_state_digest = EXCLUDED.base_state_digest,
                seed_digest = EXCLUDED.seed_digest,
                seed_entry_json = EXCLUDED.seed_entry_json
            """,
            [
                (
                    environment_id,
                    revision_id,
                    _require_text(entry.get("object_id"), field_name="seed_entry.object_id"),
                    _require_text(entry.get("instance_id"), field_name="seed_entry.instance_id"),
                    _require_text(entry.get("object_truth_ref"), field_name="seed_entry.object_truth_ref"),
                    _require_text(entry.get("object_truth_version"), field_name="seed_entry.object_truth_version"),
                    _require_text(entry.get("projection_version"), field_name="seed_entry.projection_version"),
                    _require_text(entry.get("base_state_digest"), field_name="seed_entry.base_state_digest"),
                    _require_text(entry.get("seed_digest"), field_name="seed_entry.seed_digest"),
                    _encode_jsonb(entry, field_name="seed_entry"),
                )
                for entry in seed_entries
            ],
        )

    for state in state_records:
        conn.fetchrow(
            """
            INSERT INTO virtual_lab_object_states (
                environment_id,
                revision_id,
                object_id,
                instance_id,
                stream_id,
                source_ref_json,
                base_state_digest,
                overlay_state_digest,
                effective_state_digest,
                state_digest,
                last_event_id,
                tombstone,
                state_json,
                observed_by_ref,
                source_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10,
                $11, $12, $13::jsonb, $14, $15
            )
            ON CONFLICT (environment_id, revision_id, object_id, instance_id) DO UPDATE SET
                stream_id = EXCLUDED.stream_id,
                source_ref_json = EXCLUDED.source_ref_json,
                base_state_digest = EXCLUDED.base_state_digest,
                overlay_state_digest = EXCLUDED.overlay_state_digest,
                effective_state_digest = EXCLUDED.effective_state_digest,
                state_digest = EXCLUDED.state_digest,
                last_event_id = EXCLUDED.last_event_id,
                tombstone = EXCLUDED.tombstone,
                state_json = EXCLUDED.state_json,
                observed_by_ref = EXCLUDED.observed_by_ref,
                source_ref = EXCLUDED.source_ref,
                updated_at = now()
            RETURNING *
            """,
            environment_id,
            revision_id,
            _require_text(state.get("object_id"), field_name="object_state.object_id"),
            _require_text(state.get("instance_id"), field_name="object_state.instance_id"),
            _require_text(state.get("stream_id"), field_name="object_state.stream_id"),
            _encode_jsonb(state.get("source_ref") or {}, field_name="object_state.source_ref"),
            _require_text(state.get("base_state_digest"), field_name="object_state.base_state_digest"),
            _require_text(state.get("overlay_state_digest"), field_name="object_state.overlay_state_digest"),
            _require_text(state.get("effective_state_digest"), field_name="object_state.effective_state_digest"),
            _require_text(state.get("state_digest"), field_name="object_state.state_digest"),
            _optional_clean_text(state.get("last_event_id"), field_name="object_state.last_event_id"),
            bool(state.get("tombstone")),
            _encode_jsonb(state, field_name="object_state"),
            _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )

    for event in event_records:
        conn.fetchrow(
            """
            INSERT INTO virtual_lab_events (
                event_id,
                environment_id,
                revision_id,
                stream_id,
                event_type,
                event_version,
                occurred_at,
                recorded_at,
                actor_id,
                actor_type,
                command_id,
                causation_id,
                correlation_id,
                parent_event_ids_json,
                sequence_number,
                pre_state_digest,
                post_state_digest,
                payload_digest,
                schema_digest,
                event_json,
                source_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14::jsonb, $15, $16, $17, $18, $19, $20::jsonb, $21
            )
            ON CONFLICT (event_id) DO UPDATE SET
                event_json = EXCLUDED.event_json,
                source_ref = EXCLUDED.source_ref
            RETURNING *
            """,
            _require_text(event.get("event_id"), field_name="event.event_id"),
            environment_id,
            revision_id,
            _require_text(event.get("stream_id"), field_name="event.stream_id"),
            _require_text(event.get("event_type"), field_name="event.event_type"),
            int(event.get("event_version") or 1),
            _timestamp(event.get("occurred_at"), field_name="event.occurred_at"),
            _timestamp(event.get("recorded_at"), field_name="event.recorded_at"),
            _require_text(event.get("actor_id"), field_name="event.actor_id"),
            _require_text(event.get("actor_type"), field_name="event.actor_type"),
            _require_text(event.get("command_id"), field_name="event.command_id"),
            _optional_clean_text(event.get("causation_id"), field_name="event.causation_id"),
            _optional_clean_text(event.get("correlation_id"), field_name="event.correlation_id"),
            _encode_jsonb(event.get("parent_event_ids") or [], field_name="event.parent_event_ids"),
            int(event.get("sequence_number") or 0),
            _require_text(event.get("pre_state_digest"), field_name="event.pre_state_digest"),
            _require_text(event.get("post_state_digest"), field_name="event.post_state_digest"),
            _require_text(event.get("payload_digest"), field_name="event.payload_digest"),
            _require_text(event.get("schema_digest"), field_name="event.schema_digest"),
            _encode_jsonb(event, field_name="event"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )

    for receipt in receipt_records:
        conn.fetchrow(
            """
            INSERT INTO virtual_lab_command_receipts (
                receipt_id,
                command_id,
                environment_id,
                revision_id,
                status,
                resulting_event_ids_json,
                precondition_digest,
                result_digest,
                errors_json,
                warnings_json,
                issued_at,
                issued_by,
                receipt_json,
                source_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9::jsonb, $10::jsonb,
                $11, $12, $13::jsonb, $14
            )
            ON CONFLICT (receipt_id) DO UPDATE SET
                status = EXCLUDED.status,
                resulting_event_ids_json = EXCLUDED.resulting_event_ids_json,
                precondition_digest = EXCLUDED.precondition_digest,
                result_digest = EXCLUDED.result_digest,
                errors_json = EXCLUDED.errors_json,
                warnings_json = EXCLUDED.warnings_json,
                issued_at = EXCLUDED.issued_at,
                issued_by = EXCLUDED.issued_by,
                receipt_json = EXCLUDED.receipt_json,
                source_ref = EXCLUDED.source_ref
            RETURNING *
            """,
            _require_text(receipt.get("receipt_id"), field_name="receipt.receipt_id"),
            _require_text(receipt.get("command_id"), field_name="receipt.command_id"),
            environment_id,
            revision_id,
            _require_text(receipt.get("status"), field_name="receipt.status"),
            _encode_jsonb(receipt.get("resulting_event_ids") or [], field_name="receipt.resulting_event_ids"),
            _optional_clean_text(receipt.get("precondition_digest"), field_name="receipt.precondition_digest"),
            _optional_clean_text(receipt.get("result_digest"), field_name="receipt.result_digest"),
            _encode_jsonb(receipt.get("errors") or [], field_name="receipt.errors"),
            _encode_jsonb(receipt.get("warnings") or [], field_name="receipt.warnings"),
            _timestamp(receipt.get("issued_at"), field_name="receipt.issued_at"),
            _require_text(receipt.get("issued_by"), field_name="receipt.issued_by"),
            _encode_jsonb(receipt, field_name="receipt"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )

    conn.execute(
        "DELETE FROM virtual_lab_typed_gaps WHERE environment_id = $1 AND revision_id = $2",
        environment_id,
        revision_id,
    )
    if gap_records:
        conn.execute_many(
            """
            INSERT INTO virtual_lab_typed_gaps (
                environment_id,
                revision_id,
                gap_id,
                gap_kind,
                severity,
                related_ref,
                disposition,
                gap_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (environment_id, revision_id, gap_id) DO UPDATE SET
                gap_kind = EXCLUDED.gap_kind,
                severity = EXCLUDED.severity,
                related_ref = EXCLUDED.related_ref,
                disposition = EXCLUDED.disposition,
                gap_json = EXCLUDED.gap_json
            """,
            [
                (
                    environment_id,
                    revision_id,
                    _require_text(gap.get("gap_id"), field_name="typed_gap.gap_id"),
                    _require_text(gap.get("gap_kind"), field_name="typed_gap.gap_kind"),
                    _require_text(gap.get("severity"), field_name="typed_gap.severity"),
                    _require_text(gap.get("related_ref"), field_name="typed_gap.related_ref"),
                    _require_text(gap.get("disposition") or "open", field_name="typed_gap.disposition"),
                    _encode_jsonb(gap, field_name="typed_gap"),
                )
                for gap in gap_records
            ],
        )

    return {
        "environment_head": _normalize_row(head_row, operation="persist_virtual_lab_state.head"),
        "environment_revision": _normalize_row(revision_row, operation="persist_virtual_lab_state.revision"),
        "environment_id": environment_id,
        "revision_id": revision_id,
        "revision_digest": revision_digest,
        "seed_entry_count": len(seed_entries),
        "object_state_count": len(state_records),
        "event_count": len(event_records),
        "receipt_count": len(receipt_records),
        "typed_gap_count": len(gap_records),
        "event_chain_digest": event_chain_digest,
    }


def list_virtual_lab_environments(
    conn: Any,
    *,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM virtual_lab_environment_heads
         WHERE ($1::text IS NULL OR status = $1)
         ORDER BY updated_at DESC, created_at DESC
         LIMIT $2
        """,
        _optional_clean_text(status, field_name="status"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_virtual_lab_environments")


def list_virtual_lab_revisions(
    conn: Any,
    *,
    environment_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM virtual_lab_environment_revisions
         WHERE ($1::text IS NULL OR environment_id = $1)
           AND ($2::text IS NULL OR status = $2)
         ORDER BY created_at_source DESC, created_at DESC
         LIMIT $3
        """,
        _optional_clean_text(environment_id, field_name="environment_id"),
        _optional_clean_text(status, field_name="status"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_virtual_lab_revisions")


def load_virtual_lab_revision(
    conn: Any,
    *,
    environment_id: str,
    revision_id: str,
    include_seed: bool = True,
    include_objects: bool = True,
    include_events: bool = True,
    include_receipts: bool = True,
    include_typed_gaps: bool = True,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
          FROM virtual_lab_environment_revisions
         WHERE environment_id = $1
           AND revision_id = $2
        """,
        _require_text(environment_id, field_name="environment_id"),
        _require_text(revision_id, field_name="revision_id"),
    )
    revision = _normalize_optional_row(row, operation="load_virtual_lab_revision.revision")
    if revision is None:
        return None
    if include_seed:
        revision["seed_entries"] = _normalize_rows(
            conn.fetch(
                """
                SELECT *
                  FROM virtual_lab_seed_entries
                 WHERE environment_id = $1
                   AND revision_id = $2
                 ORDER BY object_id, instance_id
                """,
                environment_id,
                revision_id,
            ),
            operation="load_virtual_lab_revision.seed_entries",
        )
    if include_objects:
        revision["object_states"] = _normalize_rows(
            conn.fetch(
                """
                SELECT *
                  FROM virtual_lab_object_states
                 WHERE environment_id = $1
                   AND revision_id = $2
                 ORDER BY object_id, instance_id
                """,
                environment_id,
                revision_id,
            ),
            operation="load_virtual_lab_revision.object_states",
        )
    if include_events:
        revision["events"] = list_virtual_lab_events(
            conn,
            environment_id=environment_id,
            revision_id=revision_id,
            limit=500,
        )
    if include_receipts:
        revision["command_receipts"] = list_virtual_lab_command_receipts(
            conn,
            environment_id=environment_id,
            revision_id=revision_id,
            limit=500,
        )
    if include_typed_gaps:
        revision["typed_gaps"] = _normalize_rows(
            conn.fetch(
                """
                SELECT *
                  FROM virtual_lab_typed_gaps
                 WHERE environment_id = $1
                   AND revision_id = $2
                 ORDER BY severity, gap_kind, gap_id
                """,
                environment_id,
                revision_id,
            ),
            operation="load_virtual_lab_revision.typed_gaps",
        )
    return revision


def list_virtual_lab_events(
    conn: Any,
    *,
    environment_id: str,
    revision_id: str,
    stream_id: str | None = None,
    event_type: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM virtual_lab_events
         WHERE environment_id = $1
           AND revision_id = $2
           AND ($3::text IS NULL OR stream_id = $3)
           AND ($4::text IS NULL OR event_type = $4)
         ORDER BY stream_id, sequence_number, recorded_at
         LIMIT $5
        """,
        _require_text(environment_id, field_name="environment_id"),
        _require_text(revision_id, field_name="revision_id"),
        _optional_clean_text(stream_id, field_name="stream_id"),
        _optional_clean_text(event_type, field_name="event_type"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_virtual_lab_events")


def list_virtual_lab_command_receipts(
    conn: Any,
    *,
    environment_id: str,
    revision_id: str,
    status: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM virtual_lab_command_receipts
         WHERE environment_id = $1
           AND revision_id = $2
           AND ($3::text IS NULL OR status = $3)
         ORDER BY issued_at DESC, receipt_id
         LIMIT $4
        """,
        _require_text(environment_id, field_name="environment_id"),
        _require_text(revision_id, field_name="revision_id"),
        _optional_clean_text(status, field_name="status"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_virtual_lab_command_receipts")


__all__ = [
    "list_virtual_lab_command_receipts",
    "list_virtual_lab_environments",
    "list_virtual_lab_events",
    "list_virtual_lab_revisions",
    "load_virtual_lab_revision",
    "persist_virtual_lab_state_packet",
]
