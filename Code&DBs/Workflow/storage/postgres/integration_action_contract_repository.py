"""Postgres persistence for integration action and automation contracts."""

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
            "integration_action_contract.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key in {"contract_json", "snapshot_json"}):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def _timestamp_or_none(value: Any, *, field_name: str) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "integration_action_contract.invalid_timestamp",
                f"{field_name} must be an ISO timestamp",
                details={"field_name": field_name, "value": value},
            ) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise PostgresWriteError(
        "integration_action_contract.invalid_timestamp",
        f"{field_name} must be an ISO timestamp",
        details={"field_name": field_name, "value": value},
    )


def _mapping_at(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return dict(value) if isinstance(value, dict) else {}


def _optional_clean_text(value: object, *, field_name: str) -> str | None:
    if value is None or value == "":
        return None
    return _optional_text(value, field_name=field_name)


def _system_ref(contract: dict[str, Any], side: str) -> str:
    systems = _mapping_at(contract, "systems")
    system = systems.get(side)
    if isinstance(system, dict):
        return _require_text(system.get("system_ref"), field_name=f"contract.systems.{side}.system_ref")
    raise PostgresWriteError(
        "integration_action_contract.invalid_contract",
        f"contract.systems.{side} must be a mapping",
        details={"field": f"contract.systems.{side}"},
    )


def _target_provider(contract: dict[str, Any]) -> str | None:
    systems = _mapping_at(contract, "systems")
    target = systems.get("target")
    if isinstance(target, dict):
        return _optional_clean_text(target.get("provider"), field_name="contract.systems.target.provider")
    return None


def _is_mutating(contract: dict[str, Any]) -> bool:
    side_effects = contract.get("side_effects") or []
    if not isinstance(side_effects, list):
        return False
    return any(isinstance(item, dict) and item.get("kind") != "none" for item in side_effects)


def _nested_value(payload: dict[str, Any], object_key: str, value_key: str) -> str | None:
    nested = _mapping_at(payload, object_key)
    return _optional_clean_text(nested.get(value_key), field_name=f"{object_key}.{value_key}")


def persist_integration_action_contract_inventory(
    conn: Any,
    *,
    contracts: list[dict[str, Any]],
    automation_snapshots: list[dict[str, Any]] | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    contract_records = [dict(_require_mapping(item, field_name="contract")) for item in contracts]
    snapshot_records = [
        dict(_require_mapping(item, field_name="automation_snapshot"))
        for item in (automation_snapshots or [])
    ]
    snapshot_links_by_action: dict[str, set[str]] = {}
    for snapshot in snapshot_records:
        rule_id = str(snapshot.get("rule_id") or "")
        for linked_action_id in snapshot.get("linked_action_ids") or []:
            if str(linked_action_id) and rule_id:
                snapshot_links_by_action.setdefault(str(linked_action_id), set()).add(rule_id)
    contract_rows: list[dict[str, Any]] = []
    revision_rows: list[dict[str, Any]] = []
    typed_gap_count = 0
    snapshot_rows: list[dict[str, Any]] = []
    snapshot_revision_rows: list[dict[str, Any]] = []
    snapshot_gap_count = 0
    link_count = 0

    for contract in contract_records:
        action_contract_id = _require_text(
            contract.get("action_contract_id") or contract.get("action_id"),
            field_name="contract.action_contract_id",
        )
        action_id = _require_text(contract.get("action_id"), field_name="contract.action_id")
        revision_id = _require_text(contract.get("revision_id"), field_name="contract.revision_id")
        contract_hash = _require_text(contract.get("contract_hash"), field_name="contract.contract_hash")
        status = _require_text(contract.get("status"), field_name="contract.status")
        gaps = [dict(item) for item in contract.get("validation_gaps") or [] if isinstance(item, dict)]
        automation_refs = {str(item) for item in contract.get("automation_rule_refs") or [] if str(item)}
        automation_refs.update(snapshot_links_by_action.get(action_contract_id, set()))
        head_row = conn.fetchrow(
            """
            INSERT INTO integration_action_contract_heads (
                action_contract_id,
                action_id,
                name,
                owner_ref,
                status,
                source_system_ref,
                target_system_ref,
                target_provider,
                execution_mode,
                idempotency_state,
                rollback_class,
                mutating,
                current_revision_id,
                current_contract_hash,
                typed_gap_count,
                automation_rule_count,
                contract_json,
                observed_by_ref,
                source_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17::jsonb, $18, $19
            )
            ON CONFLICT (action_contract_id) DO UPDATE SET
                action_id = EXCLUDED.action_id,
                name = EXCLUDED.name,
                owner_ref = EXCLUDED.owner_ref,
                status = EXCLUDED.status,
                source_system_ref = EXCLUDED.source_system_ref,
                target_system_ref = EXCLUDED.target_system_ref,
                target_provider = EXCLUDED.target_provider,
                execution_mode = EXCLUDED.execution_mode,
                idempotency_state = EXCLUDED.idempotency_state,
                rollback_class = EXCLUDED.rollback_class,
                mutating = EXCLUDED.mutating,
                current_revision_id = EXCLUDED.current_revision_id,
                current_contract_hash = EXCLUDED.current_contract_hash,
                typed_gap_count = EXCLUDED.typed_gap_count,
                automation_rule_count = EXCLUDED.automation_rule_count,
                contract_json = EXCLUDED.contract_json,
                observed_by_ref = EXCLUDED.observed_by_ref,
                source_ref = EXCLUDED.source_ref,
                updated_at = now()
            RETURNING *
            """,
            action_contract_id,
            action_id,
            _require_text(contract.get("name"), field_name="contract.name"),
            _optional_clean_text(contract.get("owner") or contract.get("owner_ref"), field_name="contract.owner"),
            status,
            _system_ref(contract, "source"),
            _system_ref(contract, "target"),
            _target_provider(contract),
            _optional_clean_text(contract.get("execution_mode"), field_name="contract.execution_mode"),
            _nested_value(contract, "idempotency", "state"),
            _nested_value(contract, "rollback", "rollback_class"),
            _is_mutating(contract),
            revision_id,
            contract_hash,
            len(gaps),
            len(automation_refs),
            _encode_jsonb(contract, field_name="contract"),
            _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )
        revision_row = conn.fetchrow(
            """
            INSERT INTO integration_action_contract_revisions (
                action_contract_id,
                revision_id,
                revision_no,
                parent_revision_id,
                action_id,
                contract_hash,
                status,
                captured_at,
                contract_json,
                validation_gaps_json,
                observed_by_ref,
                source_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12
            )
            ON CONFLICT (action_contract_id, revision_id) DO UPDATE SET
                revision_no = EXCLUDED.revision_no,
                parent_revision_id = EXCLUDED.parent_revision_id,
                action_id = EXCLUDED.action_id,
                contract_hash = EXCLUDED.contract_hash,
                status = EXCLUDED.status,
                captured_at = EXCLUDED.captured_at,
                contract_json = EXCLUDED.contract_json,
                validation_gaps_json = EXCLUDED.validation_gaps_json,
                observed_by_ref = EXCLUDED.observed_by_ref,
                source_ref = EXCLUDED.source_ref
            RETURNING *
            """,
            action_contract_id,
            revision_id,
            int(contract.get("revision_no") or 1),
            _optional_clean_text(contract.get("parent_revision_id"), field_name="contract.parent_revision_id"),
            action_id,
            contract_hash,
            status,
            _timestamp_or_none(contract.get("captured_at"), field_name="contract.captured_at"),
            _encode_jsonb(contract, field_name="contract"),
            _encode_jsonb(gaps, field_name="validation_gaps"),
            _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )
        conn.execute(
            "DELETE FROM integration_action_contract_typed_gaps WHERE action_contract_id = $1 AND revision_id = $2",
            action_contract_id,
            revision_id,
        )
        if gaps:
            conn.execute_many(
                """
                INSERT INTO integration_action_contract_typed_gaps (
                    action_contract_id,
                    revision_id,
                    gap_id,
                    gap_kind,
                    severity,
                    related_ref,
                    disposition,
                    gap_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                ON CONFLICT (action_contract_id, revision_id, gap_id) DO UPDATE SET
                    gap_kind = EXCLUDED.gap_kind,
                    severity = EXCLUDED.severity,
                    related_ref = EXCLUDED.related_ref,
                    disposition = EXCLUDED.disposition,
                    gap_json = EXCLUDED.gap_json
                """,
                [
                    (
                        action_contract_id,
                        revision_id,
                        _require_text(gap.get("gap_id"), field_name="gap.gap_id"),
                        _require_text(gap.get("gap_kind"), field_name="gap.gap_kind"),
                        _require_text(gap.get("severity"), field_name="gap.severity"),
                        _require_text(gap.get("related_ref"), field_name="gap.related_ref"),
                        _require_text(gap.get("disposition"), field_name="gap.disposition"),
                        _encode_jsonb(gap, field_name="gap"),
                    )
                    for gap in gaps
                ],
            )
        contract_rows.append(_normalize_row(head_row, operation="persist_integration_action_contract.head"))
        revision_rows.append(_normalize_row(revision_row, operation="persist_integration_action_contract.revision"))
        typed_gap_count += len(gaps)

    for snapshot in snapshot_records:
        rule_id = _require_text(snapshot.get("rule_id"), field_name="automation_snapshot.rule_id")
        snapshot_id = _require_text(snapshot.get("snapshot_id"), field_name="automation_snapshot.snapshot_id")
        snapshot_hash = _require_text(snapshot.get("snapshot_hash"), field_name="automation_snapshot.snapshot_hash")
        gaps = [dict(item) for item in snapshot.get("validation_gaps") or [] if isinstance(item, dict)]
        linked_action_ids = [str(item) for item in snapshot.get("linked_action_ids") or [] if str(item)]
        head_row = conn.fetchrow(
            """
            INSERT INTO integration_automation_rule_snapshot_heads (
                automation_rule_id,
                name,
                status,
                owner_ref,
                source_of_truth_ref,
                current_snapshot_id,
                current_snapshot_hash,
                capture_method,
                linked_action_count,
                typed_gap_count,
                snapshot_json,
                observed_by_ref,
                source_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13
            )
            ON CONFLICT (automation_rule_id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                owner_ref = EXCLUDED.owner_ref,
                source_of_truth_ref = EXCLUDED.source_of_truth_ref,
                current_snapshot_id = EXCLUDED.current_snapshot_id,
                current_snapshot_hash = EXCLUDED.current_snapshot_hash,
                capture_method = EXCLUDED.capture_method,
                linked_action_count = EXCLUDED.linked_action_count,
                typed_gap_count = EXCLUDED.typed_gap_count,
                snapshot_json = EXCLUDED.snapshot_json,
                observed_by_ref = EXCLUDED.observed_by_ref,
                source_ref = EXCLUDED.source_ref,
                updated_at = now()
            RETURNING *
            """,
            rule_id,
            _require_text(snapshot.get("name"), field_name="automation_snapshot.name"),
            _require_text(snapshot.get("status"), field_name="automation_snapshot.status"),
            _optional_clean_text(snapshot.get("owner") or snapshot.get("owner_ref"), field_name="automation_snapshot.owner"),
            _optional_clean_text(snapshot.get("source_of_truth_ref"), field_name="automation_snapshot.source_of_truth_ref"),
            snapshot_id,
            snapshot_hash,
            _optional_clean_text(snapshot.get("capture_method"), field_name="automation_snapshot.capture_method"),
            len(linked_action_ids),
            len(gaps),
            _encode_jsonb(snapshot, field_name="automation_snapshot"),
            _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )
        revision_row = conn.fetchrow(
            """
            INSERT INTO integration_automation_rule_snapshot_revisions (
                automation_rule_id,
                snapshot_id,
                snapshot_hash,
                status,
                snapshot_timestamp,
                snapshot_json,
                validation_gaps_json,
                observed_by_ref,
                source_ref
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)
            ON CONFLICT (automation_rule_id, snapshot_id) DO UPDATE SET
                snapshot_hash = EXCLUDED.snapshot_hash,
                status = EXCLUDED.status,
                snapshot_timestamp = EXCLUDED.snapshot_timestamp,
                snapshot_json = EXCLUDED.snapshot_json,
                validation_gaps_json = EXCLUDED.validation_gaps_json,
                observed_by_ref = EXCLUDED.observed_by_ref,
                source_ref = EXCLUDED.source_ref
            RETURNING *
            """,
            rule_id,
            snapshot_id,
            snapshot_hash,
            _require_text(snapshot.get("status"), field_name="automation_snapshot.status"),
            _timestamp_or_none(snapshot.get("snapshot_timestamp"), field_name="automation_snapshot.snapshot_timestamp"),
            _encode_jsonb(snapshot, field_name="automation_snapshot"),
            _encode_jsonb(gaps, field_name="validation_gaps"),
            _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
            _optional_clean_text(source_ref, field_name="source_ref"),
        )
        conn.execute(
            "DELETE FROM integration_automation_rule_snapshot_gaps WHERE automation_rule_id = $1 AND snapshot_id = $2",
            rule_id,
            snapshot_id,
        )
        conn.execute(
            "DELETE FROM integration_automation_action_links WHERE automation_rule_id = $1 AND snapshot_id = $2",
            rule_id,
            snapshot_id,
        )
        if gaps:
            conn.execute_many(
                """
                INSERT INTO integration_automation_rule_snapshot_gaps (
                    automation_rule_id,
                    snapshot_id,
                    gap_id,
                    gap_kind,
                    severity,
                    related_ref,
                    disposition,
                    gap_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                ON CONFLICT (automation_rule_id, snapshot_id, gap_id) DO UPDATE SET
                    gap_kind = EXCLUDED.gap_kind,
                    severity = EXCLUDED.severity,
                    related_ref = EXCLUDED.related_ref,
                    disposition = EXCLUDED.disposition,
                    gap_json = EXCLUDED.gap_json
                """,
                [
                    (
                        rule_id,
                        snapshot_id,
                        _require_text(gap.get("gap_id"), field_name="snapshot_gap.gap_id"),
                        _require_text(gap.get("gap_kind"), field_name="snapshot_gap.gap_kind"),
                        _require_text(gap.get("severity"), field_name="snapshot_gap.severity"),
                        _require_text(gap.get("related_ref"), field_name="snapshot_gap.related_ref"),
                        _require_text(gap.get("disposition"), field_name="snapshot_gap.disposition"),
                        _encode_jsonb(gap, field_name="snapshot_gap"),
                    )
                    for gap in gaps
                ],
            )
        if linked_action_ids:
            conn.execute_many(
                """
                INSERT INTO integration_automation_action_links (
                    automation_rule_id,
                    snapshot_id,
                    action_contract_id,
                    link_source
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (automation_rule_id, snapshot_id, action_contract_id) DO UPDATE SET
                    link_source = EXCLUDED.link_source
                """,
                [(rule_id, snapshot_id, action_id, "automation_snapshot.linked_action_ids") for action_id in linked_action_ids],
            )
            link_count += len(linked_action_ids)
        snapshot_rows.append(_normalize_row(head_row, operation="persist_integration_automation_snapshot.head"))
        snapshot_revision_rows.append(_normalize_row(revision_row, operation="persist_integration_automation_snapshot.revision"))
        snapshot_gap_count += len(gaps)

    return {
        "contracts": contract_rows,
        "contract_revisions": revision_rows,
        "contract_count": len(contract_rows),
        "contract_typed_gap_count": typed_gap_count,
        "automation_snapshots": snapshot_rows,
        "automation_snapshot_revisions": snapshot_revision_rows,
        "automation_snapshot_count": len(snapshot_rows),
        "automation_snapshot_gap_count": snapshot_gap_count,
        "automation_action_link_count": link_count,
    }


def list_integration_action_contracts(
    conn: Any,
    *,
    target_system_ref: str | None = None,
    status: str | None = None,
    owner_ref: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            action_contract_id,
            action_id,
            name,
            owner_ref,
            status,
            source_system_ref,
            target_system_ref,
            target_provider,
            execution_mode,
            idempotency_state,
            rollback_class,
            mutating,
            current_revision_id,
            current_contract_hash,
            typed_gap_count,
            automation_rule_count,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM integration_action_contract_heads
         WHERE ($1::text IS NULL OR target_system_ref = $1)
           AND ($2::text IS NULL OR status = $2)
           AND ($3::text IS NULL OR owner_ref = $3)
         ORDER BY updated_at DESC, created_at DESC
         LIMIT $4
        """,
        _optional_clean_text(target_system_ref, field_name="target_system_ref"),
        _optional_clean_text(status, field_name="status"),
        _optional_clean_text(owner_ref, field_name="owner_ref"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_integration_action_contracts")


def load_integration_action_contract(
    conn: Any,
    *,
    action_contract_id: str,
    include_history: bool = True,
    include_automation: bool = True,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
          FROM integration_action_contract_heads
         WHERE action_contract_id = $1
        """,
        _require_text(action_contract_id, field_name="action_contract_id"),
    )
    if row is None:
        return None
    contract = _normalize_row(row, operation="load_integration_action_contract.head")
    if include_history:
        revisions = conn.fetch(
            """
            SELECT *
              FROM integration_action_contract_revisions
             WHERE action_contract_id = $1
             ORDER BY revision_no, revision_id
            """,
            _require_text(action_contract_id, field_name="action_contract_id"),
        )
        gaps = conn.fetch(
            """
            SELECT gap_json
              FROM integration_action_contract_typed_gaps
             WHERE action_contract_id = $1
             ORDER BY revision_id, severity, gap_kind, gap_id
            """,
            _require_text(action_contract_id, field_name="action_contract_id"),
        )
        contract["revisions"] = _normalize_rows(revisions, operation="load_integration_action_contract.revisions")
        contract["typed_gaps"] = [
            item.get("gap_json")
            for item in _normalize_rows(gaps, operation="load_integration_action_contract.gaps")
            if item.get("gap_json") is not None
        ]
    if include_automation:
        snapshots = conn.fetch(
            """
            SELECT DISTINCT s.automation_rule_id, s.snapshot_json
              FROM integration_automation_rule_snapshot_revisions s
              JOIN integration_automation_action_links l
                ON l.automation_rule_id = s.automation_rule_id
               AND l.snapshot_id = s.snapshot_id
             WHERE l.action_contract_id = $1
             ORDER BY s.automation_rule_id
            """,
            _require_text(action_contract_id, field_name="action_contract_id"),
        )
        contract["automation_snapshots"] = [
            item.get("snapshot_json")
            for item in _normalize_rows(snapshots, operation="load_integration_action_contract.automation")
            if item.get("snapshot_json") is not None
        ]
    return contract


def list_automation_rule_snapshots(
    conn: Any,
    *,
    status: str | None = None,
    owner_ref: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            automation_rule_id,
            name,
            status,
            owner_ref,
            source_of_truth_ref,
            current_snapshot_id,
            current_snapshot_hash,
            capture_method,
            linked_action_count,
            typed_gap_count,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM integration_automation_rule_snapshot_heads
         WHERE ($1::text IS NULL OR status = $1)
           AND ($2::text IS NULL OR owner_ref = $2)
         ORDER BY updated_at DESC, created_at DESC
         LIMIT $3
        """,
        _optional_clean_text(status, field_name="status"),
        _optional_clean_text(owner_ref, field_name="owner_ref"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_automation_rule_snapshots")


def load_automation_rule_snapshot(
    conn: Any,
    *,
    automation_rule_id: str,
    include_history: bool = True,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
          FROM integration_automation_rule_snapshot_heads
         WHERE automation_rule_id = $1
        """,
        _require_text(automation_rule_id, field_name="automation_rule_id"),
    )
    if row is None:
        return None
    snapshot = _normalize_row(row, operation="load_automation_rule_snapshot.head")
    if include_history:
        revisions = conn.fetch(
            """
            SELECT *
              FROM integration_automation_rule_snapshot_revisions
             WHERE automation_rule_id = $1
             ORDER BY snapshot_timestamp NULLS LAST, snapshot_id
            """,
            _require_text(automation_rule_id, field_name="automation_rule_id"),
        )
        gaps = conn.fetch(
            """
            SELECT gap_json
              FROM integration_automation_rule_snapshot_gaps
             WHERE automation_rule_id = $1
             ORDER BY snapshot_id, severity, gap_kind, gap_id
            """,
            _require_text(automation_rule_id, field_name="automation_rule_id"),
        )
        links = conn.fetch(
            """
            SELECT action_contract_id, snapshot_id, link_source
              FROM integration_automation_action_links
             WHERE automation_rule_id = $1
             ORDER BY snapshot_id, action_contract_id
            """,
            _require_text(automation_rule_id, field_name="automation_rule_id"),
        )
        snapshot["revisions"] = _normalize_rows(revisions, operation="load_automation_rule_snapshot.revisions")
        snapshot["typed_gaps"] = [
            item.get("gap_json")
            for item in _normalize_rows(gaps, operation="load_automation_rule_snapshot.gaps")
            if item.get("gap_json") is not None
        ]
        snapshot["action_links"] = _normalize_rows(links, operation="load_automation_rule_snapshot.links")
    return snapshot


__all__ = [
    "list_automation_rule_snapshots",
    "list_integration_action_contracts",
    "load_automation_rule_snapshot",
    "load_integration_action_contract",
    "persist_integration_action_contract_inventory",
]
