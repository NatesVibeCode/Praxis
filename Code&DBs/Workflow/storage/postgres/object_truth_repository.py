"""Postgres persistence for object-truth evidence."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Mapping, Sequence

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)

_REQUIRED_TABLES = (
    "object_truth_object_versions",
    "object_truth_field_observations",
    "object_truth_schema_snapshots",
    "object_truth_comparison_runs",
)

_REQUIRED_OPERATIONS = {
    "object_truth_observe_record": {
        "operation_kind": "query",
        "idempotency_policy": "read_only",
    },
    "object_truth_compare_versions": {
        "operation_kind": "query",
        "idempotency_policy": "read_only",
    },
    "object_truth_store_observed_record": {
        "operation_kind": "command",
        "idempotency_policy": "idempotent",
    },
    "object_truth_store_schema_snapshot": {
        "operation_kind": "command",
        "idempotency_policy": "idempotent",
    },
    "object_truth_record_comparison_run": {
        "operation_kind": "command",
        "idempotency_policy": "idempotent",
    },
}

_REQUIRED_EVENT_CONTRACTS = (
    "object_truth.object_version_stored",
    "object_truth.schema_snapshot_stored",
    "object_truth.comparison_run_recorded",
)

_REQUIRED_PRIVACY_COLUMNS = {
    "object_truth_object_versions": (
        "payload_digest",
        "source_metadata_json",
        "object_version_json",
    ),
    "object_truth_field_observations": (
        "sensitive",
        "normalized_value_digest",
        "redacted_value_preview_json",
    ),
}


def _rows(rows: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in rows or []]


def _gate(
    gate_ref: str,
    *,
    status: str,
    reason: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "gate_ref": gate_ref,
        "status": status,
        "reason": reason,
        "details": details or {},
    }


def inspect_readiness(
    conn: Any,
    *,
    client_payload_mode: str = "redacted_hashes",
    privacy_policy_ref: str | None = None,
    planned_fanout: int = 1,
    include_counts: bool = True,
) -> dict[str, Any]:
    """Inspect whether Object Truth authority is ready for downstream work."""

    table_rows = _rows(
        conn.fetch(
            """
            SELECT required.name AS table_name,
                   to_regclass('public.' || required.name) IS NOT NULL AS present
              FROM unnest($1::text[]) AS required(name)
             ORDER BY required.name
            """,
            list(_REQUIRED_TABLES),
        )
    )
    table_presence = {
        str(row["table_name"]): bool(row["present"])
        for row in table_rows
    }

    operation_rows = _rows(
        conn.fetch(
            """
            SELECT operation_name, operation_kind, idempotency_policy,
                   posture, enabled, authority_domain_ref
              FROM operation_catalog_registry
             WHERE operation_name = ANY($1::text[])
             ORDER BY operation_name
            """,
            list(_REQUIRED_OPERATIONS),
        )
    )
    operations_by_name = {
        str(row["operation_name"]): row
        for row in operation_rows
    }

    authority_domain = dict(
        conn.fetchrow(
            """
            SELECT authority_domain_ref, enabled, decision_ref
              FROM authority_domains
             WHERE authority_domain_ref = 'authority.object_truth'
            """
        )
        or {}
    )

    authority_object_rows = _rows(
        conn.fetch(
            """
            SELECT object_name, object_kind, lifecycle_status,
                   data_dictionary_object_kind
              FROM authority_object_registry
             WHERE authority_domain_ref = 'authority.object_truth'
               AND object_kind = 'table'
               AND object_name = ANY($1::text[])
             ORDER BY object_name
            """,
            list(_REQUIRED_TABLES),
        )
    )
    authority_objects = {
        str(row["object_name"]): row
        for row in authority_object_rows
    }

    dictionary_rows = _rows(
        conn.fetch(
            """
            SELECT object_kind, category
              FROM data_dictionary_objects
             WHERE object_kind = ANY($1::text[])
             ORDER BY object_kind
            """,
            list(_REQUIRED_TABLES),
        )
    )
    dictionary_objects = {
        str(row["object_kind"]): row
        for row in dictionary_rows
    }

    event_rows = _rows(
        conn.fetch(
            """
            SELECT event_type, enabled, receipt_required
              FROM authority_event_contracts
             WHERE authority_domain_ref = 'authority.object_truth'
               AND event_type = ANY($1::text[])
             ORDER BY event_type
            """,
            list(_REQUIRED_EVENT_CONTRACTS),
        )
    )
    event_contracts = {
        str(row["event_type"]): row
        for row in event_rows
    }

    column_rows = _rows(
        conn.fetch(
            """
            SELECT table_name, column_name
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = ANY($1::text[])
               AND column_name = ANY($2::text[])
             ORDER BY table_name, column_name
            """,
            list(_REQUIRED_PRIVACY_COLUMNS),
            sorted({column for columns in _REQUIRED_PRIVACY_COLUMNS.values() for column in columns}),
        )
    )
    columns_present = {
        (str(row["table_name"]), str(row["column_name"]))
        for row in column_rows
    }

    gates: list[dict[str, Any]] = []

    missing_tables = [
        table for table in _REQUIRED_TABLES
        if not table_presence.get(table)
    ]
    gates.append(
        _gate(
            "object_truth.tables",
            status="blocked" if missing_tables else "passed",
            reason="Required evidence tables are present." if not missing_tables else "Required evidence tables are missing.",
            details={
                "required": list(_REQUIRED_TABLES),
                "missing": missing_tables,
            },
        )
    )

    missing_operations: list[str] = []
    mismatched_operations: list[dict[str, Any]] = []
    for operation_name, expected in _REQUIRED_OPERATIONS.items():
        row = operations_by_name.get(operation_name)
        if not row:
            missing_operations.append(operation_name)
            continue
        for field_name, expected_value in expected.items():
            if row.get(field_name) != expected_value:
                mismatched_operations.append(
                    {
                        "operation_name": operation_name,
                        "field": field_name,
                        "expected": expected_value,
                        "actual": row.get(field_name),
                    }
                )
        if row.get("enabled") is not True:
            mismatched_operations.append(
                {
                    "operation_name": operation_name,
                    "field": "enabled",
                    "expected": True,
                    "actual": row.get("enabled"),
                }
            )
    gates.append(
        _gate(
            "object_truth.operations",
            status="blocked" if missing_operations or mismatched_operations else "passed",
            reason=(
                "Required CQRS operations are registered and enabled."
                if not missing_operations and not mismatched_operations
                else "Required CQRS operations are missing or mismatched."
            ),
            details={
                "required": sorted(_REQUIRED_OPERATIONS),
                "missing": missing_operations,
                "mismatched": mismatched_operations,
            },
        )
    )

    missing_authority_objects = [
        table for table in _REQUIRED_TABLES
        if table not in authority_objects
    ]
    missing_dictionary_objects = [
        table for table in _REQUIRED_TABLES
        if table not in dictionary_objects
    ]
    gates.append(
        _gate(
            "object_truth.registry",
            status="blocked" if missing_authority_objects or missing_dictionary_objects else "passed",
            reason=(
                "Evidence tables are registered in authority and data dictionary surfaces."
                if not missing_authority_objects and not missing_dictionary_objects
                else "Evidence tables are missing registry coverage."
            ),
            details={
                "missing_authority_objects": missing_authority_objects,
                "missing_data_dictionary_objects": missing_dictionary_objects,
            },
        )
    )

    missing_event_contracts = [
        event_type for event_type in _REQUIRED_EVENT_CONTRACTS
        if event_type not in event_contracts
    ]
    disabled_event_contracts = [
        event_type for event_type, row in event_contracts.items()
        if row.get("enabled") is not True or row.get("receipt_required") is not True
    ]
    gates.append(
        _gate(
            "object_truth.events",
            status="blocked" if missing_event_contracts or disabled_event_contracts else "passed",
            reason=(
                "Write operations have replayable receipt-required event contracts."
                if not missing_event_contracts and not disabled_event_contracts
                else "Write event contracts are missing, disabled, or not receipt-required."
            ),
            details={
                "required": list(_REQUIRED_EVENT_CONTRACTS),
                "missing": missing_event_contracts,
                "disabled_or_not_receipt_required": disabled_event_contracts,
            },
        )
    )

    missing_privacy_columns = [
        {"table": table, "column": column}
        for table, columns in _REQUIRED_PRIVACY_COLUMNS.items()
        for column in columns
        if (table, column) not in columns_present
    ]
    gates.append(
        _gate(
            "object_truth.privacy_columns",
            status="blocked" if missing_privacy_columns else "passed",
            reason=(
                "Digest, sensitivity, metadata, and redacted-preview columns are present."
                if not missing_privacy_columns
                else "Privacy posture columns are missing."
            ),
            details={"missing": missing_privacy_columns},
        )
    )

    privacy_blocked = client_payload_mode == "raw_client_payloads" and not privacy_policy_ref
    gates.append(
        _gate(
            "object_truth.client_payload_policy",
            status="blocked" if privacy_blocked else "passed",
            reason=(
                "Client payload mode is compatible with the available privacy posture."
                if not privacy_blocked
                else "Raw client payload mode requires an explicit privacy_policy_ref."
            ),
            details={
                "client_payload_mode": client_payload_mode,
                "privacy_policy_ref": privacy_policy_ref,
            },
        )
    )

    fanout_blocked = planned_fanout > 1 and bool(missing_operations or mismatched_operations)
    gates.append(
        _gate(
            "object_truth.safe_fanout",
            status="blocked" if fanout_blocked else "passed",
            reason=(
                "Requested fanout is safe for the current operation catalog."
                if not fanout_blocked
                else "Fanout is blocked until required operations are registered and enabled."
            ),
            details={"planned_fanout": planned_fanout},
        )
    )

    domain_blocked = not authority_domain or authority_domain.get("enabled") is not True
    gates.append(
        _gate(
            "object_truth.authority_domain",
            status="blocked" if domain_blocked else "passed",
            reason=(
                "authority.object_truth is enabled."
                if not domain_blocked
                else "authority.object_truth is missing or disabled."
            ),
            details={"authority_domain": authority_domain or None},
        )
    )

    no_go_conditions = [
        {
            "gate_ref": gate["gate_ref"],
            "reason": gate["reason"],
            "details": gate["details"],
        }
        for gate in gates
        if gate["status"] == "blocked"
    ]
    can_advance = not no_go_conditions
    counts: dict[str, int] | None = None
    if include_counts and not missing_tables:
        count_row = dict(
            conn.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM object_truth_object_versions) AS object_versions,
                    (SELECT count(*) FROM object_truth_field_observations) AS field_observations,
                    (SELECT count(*) FROM object_truth_schema_snapshots) AS schema_snapshots,
                    (SELECT count(*) FROM object_truth_comparison_runs) AS comparison_runs
                """
            )
            or {}
        )
        counts = {str(key): int(value or 0) for key, value in count_row.items()}

    return {
        "state": "ready" if can_advance else "blocked",
        "can_advance": can_advance,
        "authority": {
            "authority_domain_ref": "authority.object_truth",
            "policy_ref": "architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences",
            "evidence_authority": "Object Truth owns observed facts, hashes, schemas, versions, lineage, and comparison evidence.",
            "consequence_authority": "Virtual Lab owns simulated effects; live sandbox promotion proves or falsifies predictions.",
        },
        "db_health": {
            "status": "ready",
            "checked_via": "postgres_connection",
        },
        "privacy_posture": {
            "status": "blocked" if privacy_blocked or missing_privacy_columns else "guarded",
            "client_payload_mode": client_payload_mode,
            "privacy_policy_ref": privacy_policy_ref,
            "identity_values_storage": "present_as_evidence",
            "default_storage_posture": "hashes_metadata_redacted_previews",
        },
        "source_authority": {
            "required_tables": list(_REQUIRED_TABLES),
            "required_operations": sorted(_REQUIRED_OPERATIONS),
            "required_event_contracts": list(_REQUIRED_EVENT_CONTRACTS),
        },
        "safe_fanout": {
            "planned_fanout": planned_fanout,
            "status": "blocked" if fanout_blocked else "ready",
        },
        "gates": gates,
        "no_go_conditions": no_go_conditions,
        "counts": counts,
    }


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "object_truth.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key in {"source_metadata", "object_version"}):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


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
                "object_truth.invalid_timestamp",
                f"{field_name} must be an ISO timestamp",
                details={"field_name": field_name, "value": value},
            ) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise PostgresWriteError(
        "object_truth.invalid_timestamp",
        f"{field_name} must be an ISO timestamp",
        details={"field_name": field_name, "value": value},
    )


def persist_object_version(
    conn: Any,
    *,
    object_version: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one deterministic object-version packet and its field evidence."""

    version = dict(_require_mapping(object_version, field_name="object_version"))
    identity = dict(_require_mapping(version.get("identity"), field_name="object_version.identity"))
    object_version_digest = _require_text(
        version.get("object_version_digest"),
        field_name="object_version.object_version_digest",
    )
    field_observations = version.get("field_observations")
    if not isinstance(field_observations, list):
        raise PostgresWriteError(
            "object_truth.invalid_object_version",
            "object_version.field_observations must be a list",
            details={"field": "object_version.field_observations"},
        )

    row = conn.fetchrow(
        """
        INSERT INTO object_truth_object_versions (
            object_version_digest,
            object_version_ref,
            system_ref,
            object_ref,
            identity_digest,
            identity_values_json,
            payload_digest,
            schema_snapshot_digest,
            source_metadata_json,
            hierarchy_signals_json,
            object_version_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12, $13
        )
        ON CONFLICT (object_version_digest) DO UPDATE SET
            object_version_ref = EXCLUDED.object_version_ref,
            system_ref = EXCLUDED.system_ref,
            object_ref = EXCLUDED.object_ref,
            identity_digest = EXCLUDED.identity_digest,
            identity_values_json = EXCLUDED.identity_values_json,
            payload_digest = EXCLUDED.payload_digest,
            schema_snapshot_digest = EXCLUDED.schema_snapshot_digest,
            source_metadata_json = EXCLUDED.source_metadata_json,
            hierarchy_signals_json = EXCLUDED.hierarchy_signals_json,
            object_version_json = EXCLUDED.object_version_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            object_version_digest,
            object_version_ref,
            system_ref,
            object_ref,
            identity_digest,
            payload_digest,
            schema_snapshot_digest,
            source_metadata_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        object_version_digest,
        f"object_truth_object_version:{object_version_digest}",
        _require_text(version.get("system_ref"), field_name="object_version.system_ref"),
        _require_text(version.get("object_ref"), field_name="object_version.object_ref"),
        _require_text(identity.get("identity_digest"), field_name="object_version.identity.identity_digest"),
        _encode_jsonb(
            dict(_require_mapping(identity.get("identity_values"), field_name="object_version.identity.identity_values")),
            field_name="identity_values",
        ),
        _require_text(version.get("payload_digest"), field_name="object_version.payload_digest"),
        _optional_text(version.get("schema_snapshot_digest"), field_name="object_version.schema_snapshot_digest"),
        _encode_jsonb(
            dict(_require_mapping(version.get("source_metadata"), field_name="object_version.source_metadata")),
            field_name="source_metadata",
        ),
        _encode_jsonb(
            dict(_require_mapping(version.get("hierarchy_signals"), field_name="object_version.hierarchy_signals")),
            field_name="hierarchy_signals",
        ),
        _encode_jsonb(version, field_name="object_version"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )

    conn.execute(
        """
        DELETE FROM object_truth_field_observations
         WHERE object_version_digest = $1
        """,
        object_version_digest,
    )
    for observation in field_observations:
        obs = dict(_require_mapping(observation, field_name="field_observation"))
        conn.execute(
            """
            INSERT INTO object_truth_field_observations (
                object_version_digest,
                field_path,
                field_kind,
                presence,
                cardinality_kind,
                cardinality_count,
                sensitive,
                normalized_value_digest,
                redacted_value_preview_json,
                observation_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb
            )
            """,
            object_version_digest,
            _require_text(obs.get("field_path"), field_name="field_observation.field_path"),
            _require_text(obs.get("field_kind"), field_name="field_observation.field_kind"),
            _require_text(obs.get("presence"), field_name="field_observation.presence"),
            _require_text(obs.get("cardinality_kind"), field_name="field_observation.cardinality_kind"),
            obs.get("cardinality_count"),
            bool(obs.get("sensitive", False)),
            _require_text(
                obs.get("normalized_value_digest"),
                field_name="field_observation.normalized_value_digest",
            ),
            _encode_jsonb(obs.get("redacted_value_preview"), field_name="redacted_value_preview"),
            _encode_jsonb(obs, field_name="field_observation"),
        )

    persisted = _normalize_row(row, operation="persist_object_version")
    persisted["field_observation_count"] = len(field_observations)
    return persisted


def persist_schema_snapshot(
    conn: Any,
    *,
    schema_snapshot: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one deterministic normalized schema snapshot."""

    snapshot = dict(_require_mapping(schema_snapshot, field_name="schema_snapshot"))
    schema_digest = _require_text(
        snapshot.get("schema_digest"),
        field_name="schema_snapshot.schema_digest",
    )
    fields = snapshot.get("fields")
    if not isinstance(fields, list):
        raise PostgresWriteError(
            "object_truth.invalid_schema_snapshot",
            "schema_snapshot.fields must be a list",
            details={"field": "schema_snapshot.fields"},
        )

    row = conn.fetchrow(
        """
        INSERT INTO object_truth_schema_snapshots (
            schema_snapshot_digest,
            schema_snapshot_ref,
            system_ref,
            object_ref,
            field_count,
            schema_snapshot_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7, $8
        )
        ON CONFLICT (schema_snapshot_digest) DO UPDATE SET
            schema_snapshot_ref = EXCLUDED.schema_snapshot_ref,
            system_ref = EXCLUDED.system_ref,
            object_ref = EXCLUDED.object_ref,
            field_count = EXCLUDED.field_count,
            schema_snapshot_json = EXCLUDED.schema_snapshot_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            schema_snapshot_digest,
            schema_snapshot_ref,
            system_ref,
            object_ref,
            field_count,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        schema_digest,
        f"object_truth_schema_snapshot:{schema_digest}",
        _require_text(snapshot.get("system_ref"), field_name="schema_snapshot.system_ref"),
        _require_text(snapshot.get("object_ref"), field_name="schema_snapshot.object_ref"),
        len(fields),
        _encode_jsonb(snapshot, field_name="schema_snapshot"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    return _normalize_row(row, operation="persist_schema_snapshot")


def load_object_version(
    conn: Any,
    *,
    object_version_digest: str,
) -> dict[str, Any] | None:
    """Load a stored object-version packet by digest."""

    row = conn.fetchrow(
        """
        SELECT object_version_json
          FROM object_truth_object_versions
         WHERE object_version_digest = $1
        """,
        _require_text(object_version_digest, field_name="object_version_digest"),
    )
    if row is None:
        return None
    payload = dict(row).get("object_version_json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PostgresWriteError(
                "object_truth.invalid_stored_object_version",
                "stored object_version_json is not valid JSON",
            ) from exc
    if not isinstance(payload, dict):
        raise PostgresWriteError(
            "object_truth.invalid_stored_object_version",
            "stored object_version_json must be a JSON object",
        )
    return payload


def load_latest_object_truth_version(
    conn: Any,
    *,
    system_ref: str | None = None,
    object_ref: str | None = None,
    identity_digest: str | None = None,
    client_ref: str | None = None,
    trusted_only: bool = True,
    max_age_seconds: int | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Load the latest trusted Object Truth version without a known digest."""

    rows = conn.fetch(
        """
        SELECT
            object_version_digest,
            object_version_ref,
            system_ref,
            object_ref,
            identity_digest,
            payload_digest,
            schema_snapshot_digest,
            source_metadata_json,
            hierarchy_signals_json,
            object_version_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM object_truth_object_versions
         WHERE ($1::text IS NULL OR system_ref = $1)
           AND ($2::text IS NULL OR object_ref = $2)
           AND ($3::text IS NULL OR identity_digest = $3)
           AND (
                $4::text IS NULL
                OR source_metadata_json ->> 'client_ref' = $4
                OR object_version_json ->> 'client_ref' = $4
           )
         ORDER BY updated_at DESC, created_at DESC, object_version_digest
         LIMIT $5
        """,
        _optional_text(system_ref, field_name="system_ref"),
        _optional_text(object_ref, field_name="object_ref"),
        _optional_text(identity_digest, field_name="identity_digest"),
        _optional_text(client_ref, field_name="client_ref"),
        max(1, min(int(limit), 200)),
    )
    candidates = _normalize_rows(rows, operation="load_latest_object_truth_version")
    if trusted_only:
        candidates = [row for row in candidates if _object_truth_row_is_trusted(row)]
    if not candidates:
        return {
            "state": "missing",
            "version": None,
            "freshness": {"state": "missing"},
            "conflicts": [],
            "no_go_states": ["missing"],
            "filters": {
                "system_ref": system_ref,
                "object_ref": object_ref,
                "identity_digest": identity_digest,
                "client_ref": client_ref,
                "trusted_only": trusted_only,
            },
        }

    latest = candidates[0]
    freshness = _latest_version_freshness(latest, max_age_seconds=max_age_seconds)
    conflicts = _latest_version_conflicts(candidates)
    no_go_states: list[str] = []
    if freshness["state"] == "stale":
        no_go_states.append("stale")
    if conflicts:
        no_go_states.append("conflict")
    state = "ready" if not no_go_states else "blocked"
    return {
        "state": state,
        "version": latest,
        "freshness": freshness,
        "conflicts": conflicts,
        "no_go_states": no_go_states,
        "candidate_count": len(candidates),
        "filters": {
            "system_ref": system_ref,
            "object_ref": object_ref,
            "identity_digest": identity_digest,
            "client_ref": client_ref,
            "trusted_only": trusted_only,
        },
    }


def _object_truth_row_is_trusted(row: Mapping[str, Any]) -> bool:
    source_metadata = row.get("source_metadata_json") or {}
    version = row.get("object_version_json") or {}
    if not isinstance(source_metadata, Mapping):
        source_metadata = {}
    if not isinstance(version, Mapping):
        version = {}
    trust_markers = {
        str(source_metadata.get("trust_state") or "").lower(),
        str(source_metadata.get("evidence_tier") or "").lower(),
        str(version.get("truth_state") or "").lower(),
        str(version.get("evidence_tier") or "").lower(),
    }
    if trust_markers.intersection({"trusted", "observed", "verified", "promoted", "schema_bound"}):
        return True
    if bool(source_metadata.get("trusted")) or bool(version.get("trusted")):
        return True
    return not trust_markers.difference({""})


def _latest_version_freshness(
    row: Mapping[str, Any],
    *,
    max_age_seconds: int | None,
) -> dict[str, Any]:
    updated_at = row.get("updated_at") or row.get("created_at")
    observed_at = updated_at
    source_metadata = row.get("source_metadata_json") or {}
    if isinstance(source_metadata, Mapping) and source_metadata.get("observed_at"):
        try:
            observed_at = _timestamp(source_metadata.get("observed_at"), field_name="source_metadata.observed_at")
        except PostgresWriteError:
            observed_at = updated_at
    if not isinstance(observed_at, datetime):
        return {"state": "unknown", "observed_at": None, "max_age_seconds": max_age_seconds}
    normalized = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (datetime.now(timezone.utc) - normalized.astimezone(timezone.utc)).total_seconds())
    state = "fresh"
    if max_age_seconds is not None and age_seconds > int(max_age_seconds):
        state = "stale"
    return {
        "state": state,
        "observed_at": normalized.isoformat(),
        "age_seconds": round(age_seconds, 3),
        "max_age_seconds": max_age_seconds,
    }


def _latest_version_conflicts(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    payload_digests = {
        str(row.get("payload_digest"))
        for row in rows[:10]
        if row.get("payload_digest")
    }
    if len(payload_digests) > 1:
        conflicts.append(
            {
                "conflict_type": "multiple_payload_digests",
                "payload_digests": sorted(payload_digests),
                "candidate_count": len(rows[:10]),
            }
        )
    for row in rows[:10]:
        metadata = row.get("source_metadata_json") or {}
        if isinstance(metadata, Mapping) and metadata.get("conflicts"):
            conflicts.append(
                {
                    "conflict_type": "source_metadata_conflict",
                    "object_version_digest": row.get("object_version_digest"),
                    "details": metadata.get("conflicts"),
                }
            )
    return conflicts


def persist_comparison_run(
    conn: Any,
    *,
    comparison: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one deterministic comparison result between stored object versions."""

    payload = dict(_require_mapping(comparison, field_name="comparison"))
    comparison_digest = _require_text(
        payload.get("comparison_digest"),
        field_name="comparison.comparison_digest",
    )
    summary = dict(_require_mapping(payload.get("summary"), field_name="comparison.summary"))
    freshness = dict(_require_mapping(payload.get("freshness"), field_name="comparison.freshness"))

    row = conn.fetchrow(
        """
        INSERT INTO object_truth_comparison_runs (
            comparison_run_digest,
            comparison_run_ref,
            comparison_digest,
            left_object_version_digest,
            right_object_version_digest,
            left_identity_digest,
            right_identity_digest,
            summary_json,
            freshness_json,
            comparison_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11, $12
        )
        ON CONFLICT (comparison_run_digest) DO UPDATE SET
            comparison_run_ref = EXCLUDED.comparison_run_ref,
            comparison_digest = EXCLUDED.comparison_digest,
            left_object_version_digest = EXCLUDED.left_object_version_digest,
            right_object_version_digest = EXCLUDED.right_object_version_digest,
            left_identity_digest = EXCLUDED.left_identity_digest,
            right_identity_digest = EXCLUDED.right_identity_digest,
            summary_json = EXCLUDED.summary_json,
            freshness_json = EXCLUDED.freshness_json,
            comparison_json = EXCLUDED.comparison_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            comparison_run_digest,
            comparison_run_ref,
            comparison_digest,
            left_object_version_digest,
            right_object_version_digest,
            left_identity_digest,
            right_identity_digest,
            summary_json,
            freshness_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        comparison_digest,
        f"object_truth_comparison_run:{comparison_digest}",
        comparison_digest,
        _require_text(
            payload.get("left_object_version_digest"),
            field_name="comparison.left_object_version_digest",
        ),
        _require_text(
            payload.get("right_object_version_digest"),
            field_name="comparison.right_object_version_digest",
        ),
        _require_text(payload.get("left_identity_digest"), field_name="comparison.left_identity_digest"),
        _require_text(payload.get("right_identity_digest"), field_name="comparison.right_identity_digest"),
        _encode_jsonb(summary, field_name="comparison.summary"),
        _encode_jsonb(freshness, field_name="comparison.freshness"),
        _encode_jsonb(payload, field_name="comparison"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    return _normalize_row(row, operation="persist_comparison_run")


def persist_ingestion_sample(
    conn: Any,
    *,
    system_snapshot: dict[str, Any],
    sample_capture: dict[str, Any],
    payload_references: list[dict[str, Any]] | None = None,
    object_version_refs: list[dict[str, Any]] | None = None,
    replay_fixture: dict[str, Any] | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one Object Truth ingestion sample packet and its child evidence."""

    snapshot = dict(_require_mapping(system_snapshot, field_name="system_snapshot"))
    sample = dict(_require_mapping(sample_capture, field_name="sample_capture"))
    payload_rows = [
        dict(_require_mapping(item, field_name="payload_reference"))
        for item in (payload_references or [])
    ]
    object_refs = [
        dict(_require_mapping(item, field_name="object_version_ref"))
        for item in (object_version_refs or [])
    ]
    fixture = dict(_require_mapping(replay_fixture or {}, field_name="replay_fixture"))

    system_snapshot_id = _require_text(
        snapshot.get("system_snapshot_id"),
        field_name="system_snapshot.system_snapshot_id",
    )
    sample_id = _require_text(sample.get("sample_id"), field_name="sample_capture.sample_id")

    snapshot_row = conn.fetchrow(
        """
        INSERT INTO object_truth_system_snapshots (
            system_snapshot_id,
            system_snapshot_digest,
            client_ref,
            system_ref,
            integration_id,
            connector_ref,
            environment_ref,
            auth_context_hash,
            captured_at,
            capture_receipt_id,
            schema_snapshot_count,
            sample_count,
            metadata_json,
            system_snapshot_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15, $16
        )
        ON CONFLICT (system_snapshot_id) DO UPDATE SET
            system_snapshot_digest = EXCLUDED.system_snapshot_digest,
            client_ref = EXCLUDED.client_ref,
            system_ref = EXCLUDED.system_ref,
            integration_id = EXCLUDED.integration_id,
            connector_ref = EXCLUDED.connector_ref,
            environment_ref = EXCLUDED.environment_ref,
            auth_context_hash = EXCLUDED.auth_context_hash,
            captured_at = EXCLUDED.captured_at,
            capture_receipt_id = EXCLUDED.capture_receipt_id,
            schema_snapshot_count = EXCLUDED.schema_snapshot_count,
            sample_count = EXCLUDED.sample_count,
            metadata_json = EXCLUDED.metadata_json,
            system_snapshot_json = EXCLUDED.system_snapshot_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            system_snapshot_id,
            system_snapshot_digest,
            client_ref,
            system_ref,
            integration_id,
            connector_ref,
            environment_ref,
            auth_context_hash,
            captured_at,
            capture_receipt_id,
            schema_snapshot_count,
            sample_count,
            metadata_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        system_snapshot_id,
        _require_text(
            snapshot.get("system_snapshot_digest"),
            field_name="system_snapshot.system_snapshot_digest",
        ),
        _require_text(snapshot.get("client_ref"), field_name="system_snapshot.client_ref"),
        _require_text(snapshot.get("system_ref"), field_name="system_snapshot.system_ref"),
        _require_text(snapshot.get("integration_id"), field_name="system_snapshot.integration_id"),
        _require_text(snapshot.get("connector_ref"), field_name="system_snapshot.connector_ref"),
        _require_text(snapshot.get("environment_ref"), field_name="system_snapshot.environment_ref"),
        _require_text(snapshot.get("auth_context_hash"), field_name="system_snapshot.auth_context_hash"),
        _timestamp(snapshot.get("captured_at"), field_name="system_snapshot.captured_at"),
        _require_text(snapshot.get("capture_receipt_id"), field_name="system_snapshot.capture_receipt_id"),
        int(snapshot.get("schema_snapshot_count") or 0),
        int(snapshot.get("sample_count") or 0),
        _encode_jsonb(snapshot.get("metadata_json") or {}, field_name="system_snapshot.metadata_json"),
        _encode_jsonb(snapshot, field_name="system_snapshot"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )

    sample_row = conn.fetchrow(
        """
        INSERT INTO object_truth_sample_captures (
            sample_id,
            system_snapshot_id,
            sample_capture_digest,
            schema_snapshot_digest,
            system_ref,
            object_ref,
            sample_strategy,
            source_query_json,
            cursor_ref,
            sample_size_requested,
            sample_size_returned,
            sample_hash,
            status,
            receipt_id,
            source_window_json,
            source_evidence_digest,
            metadata_json,
            sample_capture_json,
            replay_fixture_json,
            object_version_refs_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14,
            $15::jsonb, $16, $17::jsonb, $18::jsonb, $19::jsonb, $20::jsonb, $21, $22
        )
        ON CONFLICT (sample_id) DO UPDATE SET
            system_snapshot_id = EXCLUDED.system_snapshot_id,
            sample_capture_digest = EXCLUDED.sample_capture_digest,
            schema_snapshot_digest = EXCLUDED.schema_snapshot_digest,
            system_ref = EXCLUDED.system_ref,
            object_ref = EXCLUDED.object_ref,
            sample_strategy = EXCLUDED.sample_strategy,
            source_query_json = EXCLUDED.source_query_json,
            cursor_ref = EXCLUDED.cursor_ref,
            sample_size_requested = EXCLUDED.sample_size_requested,
            sample_size_returned = EXCLUDED.sample_size_returned,
            sample_hash = EXCLUDED.sample_hash,
            status = EXCLUDED.status,
            receipt_id = EXCLUDED.receipt_id,
            source_window_json = EXCLUDED.source_window_json,
            source_evidence_digest = EXCLUDED.source_evidence_digest,
            metadata_json = EXCLUDED.metadata_json,
            sample_capture_json = EXCLUDED.sample_capture_json,
            replay_fixture_json = EXCLUDED.replay_fixture_json,
            object_version_refs_json = EXCLUDED.object_version_refs_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            sample_id,
            system_snapshot_id,
            sample_capture_digest,
            schema_snapshot_digest,
            system_ref,
            object_ref,
            sample_strategy,
            cursor_ref,
            sample_size_requested,
            sample_size_returned,
            sample_hash,
            status,
            receipt_id,
            source_evidence_digest,
            metadata_json,
            replay_fixture_json,
            object_version_refs_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        sample_id,
        system_snapshot_id,
        _require_text(
            sample.get("sample_capture_digest"),
            field_name="sample_capture.sample_capture_digest",
        ),
        _require_text(
            sample.get("schema_snapshot_digest"),
            field_name="sample_capture.schema_snapshot_digest",
        ),
        _require_text(sample.get("system_ref"), field_name="sample_capture.system_ref"),
        _require_text(sample.get("object_ref"), field_name="sample_capture.object_ref"),
        _require_text(sample.get("sample_strategy"), field_name="sample_capture.sample_strategy"),
        _encode_jsonb(sample.get("source_query_json") or {}, field_name="sample_capture.source_query_json"),
        _optional_text(sample.get("cursor_ref"), field_name="sample_capture.cursor_ref"),
        int(sample.get("sample_size_requested") or 0),
        int(sample.get("sample_size_returned") or 0),
        _require_text(sample.get("sample_hash"), field_name="sample_capture.sample_hash"),
        _require_text(sample.get("status"), field_name="sample_capture.status"),
        _optional_text(sample.get("receipt_id"), field_name="sample_capture.receipt_id"),
        _encode_jsonb(sample.get("source_window_json") or {}, field_name="sample_capture.source_window_json"),
        _optional_text(sample.get("source_evidence_digest"), field_name="sample_capture.source_evidence_digest"),
        _encode_jsonb(sample.get("metadata_json") or {}, field_name="sample_capture.metadata_json"),
        _encode_jsonb(sample, field_name="sample_capture"),
        _encode_jsonb(fixture, field_name="replay_fixture"),
        _encode_jsonb(object_refs, field_name="object_version_refs"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )

    conn.execute(
        """
        DELETE FROM object_truth_raw_payload_references
         WHERE sample_id = $1
        """,
        sample_id,
    )
    if payload_rows:
        conn.execute_many(
            """
            INSERT INTO object_truth_raw_payload_references (
                sample_id,
                payload_index,
                external_record_id,
                source_metadata_digest,
                raw_payload_ref,
                raw_payload_hash,
                normalized_payload_hash,
                privacy_classification,
                retention_policy_ref,
                privacy_policy_ref,
                inline_payload_stored,
                reference_digest,
                redacted_preview_digest,
                source_metadata_json,
                redacted_preview_json,
                raw_payload_reference_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15::jsonb, $16::jsonb
            )
            ON CONFLICT (sample_id, payload_index) DO UPDATE SET
                external_record_id = EXCLUDED.external_record_id,
                source_metadata_digest = EXCLUDED.source_metadata_digest,
                raw_payload_ref = EXCLUDED.raw_payload_ref,
                raw_payload_hash = EXCLUDED.raw_payload_hash,
                normalized_payload_hash = EXCLUDED.normalized_payload_hash,
                privacy_classification = EXCLUDED.privacy_classification,
                retention_policy_ref = EXCLUDED.retention_policy_ref,
                privacy_policy_ref = EXCLUDED.privacy_policy_ref,
                inline_payload_stored = EXCLUDED.inline_payload_stored,
                reference_digest = EXCLUDED.reference_digest,
                redacted_preview_digest = EXCLUDED.redacted_preview_digest,
                source_metadata_json = EXCLUDED.source_metadata_json,
                redacted_preview_json = EXCLUDED.redacted_preview_json,
                raw_payload_reference_json = EXCLUDED.raw_payload_reference_json
            """,
            [
                (
                    sample_id,
                    int(item.get("payload_index") or index),
                    _optional_text(item.get("external_record_id"), field_name="external_record_id"),
                    _require_text(
                        item.get("source_metadata_digest"),
                        field_name="payload_reference.source_metadata_digest",
                    ),
                    _optional_text(item.get("raw_payload_ref"), field_name="payload_reference.raw_payload_ref"),
                    _optional_text(item.get("raw_payload_hash"), field_name="payload_reference.raw_payload_hash"),
                    _optional_text(
                        item.get("normalized_payload_hash"),
                        field_name="payload_reference.normalized_payload_hash",
                    ),
                    _require_text(
                        item.get("privacy_classification"),
                        field_name="payload_reference.privacy_classification",
                    ),
                    _optional_text(
                        item.get("retention_policy_ref"),
                        field_name="payload_reference.retention_policy_ref",
                    ),
                    _optional_text(
                        item.get("privacy_policy_ref"),
                        field_name="payload_reference.privacy_policy_ref",
                    ),
                    bool(item.get("inline_payload_stored", False)),
                    _require_text(item.get("reference_digest"), field_name="payload_reference.reference_digest"),
                    _optional_text(
                        item.get("redacted_preview_digest"),
                        field_name="payload_reference.redacted_preview_digest",
                    ),
                    _encode_jsonb(item.get("source_metadata_json") or {}, field_name="source_metadata_json"),
                    _encode_jsonb(item.get("redacted_preview_json") or {}, field_name="redacted_preview_json"),
                    _encode_jsonb(
                        item.get("raw_payload_reference_json") or {},
                        field_name="raw_payload_reference_json",
                    ),
                )
                for index, item in enumerate(payload_rows)
            ],
        )

    return {
        "system_snapshot": _normalize_row(snapshot_row, operation="persist_ingestion_sample.snapshot"),
        "sample_capture": _normalize_row(sample_row, operation="persist_ingestion_sample.sample"),
        "payload_reference_count": len(payload_rows),
        "object_version_count": len(object_refs),
    }


def list_ingestion_samples(
    conn: Any,
    *,
    client_ref: str | None = None,
    system_ref: str | None = None,
    object_ref: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            s.client_ref,
            s.integration_id,
            s.connector_ref,
            s.environment_ref,
            s.captured_at,
            c.sample_id,
            c.system_snapshot_id,
            c.sample_capture_digest,
            c.schema_snapshot_digest,
            c.system_ref,
            c.object_ref,
            c.sample_strategy,
            c.sample_size_requested,
            c.sample_size_returned,
            c.sample_hash,
            c.status,
            c.source_evidence_digest,
            c.object_version_refs_json,
            c.created_at,
            c.updated_at
          FROM object_truth_sample_captures c
          JOIN object_truth_system_snapshots s
            ON s.system_snapshot_id = c.system_snapshot_id
         WHERE ($1::text IS NULL OR s.client_ref = $1)
           AND ($2::text IS NULL OR c.system_ref = $2)
           AND ($3::text IS NULL OR c.object_ref = $3)
         ORDER BY c.updated_at DESC, c.created_at DESC
         LIMIT $4
        """,
        _optional_text(client_ref, field_name="client_ref"),
        _optional_text(system_ref, field_name="system_ref"),
        _optional_text(object_ref, field_name="object_ref"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_ingestion_samples")


def load_ingestion_sample(
    conn: Any,
    *,
    sample_id: str,
    include_payload_references: bool = True,
) -> dict[str, Any] | None:
    sample_row = conn.fetchrow(
        """
        SELECT
            c.*,
            s.client_ref,
            s.integration_id,
            s.connector_ref,
            s.environment_ref,
            s.auth_context_hash,
            s.captured_at,
            s.capture_receipt_id,
            s.system_snapshot_digest,
            s.system_snapshot_json
          FROM object_truth_sample_captures c
          JOIN object_truth_system_snapshots s
            ON s.system_snapshot_id = c.system_snapshot_id
         WHERE c.sample_id = $1
        """,
        _require_text(sample_id, field_name="sample_id"),
    )
    if sample_row is None:
        return None
    sample = _normalize_row(sample_row, operation="load_ingestion_sample.sample")
    payload_refs: list[dict[str, Any]] = []
    if include_payload_references:
        rows = conn.fetch(
            """
            SELECT
                sample_id,
                payload_index,
                external_record_id,
                source_metadata_digest,
                raw_payload_ref,
                raw_payload_hash,
                normalized_payload_hash,
                privacy_classification,
                retention_policy_ref,
                privacy_policy_ref,
                inline_payload_stored,
                reference_digest,
                redacted_preview_digest,
                source_metadata_json,
                redacted_preview_json,
                raw_payload_reference_json,
                created_at
              FROM object_truth_raw_payload_references
             WHERE sample_id = $1
             ORDER BY payload_index
            """,
            _require_text(sample_id, field_name="sample_id"),
        )
        payload_refs = _normalize_rows(rows, operation="load_ingestion_sample.payload_references")
    sample["payload_references"] = payload_refs
    sample["payload_reference_count"] = len(payload_refs)
    return sample


def persist_mdm_resolution_packet(
    conn: Any,
    *,
    packet: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    packet_record = dict(_require_mapping(packet, field_name="mdm_packet"))
    packet_ref = _require_text(packet_record.get("packet_ref"), field_name="mdm_packet.packet_ref")
    clusters = [dict(_require_mapping(item, field_name="identity_cluster")) for item in packet_record.get("identity_clusters") or []]
    comparisons = [dict(_require_mapping(item, field_name="field_comparison")) for item in packet_record.get("field_comparisons") or []]
    rules = [dict(_require_mapping(item, field_name="normalization_rule")) for item in packet_record.get("normalization_rules") or []]
    authority = [dict(_require_mapping(item, field_name="authority_evidence")) for item in packet_record.get("authority_evidence") or []]
    hierarchy = [dict(_require_mapping(item, field_name="hierarchy_signal")) for item in packet_record.get("hierarchy_signals") or []]
    gaps = [dict(_require_mapping(item, field_name="typed_gap")) for item in packet_record.get("typed_gaps") or []]

    packet_row = conn.fetchrow(
        """
        INSERT INTO object_truth_mdm_resolution_packets (
            packet_ref,
            resolution_packet_digest,
            client_ref,
            entity_type,
            as_of,
            identity_cluster_count,
            field_comparison_count,
            normalization_rule_count,
            authority_evidence_count,
            hierarchy_signal_count,
            typed_gap_count,
            packet_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14
        )
        ON CONFLICT (packet_ref) DO UPDATE SET
            resolution_packet_digest = EXCLUDED.resolution_packet_digest,
            client_ref = EXCLUDED.client_ref,
            entity_type = EXCLUDED.entity_type,
            as_of = EXCLUDED.as_of,
            identity_cluster_count = EXCLUDED.identity_cluster_count,
            field_comparison_count = EXCLUDED.field_comparison_count,
            normalization_rule_count = EXCLUDED.normalization_rule_count,
            authority_evidence_count = EXCLUDED.authority_evidence_count,
            hierarchy_signal_count = EXCLUDED.hierarchy_signal_count,
            typed_gap_count = EXCLUDED.typed_gap_count,
            packet_json = EXCLUDED.packet_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            packet_ref,
            resolution_packet_digest,
            client_ref,
            entity_type,
            as_of,
            identity_cluster_count,
            field_comparison_count,
            normalization_rule_count,
            authority_evidence_count,
            hierarchy_signal_count,
            typed_gap_count,
            packet_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        packet_ref,
        _require_text(
            packet_record.get("resolution_packet_digest"),
            field_name="mdm_packet.resolution_packet_digest",
        ),
        _require_text(packet_record.get("client_ref"), field_name="mdm_packet.client_ref"),
        _require_text(packet_record.get("entity_type"), field_name="mdm_packet.entity_type"),
        _timestamp(packet_record.get("as_of"), field_name="mdm_packet.as_of"),
        len(clusters),
        len(comparisons),
        len(rules),
        len(authority),
        len(hierarchy),
        len(gaps),
        _encode_jsonb(packet_record, field_name="mdm_packet"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    for table_name in (
        "object_truth_mdm_identity_clusters",
        "object_truth_mdm_field_comparisons",
        "object_truth_mdm_normalization_rules",
        "object_truth_mdm_source_authority_evidence",
        "object_truth_mdm_hierarchy_signals",
        "object_truth_mdm_typed_gaps",
    ):
        conn.execute(f"DELETE FROM {table_name} WHERE packet_ref = $1", packet_ref)

    if clusters:
        conn.execute_many(
            """
            INSERT INTO object_truth_mdm_identity_clusters (
                packet_ref,
                cluster_id,
                identity_cluster_digest,
                entity_type,
                review_status,
                cluster_confidence,
                member_count,
                cluster_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (packet_ref, cluster_id) DO UPDATE SET
                identity_cluster_digest = EXCLUDED.identity_cluster_digest,
                entity_type = EXCLUDED.entity_type,
                review_status = EXCLUDED.review_status,
                cluster_confidence = EXCLUDED.cluster_confidence,
                member_count = EXCLUDED.member_count,
                cluster_json = EXCLUDED.cluster_json
            """,
            [
                (
                    packet_ref,
                    _require_text(item.get("cluster_id"), field_name="identity_cluster.cluster_id"),
                    _require_text(
                        item.get("identity_cluster_digest"),
                        field_name="identity_cluster.identity_cluster_digest",
                    ),
                    _require_text(item.get("entity_type"), field_name="identity_cluster.entity_type"),
                    _require_text(item.get("review_status"), field_name="identity_cluster.review_status"),
                    float(item.get("cluster_confidence") or 0.0),
                    len(item.get("member_records") or []),
                    _encode_jsonb(item, field_name="identity_cluster"),
                )
                for item in clusters
            ],
        )

    if comparisons:
        conn.execute_many(
            """
            INSERT INTO object_truth_mdm_field_comparisons (
                packet_ref,
                field_comparison_digest,
                cluster_id,
                canonical_record_id,
                canonical_field,
                entity_type,
                selection_state,
                conflict_flag,
                consensus_flag,
                typed_gap_count,
                comparison_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            ON CONFLICT (packet_ref, field_comparison_digest) DO UPDATE SET
                cluster_id = EXCLUDED.cluster_id,
                canonical_record_id = EXCLUDED.canonical_record_id,
                canonical_field = EXCLUDED.canonical_field,
                entity_type = EXCLUDED.entity_type,
                selection_state = EXCLUDED.selection_state,
                conflict_flag = EXCLUDED.conflict_flag,
                consensus_flag = EXCLUDED.consensus_flag,
                typed_gap_count = EXCLUDED.typed_gap_count,
                comparison_json = EXCLUDED.comparison_json
            """,
            [
                (
                    packet_ref,
                    _require_text(
                        item.get("field_comparison_digest"),
                        field_name="field_comparison.field_comparison_digest",
                    ),
                    _optional_text(item.get("cluster_id"), field_name="field_comparison.cluster_id"),
                    _require_text(
                        item.get("canonical_record_id"),
                        field_name="field_comparison.canonical_record_id",
                    ),
                    _require_text(item.get("canonical_field"), field_name="field_comparison.canonical_field"),
                    _require_text(item.get("entity_type"), field_name="field_comparison.entity_type"),
                    _require_text(item.get("selection_state"), field_name="field_comparison.selection_state"),
                    bool(item.get("conflict_flag", False)),
                    bool(item.get("consensus_flag", False)),
                    len(item.get("typed_gaps") or []),
                    _encode_jsonb(item, field_name="field_comparison"),
                )
                for item in comparisons
            ],
        )

    if rules:
        conn.execute_many(
            """
            INSERT INTO object_truth_mdm_normalization_rules (
                packet_ref,
                rule_ref,
                normalization_rule_digest,
                entity_type,
                field_name,
                reversible,
                loss_risk,
                rule_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (packet_ref, rule_ref) DO UPDATE SET
                normalization_rule_digest = EXCLUDED.normalization_rule_digest,
                entity_type = EXCLUDED.entity_type,
                field_name = EXCLUDED.field_name,
                reversible = EXCLUDED.reversible,
                loss_risk = EXCLUDED.loss_risk,
                rule_json = EXCLUDED.rule_json
            """,
            [
                (
                    packet_ref,
                    _require_text(item.get("rule_ref"), field_name="normalization_rule.rule_ref"),
                    _require_text(
                        item.get("normalization_rule_digest"),
                        field_name="normalization_rule.normalization_rule_digest",
                    ),
                    _require_text(item.get("entity_type"), field_name="normalization_rule.entity_type"),
                    _require_text(item.get("field_name"), field_name="normalization_rule.field_name"),
                    bool(item.get("reversible", False)),
                    _require_text(item.get("loss_risk"), field_name="normalization_rule.loss_risk"),
                    _encode_jsonb(item, field_name="normalization_rule"),
                )
                for item in rules
            ],
        )

    if authority:
        conn.execute_many(
            """
            INSERT INTO object_truth_mdm_source_authority_evidence (
                packet_ref,
                authority_evidence_digest,
                entity_type,
                field_name,
                source_system,
                authority_rank,
                evidence_type,
                evidence_reference,
                evidence_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
            ON CONFLICT (packet_ref, authority_evidence_digest) DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                field_name = EXCLUDED.field_name,
                source_system = EXCLUDED.source_system,
                authority_rank = EXCLUDED.authority_rank,
                evidence_type = EXCLUDED.evidence_type,
                evidence_reference = EXCLUDED.evidence_reference,
                evidence_json = EXCLUDED.evidence_json
            """,
            [
                (
                    packet_ref,
                    _require_text(
                        item.get("authority_evidence_digest"),
                        field_name="authority_evidence.authority_evidence_digest",
                    ),
                    _require_text(item.get("entity_type"), field_name="authority_evidence.entity_type"),
                    _require_text(item.get("field_name"), field_name="authority_evidence.field_name"),
                    _require_text(item.get("source_system"), field_name="authority_evidence.source_system"),
                    int(item.get("authority_rank") or 0),
                    _require_text(item.get("evidence_type"), field_name="authority_evidence.evidence_type"),
                    _require_text(
                        item.get("evidence_reference"),
                        field_name="authority_evidence.evidence_reference",
                    ),
                    _encode_jsonb(item, field_name="authority_evidence"),
                )
                for item in authority
            ],
        )

    if hierarchy:
        conn.execute_many(
            """
            INSERT INTO object_truth_mdm_hierarchy_signals (
                packet_ref,
                hierarchy_signal_digest,
                entity_type,
                signal_type,
                source_system,
                source_record_id,
                authoritative,
                hierarchy_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (packet_ref, hierarchy_signal_digest) DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                signal_type = EXCLUDED.signal_type,
                source_system = EXCLUDED.source_system,
                source_record_id = EXCLUDED.source_record_id,
                authoritative = EXCLUDED.authoritative,
                hierarchy_json = EXCLUDED.hierarchy_json
            """,
            [
                (
                    packet_ref,
                    _require_text(
                        item.get("hierarchy_signal_digest"),
                        field_name="hierarchy_signal.hierarchy_signal_digest",
                    ),
                    _require_text(item.get("entity_type"), field_name="hierarchy_signal.entity_type"),
                    _require_text(item.get("signal_type"), field_name="hierarchy_signal.signal_type"),
                    _require_text(item.get("source_system"), field_name="hierarchy_signal.source_system"),
                    _require_text(item.get("source_record_id"), field_name="hierarchy_signal.source_record_id"),
                    bool(item.get("authoritative", False)),
                    _encode_jsonb(item, field_name="hierarchy_signal"),
                )
                for item in hierarchy
            ],
        )

    if gaps:
        conn.execute_many(
            """
            INSERT INTO object_truth_mdm_typed_gaps (
                packet_ref,
                gap_id,
                gap_digest,
                entity_type,
                field_name,
                gap_type,
                severity,
                gap_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (packet_ref, gap_id) DO UPDATE SET
                gap_digest = EXCLUDED.gap_digest,
                entity_type = EXCLUDED.entity_type,
                field_name = EXCLUDED.field_name,
                gap_type = EXCLUDED.gap_type,
                severity = EXCLUDED.severity,
                gap_json = EXCLUDED.gap_json
            """,
            [
                (
                    packet_ref,
                    _require_text(item.get("gap_id"), field_name="typed_gap.gap_id"),
                    _require_text(item.get("gap_digest"), field_name="typed_gap.gap_digest"),
                    _require_text(item.get("entity_type"), field_name="typed_gap.entity_type"),
                    _require_text(item.get("field_name"), field_name="typed_gap.field_name"),
                    _require_text(item.get("gap_type"), field_name="typed_gap.gap_type"),
                    _require_text(item.get("severity"), field_name="typed_gap.severity"),
                    _encode_jsonb(item, field_name="typed_gap"),
                )
                for item in gaps
            ],
        )

    return {
        "packet": _normalize_row(packet_row, operation="persist_mdm_resolution_packet.packet"),
        "identity_cluster_count": len(clusters),
        "field_comparison_count": len(comparisons),
        "normalization_rule_count": len(rules),
        "authority_evidence_count": len(authority),
        "hierarchy_signal_count": len(hierarchy),
        "typed_gap_count": len(gaps),
    }


def list_mdm_resolution_packets(
    conn: Any,
    *,
    client_ref: str | None = None,
    entity_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            packet_ref,
            resolution_packet_digest,
            client_ref,
            entity_type,
            as_of,
            identity_cluster_count,
            field_comparison_count,
            normalization_rule_count,
            authority_evidence_count,
            hierarchy_signal_count,
            typed_gap_count,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM object_truth_mdm_resolution_packets
         WHERE ($1::text IS NULL OR client_ref = $1)
           AND ($2::text IS NULL OR entity_type = $2)
         ORDER BY as_of DESC, updated_at DESC
         LIMIT $3
        """,
        _optional_text(client_ref, field_name="client_ref"),
        _optional_text(entity_type, field_name="entity_type"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_mdm_resolution_packets")


def load_mdm_resolution_packet(
    conn: Any,
    *,
    packet_ref: str,
    include_records: bool = True,
) -> dict[str, Any] | None:
    packet_row = conn.fetchrow(
        """
        SELECT *
          FROM object_truth_mdm_resolution_packets
         WHERE packet_ref = $1
        """,
        _require_text(packet_ref, field_name="packet_ref"),
    )
    if packet_row is None:
        return None
    packet = _normalize_row(packet_row, operation="load_mdm_resolution_packet.packet")
    if not include_records:
        return packet
    child_specs = {
        "identity_clusters": ("object_truth_mdm_identity_clusters", "cluster_json", "cluster_id"),
        "field_comparisons": ("object_truth_mdm_field_comparisons", "comparison_json", "canonical_field"),
        "normalization_rules": ("object_truth_mdm_normalization_rules", "rule_json", "rule_ref"),
        "authority_evidence": ("object_truth_mdm_source_authority_evidence", "evidence_json", "authority_rank"),
        "hierarchy_signals": ("object_truth_mdm_hierarchy_signals", "hierarchy_json", "signal_type"),
        "typed_gaps": ("object_truth_mdm_typed_gaps", "gap_json", "gap_id"),
    }
    for key, (table_name, json_column, order_column) in child_specs.items():
        rows = conn.fetch(
            f"""
            SELECT {json_column}
              FROM {table_name}
             WHERE packet_ref = $1
             ORDER BY {order_column}
            """,
            _require_text(packet_ref, field_name="packet_ref"),
        )
        records = _normalize_rows(rows, operation=f"load_mdm_resolution_packet.{key}")
        packet[key] = [item.get(json_column) for item in records if item.get(json_column) is not None]
    return packet


__all__ = [
    "inspect_readiness",
    "list_ingestion_samples",
    "list_mdm_resolution_packets",
    "load_ingestion_sample",
    "load_mdm_resolution_packet",
    "load_latest_object_truth_version",
    "load_object_version",
    "persist_mdm_resolution_packet",
    "persist_ingestion_sample",
    "persist_comparison_run",
    "persist_object_version",
    "persist_schema_snapshot",
]
