"""Tools: praxis_object_truth."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_object_truth(params: dict, _progress_emitter=None) -> dict:
    """Build deterministic object-truth evidence for one inline record."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Observing object truth for {payload.get('system_ref') or '?'}:{payload.get('object_ref') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_observe_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth observe {status}",
        )
    return result


def tool_praxis_object_truth_store(params: dict, _progress_emitter=None) -> dict:
    """Build and persist deterministic object-truth evidence for one inline record."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Storing object truth for {payload.get('system_ref') or '?'}:{payload.get('object_ref') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_store_observed_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth store {status}",
        )
    return result


def tool_praxis_object_truth_store_schema_snapshot(params: dict, _progress_emitter=None) -> dict:
    """Normalize and persist deterministic schema-snapshot evidence."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Storing schema snapshot for {payload.get('system_ref') or '?'}:{payload.get('object_ref') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_store_schema_snapshot",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth schema store {status}",
        )
    return result


def tool_praxis_object_truth_compare_versions(params: dict, _progress_emitter=None) -> dict:
    """Compare two persisted object-truth object versions."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Comparing persisted object-truth versions",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_compare_versions",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth compare {status}",
        )
    return result


def tool_praxis_object_truth_readiness(params: dict, _progress_emitter=None) -> dict:
    """Inspect whether Object Truth authority is ready for downstream work."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Inspecting object-truth readiness",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_readiness",
        payload=payload,
    )
    if _progress_emitter:
        state = result.get("state") or ("ready" if result.get("ok") else "failed")
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth readiness {state}",
        )
    return result


def tool_praxis_object_truth_record_comparison_run(params: dict, _progress_emitter=None) -> dict:
    """Compare two stored object versions and persist the comparison run."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Recording object-truth comparison run",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_record_comparison_run",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth comparison run {status}",
        )
    return result


def tool_praxis_object_truth_ingestion_sample_record(params: dict, _progress_emitter=None) -> dict:
    """Record one Object Truth ingestion sample packet."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Recording ingestion sample for {payload.get('system_ref') or '?'}:{payload.get('object_ref') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_ingestion_sample_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth ingestion sample record {status}",
        )
    return result


def tool_praxis_object_truth_ingestion_sample_read(params: dict, _progress_emitter=None) -> dict:
    """Read Object Truth ingestion sample evidence."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading object-truth ingestion sample evidence",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_ingestion_sample_read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - object truth ingestion sample read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_object_truth": (
        tool_praxis_object_truth,
        {
            "kind": "analytics",
            "operation_names": ["object_truth_observe_record"],
            "description": (
                "Build deterministic object-truth evidence for one inline record. This "
                "is a thin read-only MCP wrapper over the gateway operation "
                "`object_truth_observe_record`; it normalizes identity, field "
                "observations, value digests, source metadata, hierarchy signals, "
                "and redaction-safe previews without deciding business truth.\n\n"
                "USE WHEN: you need to inspect one external object payload before "
                "sampling or comparing across systems. For durable domain creation, "
                "use praxis_authority_domain_forge/register first. For multi-system "
                "sampling or writes, wait for object-truth persistence operations."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "system_ref",
                    "object_ref",
                    "record",
                    "identity_fields",
                ],
                "properties": {
                    "system_ref": {
                        "type": "string",
                        "description": "External system or integration reference, e.g. salesforce.",
                    },
                    "object_ref": {
                        "type": "string",
                        "description": "External object reference inside the system, e.g. account.",
                    },
                    "record": {
                        "type": "object",
                        "description": "Inline JSON object to observe.",
                    },
                    "identity_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Required field paths that identify the object.",
                    },
                    "source_metadata": {
                        "type": "object",
                        "description": "Optional source metadata such as updated_at, source, owner, or cursor.",
                    },
                    "schema_snapshot_digest": {
                        "type": "string",
                        "description": "Optional digest for the schema snapshot this record was observed against.",
                    },
                },
            },
            "type_contract": {
                "observe_record": {
                    "consumes": [
                        "object_truth.system_ref",
                        "object_truth.object_ref",
                        "object_truth.record",
                        "object_truth.identity_fields",
                    ],
                    "produces": [
                        "object_truth.object_version",
                        "object_truth.field_observations",
                        "object_truth.hierarchy_signals",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_store": (
        tool_praxis_object_truth_store,
        {
            "kind": "write",
            "operation_names": ["object_truth_store_observed_record"],
            "description": (
                "Build and persist deterministic object-truth evidence for one inline "
                "record. This is a thin write MCP wrapper over the gateway command "
                "`object_truth_store_observed_record`; it creates durable object-version "
                "and field-observation evidence, plus the command receipt/event."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "system_ref",
                    "object_ref",
                    "record",
                    "identity_fields",
                ],
                "properties": {
                    "system_ref": {
                        "type": "string",
                        "description": "External system or integration reference, e.g. salesforce.",
                    },
                    "object_ref": {
                        "type": "string",
                        "description": "External object reference inside the system, e.g. account.",
                    },
                    "record": {
                        "type": "object",
                        "description": "Inline JSON object to persist.",
                    },
                    "identity_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Required field paths that identify the object.",
                    },
                    "source_metadata": {
                        "type": "object",
                        "description": "Optional source metadata such as updated_at, source, owner, or cursor.",
                    },
                    "schema_snapshot_digest": {
                        "type": "string",
                        "description": "Optional digest for the schema snapshot this record was observed against.",
                    },
                    "observed_by_ref": {
                        "type": "string",
                        "description": "Optional actor/operator/process ref responsible for this observation.",
                    },
                    "source_ref": {
                        "type": "string",
                        "description": "Optional sample, connector, cursor, or extraction batch ref.",
                    },
                },
            },
            "type_contract": {
                "store_observed_record": {
                    "consumes": [
                        "object_truth.system_ref",
                        "object_truth.object_ref",
                        "object_truth.record",
                        "object_truth.identity_fields",
                    ],
                    "produces": [
                        "object_truth.object_version_ref",
                        "object_truth.object_version_digest",
                        "object_truth.field_observation_count",
                        "authority_operation_receipt",
                        "authority_event.object_truth.object_version_stored",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_store_schema_snapshot": (
        tool_praxis_object_truth_store_schema_snapshot,
        {
            "kind": "write",
            "operation_names": ["object_truth_store_schema_snapshot"],
            "description": (
                "Normalize and persist deterministic schema-snapshot evidence for one "
                "external object. This is a thin write MCP wrapper over the gateway "
                "command `object_truth_store_schema_snapshot`."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "system_ref",
                    "object_ref",
                    "raw_schema",
                ],
                "properties": {
                    "system_ref": {
                        "type": "string",
                        "description": "External system or integration reference, e.g. salesforce.",
                    },
                    "object_ref": {
                        "type": "string",
                        "description": "External object reference inside the system, e.g. account.",
                    },
                    "raw_schema": {
                        "description": "External schema object or list of field objects to normalize and persist.",
                    },
                    "observed_by_ref": {
                        "type": "string",
                        "description": "Optional actor/operator/process ref responsible for this observation.",
                    },
                    "source_ref": {
                        "type": "string",
                        "description": "Optional schema, connector, cursor, or extraction batch ref.",
                    },
                },
            },
            "type_contract": {
                "store_schema_snapshot": {
                    "consumes": [
                        "object_truth.system_ref",
                        "object_truth.object_ref",
                        "object_truth.raw_schema",
                    ],
                    "produces": [
                        "object_truth.schema_snapshot_ref",
                        "object_truth.schema_snapshot_digest",
                        "object_truth.schema_fields",
                        "authority_operation_receipt",
                        "authority_event.object_truth.schema_snapshot_stored",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_compare_versions": (
        tool_praxis_object_truth_compare_versions,
        {
            "kind": "analytics",
            "operation_names": ["object_truth_compare_versions"],
            "description": (
                "Compare two persisted object-truth object versions by digest. This is "
                "a thin read-only MCP wrapper over the gateway query "
                "`object_truth_compare_versions`; it compares field observations and "
                "freshness hints without deciding business truth."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "left_object_version_digest",
                    "right_object_version_digest",
                ],
                "properties": {
                    "left_object_version_digest": {
                        "type": "string",
                        "description": "Digest of the left stored object version.",
                    },
                    "right_object_version_digest": {
                        "type": "string",
                        "description": "Digest of the right stored object version.",
                    },
                },
            },
            "type_contract": {
                "compare_versions": {
                    "consumes": [
                        "object_truth.left_object_version_digest",
                        "object_truth.right_object_version_digest",
                    ],
                    "produces": [
                        "object_truth.object_version_comparison",
                        "object_truth.field_comparisons",
                        "object_truth.freshness_comparison",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_readiness": (
        tool_praxis_object_truth_readiness,
        {
            "kind": "analytics",
            "operation_names": ["object_truth_readiness"],
            "description": (
                "Inspect whether Object Truth authority is ready for downstream "
                "client-system discovery, ingestion, and Virtual Lab planning. "
                "This is a thin read-only MCP wrapper over the gateway query "
                "`object_truth_readiness`; blocked readiness is returned as a "
                "query result with explicit no-go conditions."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "client_payload_mode": {
                        "type": "string",
                        "enum": ["redacted_hashes", "raw_client_payloads"],
                        "description": "Expected client payload handling mode for downstream work.",
                    },
                    "privacy_policy_ref": {
                        "type": "string",
                        "description": "Required when downstream work expects raw client payloads.",
                    },
                    "planned_fanout": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Number of downstream jobs expected after this gate.",
                    },
                    "include_counts": {
                        "type": "boolean",
                        "description": "When true, include current Object Truth evidence row counts.",
                    },
                },
            },
            "type_contract": {
                "readiness": {
                    "consumes": [
                        "object_truth.client_payload_mode",
                        "object_truth.privacy_policy_ref",
                        "workflow.planned_fanout",
                    ],
                    "produces": [
                        "object_truth.readiness_state",
                        "object_truth.readiness_gates",
                        "object_truth.no_go_conditions",
                        "object_truth.privacy_posture",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_record_comparison_run": (
        tool_praxis_object_truth_record_comparison_run,
        {
            "kind": "write",
            "operation_names": ["object_truth_record_comparison_run"],
            "description": (
                "Compare two persisted object-truth object versions and store the "
                "comparison output as durable evidence. This is a thin write MCP "
                "wrapper over the gateway command `object_truth_record_comparison_run`."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "left_object_version_digest",
                    "right_object_version_digest",
                ],
                "properties": {
                    "left_object_version_digest": {
                        "type": "string",
                        "description": "Digest of the left stored object version.",
                    },
                    "right_object_version_digest": {
                        "type": "string",
                        "description": "Digest of the right stored object version.",
                    },
                    "observed_by_ref": {
                        "type": "string",
                        "description": "Optional actor/operator/process ref responsible for this comparison.",
                    },
                    "source_ref": {
                        "type": "string",
                        "description": "Optional comparison batch or evidence source ref.",
                    },
                },
            },
            "type_contract": {
                "record_comparison_run": {
                    "consumes": [
                        "object_truth.left_object_version_digest",
                        "object_truth.right_object_version_digest",
                    ],
                    "produces": [
                        "object_truth.comparison_run_ref",
                        "object_truth.comparison_run_digest",
                        "object_truth.object_version_comparison",
                        "authority_operation_receipt",
                        "authority_event.object_truth.comparison_run_recorded",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_ingestion_sample_record": (
        tool_praxis_object_truth_ingestion_sample_record,
        {
            "kind": "write",
            "operation_names": ["object_truth_ingestion_sample_record"],
            "description": (
                "Record receipt-backed Object Truth ingestion sample evidence. This "
                "thin MCP wrapper dispatches to the gateway command "
                "`object_truth_ingestion_sample_record`; it persists a system "
                "snapshot, source-query evidence, sample capture, redacted payload "
                "previews, raw payload references, object versions, field "
                "observations, and replay fixture evidence."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "client_ref",
                    "system_ref",
                    "integration_id",
                    "connector_ref",
                    "environment_ref",
                    "object_ref",
                    "schema_snapshot_digest",
                    "captured_at",
                    "capture_receipt_id",
                    "identity_fields",
                    "sample_payloads",
                ],
                "properties": {
                    "client_ref": {"type": "string"},
                    "system_ref": {"type": "string"},
                    "integration_id": {"type": "string"},
                    "connector_ref": {"type": "string"},
                    "environment_ref": {"type": "string"},
                    "object_ref": {"type": "string"},
                    "schema_snapshot_digest": {"type": "string"},
                    "captured_at": {"type": "string"},
                    "capture_receipt_id": {"type": "string"},
                    "identity_fields": {"type": "array", "items": {"type": "string"}},
                    "sample_payloads": {"type": "array", "items": {"type": "object"}},
                    "sample_payload_refs": {"type": "array", "items": {"type": "string"}},
                    "sample_strategy": {
                        "type": "string",
                        "enum": [
                            "recent",
                            "claimed_source_truth",
                            "matching_ids",
                            "random_window",
                            "operator_supplied",
                            "fixture",
                        ],
                    },
                    "source_query": {"type": "object"},
                    "cursor_ref": {"type": "string"},
                    "cursor_value": {},
                    "window_kind": {"type": "string"},
                    "window_start": {"type": "string"},
                    "window_end": {"type": "string"},
                    "limit": {"type": "integer"},
                    "sample_size_requested": {"type": "integer"},
                    "auth_context_hash": {"type": "string"},
                    "auth_context": {},
                    "privacy_classification": {
                        "type": "string",
                        "enum": ["public", "internal", "confidential", "restricted"],
                    },
                    "privacy_policy_ref": {"type": "string"},
                    "retention_policy_ref": {"type": "string"},
                    "preview_policy": {"type": "object"},
                    "metadata": {"type": "object"},
                    "snapshot_metadata": {"type": "object"},
                    "source_metadata": {"type": "object"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "type_contract": {
                "record_ingestion_sample": {
                    "consumes": [
                        "object_truth.client_ref",
                        "object_truth.system_ref",
                        "object_truth.object_ref",
                        "object_truth.sample_payloads",
                        "object_truth.identity_fields",
                    ],
                    "produces": [
                        "object_truth.system_snapshot",
                        "object_truth.sample_capture",
                        "object_truth.raw_payload_references",
                        "object_truth.redacted_previews",
                        "object_truth.object_version_refs",
                        "object_truth.replay_fixture",
                        "authority_operation_receipt",
                        "authority_event.object_truth.ingestion_sample_recorded",
                    ],
                }
            },
        },
    ),
    "praxis_object_truth_ingestion_sample_read": (
        tool_praxis_object_truth_ingestion_sample_read,
        {
            "kind": "analytics",
            "operation_names": ["object_truth_ingestion_sample_read"],
            "description": (
                "Read queryable Object Truth ingestion sample evidence and replay "
                "fixture packets through the gateway query "
                "`object_truth_ingestion_sample_read`."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["list", "describe"]},
                    "client_ref": {"type": "string"},
                    "system_ref": {"type": "string"},
                    "object_ref": {"type": "string"},
                    "sample_id": {"type": "string"},
                    "include_payload_references": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
            },
            "type_contract": {
                "read_ingestion_sample": {
                    "consumes": [
                        "object_truth.client_ref",
                        "object_truth.system_ref",
                        "object_truth.object_ref",
                        "object_truth.sample_id",
                    ],
                    "produces": [
                        "object_truth.sample_captures",
                        "object_truth.raw_payload_references",
                        "object_truth.replay_fixture",
                    ],
                }
            },
        },
    ),
}
