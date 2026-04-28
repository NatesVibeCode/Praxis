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
}
