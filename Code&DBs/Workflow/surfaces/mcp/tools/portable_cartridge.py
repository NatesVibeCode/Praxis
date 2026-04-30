"""Tools: praxis_authority_portable_cartridge_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_authority_portable_cartridge_record(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Record a portable cartridge deployment contract through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Recording portable cartridge contract",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority.portable_cartridge.record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - portable cartridge record {status}",
        )
    return result


def tool_praxis_authority_portable_cartridge_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read portable cartridge deployment contracts through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading portable cartridge contracts",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority.portable_cartridge.read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - portable cartridge read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_authority_portable_cartridge_record": (
        tool_praxis_authority_portable_cartridge_record,
        {
            "kind": "write",
            "operation_names": ["authority.portable_cartridge.record"],
            "description": (
                "Validate and persist portable cartridge manifests, Object "
                "Truth dependencies, assets, binding contracts, verifier "
                "checks, drift hooks, runtime assumptions, and deployment "
                "readiness through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "manifest": {
                        "type": "object",
                        "description": "Portable cartridge manifest JSON from runtime.cartridge.",
                    },
                    "deployment_mode": {
                        "type": "string",
                        "enum": [
                            "local_verification",
                            "staged_deployment",
                            "production_deployment",
                            "offline_air_gapped",
                        ],
                    },
                    "runtime_capability_profile": {
                        "type": "object",
                        "description": "Optional target runtime capability profile for compatibility proof.",
                    },
                    "binding_values": {
                        "type": "object",
                        "description": "Optional binding_id keyed runtime binding references.",
                    },
                    "cartridge_record_id": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "require_ready": {"type": "boolean"},
                },
                "required": ["manifest"],
            },
            "type_contract": {
                "record_portable_cartridge": {
                    "consumes": [
                        "portable_cartridge.manifest",
                        "portable_cartridge.deployment_mode",
                        "portable_cartridge.runtime_capability_profile",
                        "portable_cartridge.binding_values",
                    ],
                    "produces": [
                        "portable_cartridge.record",
                        "portable_cartridge.deployment_contract",
                        "authority_operation_receipt",
                        "authority_event.portable_cartridge.recorded",
                    ],
                }
            },
        },
    ),
    "praxis_authority_portable_cartridge_read": (
        tool_praxis_authority_portable_cartridge_read,
        {
            "kind": "analytics",
            "operation_names": ["authority.portable_cartridge.read"],
            "description": (
                "Read persisted portable cartridge deployment contract "
                "records, dependencies, assets, binding contracts, verifier "
                "checks, drift hooks, and readiness state through the CQRS "
                "gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list_records",
                            "describe_record",
                            "list_dependencies",
                            "list_assets",
                            "list_bindings",
                            "list_verifiers",
                            "list_drift_hooks",
                        ],
                    },
                    "cartridge_record_id": {"type": "string"},
                    "cartridge_id": {"type": "string"},
                    "readiness_status": {"type": "string"},
                    "deployment_mode": {"type": "string"},
                    "manifest_digest": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "dependency_id": {"type": "string"},
                    "dependency_class": {"type": "string"},
                    "authority_source": {"type": "string"},
                    "asset_role": {"type": "string"},
                    "binding_kind": {"type": "string"},
                    "verifier_category": {"type": "string"},
                    "hook_point": {"type": "string"},
                    "required": {"type": "boolean"},
                    "include_dependencies": {"type": "boolean"},
                    "include_assets": {"type": "boolean"},
                    "include_bindings": {"type": "boolean"},
                    "include_verifiers": {"type": "boolean"},
                    "include_drift_hooks": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
            },
            "type_contract": {
                "read_portable_cartridge": {
                    "consumes": [
                        "portable_cartridge.record_id",
                        "portable_cartridge.dependency_filter",
                        "portable_cartridge.readiness_filter",
                    ],
                    "produces": [
                        "portable_cartridge.records",
                        "portable_cartridge.object_truth_dependencies",
                        "portable_cartridge.binding_contracts",
                        "portable_cartridge.verifier_checks",
                        "portable_cartridge.drift_hooks",
                    ],
                }
            },
        },
    ),
}
