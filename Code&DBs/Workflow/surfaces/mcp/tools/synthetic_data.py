"""Tools: Synthetic Data authority."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def _payload(params: dict | None) -> dict:
    return {key: value for key, value in dict(params or {}).items() if value is not None}


def tool_praxis_synthetic_data_generate(params: dict, _progress_emitter=None) -> dict:
    """Generate Synthetic Data through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Generating synthetic data")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="synthetic_data_generate",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - synthetic data generate {state}")
    return result


def tool_praxis_synthetic_data_read(params: dict, _progress_emitter=None) -> dict:
    """Read Synthetic Data authority through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Reading synthetic data")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="synthetic_data_read",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - synthetic data read {state}")
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_synthetic_data_generate": (
        tool_praxis_synthetic_data_generate,
        {
            "kind": "write",
            "operation_names": ["synthetic_data_generate"],
            "description": (
                "Generate and persist a deterministic Synthetic Data dataset with "
                "stable record refs, stable name refs, an explicit naming plan, "
                "reserved-term checks, collision gates, schema contract, privacy "
                "posture, and quality report. Synthetic Data can seed Workflow "
                "Context and Virtual Lab but cannot become Object Truth evidence."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["intent"],
                "properties": {
                    "intent": {"type": "string"},
                    "namespace": {"type": "string"},
                    "workflow_ref": {"type": "string"},
                    "source_context_ref": {"type": "string"},
                    "source_object_truth_refs": {"type": "array", "items": {"type": "string"}},
                    "scenario_pack_refs": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "crm_sync",
                                "duplicate_merge",
                                "renewal_risk",
                                "support_escalation",
                                "invoice_failure",
                                "permission_denied",
                                "stale_import",
                                "webhook_storm",
                                "slack_approval",
                            ],
                        },
                    },
                    "object_counts": {"type": "object"},
                    "records_per_object": {"type": "integer", "minimum": 1, "maximum": 50000},
                    "seed": {"type": "string"},
                    "domain_pack": {
                        "type": "string",
                        "enum": ["saas_b2b", "support_ops", "finance_ops", "healthcare_ops", "logistics_ops"],
                    },
                    "locale_ref": {"type": "string"},
                    "uniqueness_scope": {"type": "string"},
                    "privacy_mode": {
                        "type": "string",
                        "enum": ["synthetic_only", "schema_only", "anonymized_operational_seeded"],
                    },
                    "reserved_terms": {"type": "array", "items": {"type": "string"}},
                    "metadata": {"type": "object"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-data-generate",
                "when_to_use": (
                    "Use when a workflow, Virtual Lab run, demo, test fixture, "
                    "or model-eval fixture needs generated data with durable "
                    "records and a quality-checked naming plan."
                ),
                "when_not_to_use": (
                    "Do not use as Object Truth evidence or as a live client "
                    "system read. Bind or promote only through verified evidence."
                ),
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Generate renewal-risk data",
                        "input": {
                            "intent": "Renewal risk demo data for CRM, billing, support, and Slack.",
                            "namespace": "renewal-risk-demo",
                            "scenario_pack_refs": ["renewal_risk"],
                            "object_counts": {"Account": 1000, "Ticket": 1000, "Subscription": 1000},
                            "seed": "renewal-risk-demo-v1",
                            "reserved_terms": ["Acme", "Praxis"],
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_data_generate": {
                    "consumes": [
                        "workflow.intent",
                        "workflow_context.context_ref",
                        "object_truth.ref",
                        "synthetic_data.naming_plan",
                    ],
                    "produces": [
                        "synthetic_data.dataset",
                        "synthetic_data.record",
                        "synthetic_data.name_plan",
                        "synthetic_data.quality_report",
                        "authority_operation_receipt",
                        "authority_event.synthetic_data.generated",
                    ],
                }
            },
        },
    ),
    "praxis_synthetic_data_read": (
        tool_praxis_synthetic_data_read,
        {
            "kind": "analytics",
            "operation_names": ["synthetic_data_read"],
            "description": (
                "Read Synthetic Data datasets, records, naming plans, schema "
                "contracts, privacy posture, and quality reports through the "
                "CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_datasets", "describe_dataset", "list_records"],
                    },
                    "dataset_ref": {"type": "string"},
                    "namespace": {"type": "string"},
                    "source_context_ref": {"type": "string"},
                    "quality_state": {"type": "string"},
                    "object_kind": {"type": "string"},
                    "include_records": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 5000},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-data-read",
                "when_to_use": (
                    "Use to inspect generated datasets, naming plans, quality "
                    "reports, and individual synthetic records."
                ),
                "when_not_to_use": (
                    "Do not use to infer observed client truth; Object Truth owns "
                    "observed evidence."
                ),
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Describe a generated dataset",
                        "input": {
                            "action": "describe_dataset",
                            "dataset_ref": "synthetic_dataset:renewal_risk_demo:abc123",
                            "include_records": True,
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_data_read": {
                    "consumes": ["synthetic_data.dataset_ref", "synthetic_data.namespace"],
                    "produces": [
                        "synthetic_data.dataset",
                        "synthetic_data.record",
                        "synthetic_data.name_plan",
                        "synthetic_data.quality_report",
                    ],
                }
            },
        },
    ),
}


__all__ = [
    "tool_praxis_synthetic_data_generate",
    "tool_praxis_synthetic_data_read",
]
