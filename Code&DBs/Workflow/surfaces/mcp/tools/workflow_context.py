"""Tools: Workflow Context authority."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def _payload(params: dict | None) -> dict:
    return {key: value for key, value in dict(params or {}).items() if value is not None}


def tool_praxis_workflow_context_compile(params: dict, _progress_emitter=None) -> dict:
    """Compile inferred or synthetic Workflow Context through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Compiling workflow context")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="workflow_context_compile",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - workflow context compile {state}")
    return result


def tool_praxis_workflow_context_read(params: dict, _progress_emitter=None) -> dict:
    """Read Workflow Context authority through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Reading workflow context")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="workflow_context_read",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - workflow context read {state}")
    return result


def tool_praxis_workflow_context_transition(params: dict, _progress_emitter=None) -> dict:
    """Transition Workflow Context truth state through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Transitioning workflow context")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="workflow_context_transition",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - workflow context transition {state}")
    return result


def tool_praxis_workflow_context_bind(params: dict, _progress_emitter=None) -> dict:
    """Bind Workflow Context entities to authority refs through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Binding workflow context")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="workflow_context_bind",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - workflow context bind {state}")
    return result


def tool_praxis_workflow_context_guardrail_check(params: dict, _progress_emitter=None) -> dict:
    """Read Workflow Context guardrails through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Checking workflow context guardrails")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="workflow_context_guardrail_check",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - workflow context guardrail {state}")
    return result


def tool_praxis_object_truth_latest_version_read(params: dict, _progress_emitter=None) -> dict:
    """Read latest trusted Object Truth version through CQRS."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Reading latest Object Truth version")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="object_truth_latest_version_read",
        payload=payload,
    )
    if _progress_emitter:
        state = result.get("state") or ("ok" if result.get("ok") else "failed")
        _progress_emitter.emit(progress=1, total=1, message=f"Done - Object Truth latest {state}")
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_workflow_context_compile": (
        tool_praxis_workflow_context_compile,
        {
            "kind": "write",
            "operation_names": ["workflow_context_compile"],
            "description": (
                "Compile Workflow Context from intent and optional graph through the CQRS gateway. "
                "It persists inferred assumptions, scenario packs, computed confidence, blockers, "
                "verifier expectations, and optional deterministic synthetic worlds. It does not call "
                "live client systems."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["intent"],
                "properties": {
                    "intent": {"type": "string"},
                    "workflow_ref": {"type": "string"},
                    "graph": {"type": "object"},
                    "context_mode": {
                        "type": "string",
                        "enum": ["standalone", "inferred", "synthetic", "bound", "hybrid"],
                    },
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
                    "seed": {"type": "string"},
                    "source_prompt_ref": {"type": "string"},
                    "evidence": {"type": "array"},
                    "unknown_mutator_risk": {"type": "boolean"},
                    "metadata": {"type": "object"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "type_contract": {
                "workflow_context": {
                    "consumes": ["workflow.intent", "workflow.graph", "workflow.context.evidence"],
                    "produces": [
                        "workflow_context.context_pack",
                        "workflow_context.synthetic_world",
                        "workflow_context.review_packet",
                    ],
                }
            },
        },
    ),
    "praxis_workflow_context_read": (
        tool_praxis_workflow_context_read,
        {
            "kind": "analytics",
            "operation_names": ["workflow_context_read"],
            "description": (
                "Read Workflow Context packs, entities, bindings, transitions, guardrails, and "
                "review packets through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "context_ref": {"type": "string"},
                    "workflow_ref": {"type": "string"},
                    "truth_state": {
                        "type": "string",
                        "enum": [
                            "none",
                            "inferred",
                            "synthetic",
                            "documented",
                            "anonymized_operational",
                            "schema_bound",
                            "observed",
                            "verified",
                            "promoted",
                            "stale",
                            "contradicted",
                            "blocked",
                        ],
                    },
                    "include_entities": {"type": "boolean"},
                    "include_bindings": {"type": "boolean"},
                    "include_transitions": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
            },
        },
    ),
    "praxis_workflow_context_transition": (
        tool_praxis_workflow_context_transition,
        {
            "kind": "write",
            "operation_names": ["workflow_context_transition"],
            "description": (
                "Transition Workflow Context truth state through backend guardrails. Synthetic and "
                "inferred context can continue building, but promotion is blocked unless verified "
                "evidence and risk requirements are satisfied."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["context_ref", "to_truth_state", "transition_reason"],
                "properties": {
                    "context_ref": {"type": "string"},
                    "to_truth_state": {
                        "type": "string",
                        "enum": [
                            "none",
                            "inferred",
                            "synthetic",
                            "documented",
                            "anonymized_operational",
                            "schema_bound",
                            "observed",
                            "verified",
                            "promoted",
                            "stale",
                            "contradicted",
                            "blocked",
                        ],
                    },
                    "transition_reason": {"type": "string"},
                    "evidence": {"type": "array"},
                    "risk_disposition": {"type": "string"},
                    "decision_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
        },
    ),
    "praxis_workflow_context_bind": (
        tool_praxis_workflow_context_bind,
        {
            "kind": "write",
            "operation_names": ["workflow_context_bind"],
            "description": (
                "Bind inferred or synthetic Workflow Context entities to Object Truth or another "
                "authority ref. Context owns the binding record; Object Truth owns evidence."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["context_ref", "entity_ref", "target_ref"],
                "properties": {
                    "context_ref": {"type": "string"},
                    "entity_ref": {"type": "string"},
                    "target_ref": {"type": "string"},
                    "target_authority_domain": {"type": "string"},
                    "evidence": {"type": "array"},
                    "risk_level": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "critical"],
                    },
                    "binding_state": {
                        "type": "string",
                        "enum": ["proposed", "accepted", "rejected", "revoked"],
                    },
                    "reversible": {"type": "boolean"},
                    "reviewed_by_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
        },
    ),
    "praxis_workflow_context_guardrail_check": (
        tool_praxis_workflow_context_guardrail_check,
        {
            "kind": "analytics",
            "operation_names": ["workflow_context_guardrail_check"],
            "description": (
                "Read allowed next LLM actions, review requirements, and no-go states for a "
                "Workflow Context pack through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["context_ref"],
                "properties": {
                    "context_ref": {"type": "string"},
                    "target_truth_state": {
                        "type": "string",
                        "enum": [
                            "none",
                            "inferred",
                            "synthetic",
                            "documented",
                            "anonymized_operational",
                            "schema_bound",
                            "observed",
                            "verified",
                            "promoted",
                            "stale",
                            "contradicted",
                            "blocked",
                        ],
                    },
                    "risk_disposition": {"type": "string"},
                    "requested_action": {"type": "string"},
                },
            },
        },
    ),
    "praxis_object_truth_latest_version_read": (
        tool_praxis_object_truth_latest_version_read,
        {
            "kind": "analytics",
            "operation_names": ["object_truth_latest_version_read"],
            "description": (
                "Read the latest trusted Object Truth object version by system/object/identity/client "
                "filters through the CQRS gateway. Returns freshness, conflicts, and no-go states."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "system_ref": {"type": "string"},
                    "object_ref": {"type": "string"},
                    "identity_digest": {"type": "string"},
                    "client_ref": {"type": "string"},
                    "trusted_only": {"type": "boolean"},
                    "max_age_seconds": {"type": "integer", "minimum": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
            },
        },
    ),
}
