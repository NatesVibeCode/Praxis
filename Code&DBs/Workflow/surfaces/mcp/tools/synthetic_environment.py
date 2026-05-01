"""Tools: Synthetic Environment authority."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def _payload(params: dict | None) -> dict:
    return {key: value for key, value in dict(params or {}).items() if value is not None}


def _execute(operation_name: str, params: dict, progress_message: str, _progress_emitter=None) -> dict:
    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message=progress_message)
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name=operation_name,
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=1, total=1, message=f"Done - {operation_name} {state}")
    return result


def tool_praxis_synthetic_environment_create(params: dict, _progress_emitter=None) -> dict:
    """Create a Synthetic Environment through CQRS."""

    return _execute(
        "synthetic_environment_create",
        params,
        "Creating synthetic environment",
        _progress_emitter,
    )


def tool_praxis_synthetic_environment_clear(params: dict, _progress_emitter=None) -> dict:
    """Clear a Synthetic Environment through CQRS."""

    return _execute(
        "synthetic_environment_clear",
        params,
        "Clearing synthetic environment",
        _progress_emitter,
    )


def tool_praxis_synthetic_environment_reset(params: dict, _progress_emitter=None) -> dict:
    """Reset a Synthetic Environment through CQRS."""

    return _execute(
        "synthetic_environment_reset",
        params,
        "Resetting synthetic environment",
        _progress_emitter,
    )


def tool_praxis_synthetic_environment_event_inject(params: dict, _progress_emitter=None) -> dict:
    """Inject an outside event into a Synthetic Environment through CQRS."""

    return _execute(
        "synthetic_environment_event_inject",
        params,
        "Injecting synthetic environment event",
        _progress_emitter,
    )


def tool_praxis_synthetic_environment_clock_advance(params: dict, _progress_emitter=None) -> dict:
    """Advance a Synthetic Environment clock through CQRS."""

    return _execute(
        "synthetic_environment_clock_advance",
        params,
        "Advancing synthetic environment clock",
        _progress_emitter,
    )


def tool_praxis_synthetic_environment_read(params: dict, _progress_emitter=None) -> dict:
    """Read Synthetic Environment authority through CQRS."""

    return _execute(
        "synthetic_environment_read",
        params,
        "Reading synthetic environment",
        _progress_emitter,
    )


_ENVIRONMENT_REF = {"type": "string"}
_COMMON_MUTATION_FIELDS = {
    "environment_ref": _ENVIRONMENT_REF,
    "reason": {"type": "string"},
    "actor_ref": {"type": "string"},
    "observed_by_ref": {"type": "string"},
    "source_ref": {"type": "string"},
}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_synthetic_environment_create": (
        tool_praxis_synthetic_environment_create,
        {
            "kind": "write",
            "operation_names": ["synthetic_environment_create"],
            "description": (
                "Create a mutable Synthetic Environment seeded from one Synthetic "
                "Data dataset. The environment keeps seed state, current state, "
                "clock state, permissions, and the first effect receipt."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["dataset_ref"],
                "properties": {
                    "dataset_ref": {"type": "string"},
                    "namespace": {"type": "string"},
                    "environment_ref": {"type": "string"},
                    "seed": {"type": "string"},
                    "clock_time": {"type": "string"},
                    "metadata": {"type": "object"},
                    "max_records": {"type": "integer", "minimum": 1, "maximum": 100000},
                    "actor_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-environment-create",
                "when_to_use": (
                    "Use after generating Synthetic Data when you need a mutable, "
                    "resettable world for outside events, demos, or simulation setup."
                ),
                "when_not_to_use": (
                    "Do not use as observed client truth or as proof of real-world "
                    "system behavior. Virtual Lab and Object Truth own those lanes."
                ),
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Create a renewal-risk environment",
                        "input": {
                            "dataset_ref": "synthetic_dataset:renewal_risk_demo:abc123",
                            "namespace": "renewal-risk-demo",
                            "seed": "renewal-risk-env-v1",
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_environment_create": {
                    "consumes": ["synthetic_data.dataset"],
                    "produces": [
                        "synthetic_environment.environment",
                        "synthetic_environment.effect",
                        "authority_operation_receipt",
                        "authority_event.synthetic_environment.created",
                    ],
                }
            },
        },
    ),
    "praxis_synthetic_environment_clear": (
        tool_praxis_synthetic_environment_clear,
        {
            "kind": "write",
            "operation_names": ["synthetic_environment_clear"],
            "description": (
                "Clear current Synthetic Environment records while preserving the "
                "original seed, operation receipts, and effect history."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["environment_ref"],
                "properties": {
                    "environment_ref": {"type": "string"},
                    "reason": {"type": "string"},
                    "actor_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-environment-clear",
                "when_to_use": "Use when an environment must be emptied without erasing audit history.",
                "when_not_to_use": "Do not use to delete receipts, seed data, or generated datasets.",
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Clear current environment records",
                        "input": {
                            "environment_ref": "synthetic_environment:renewal_risk_demo:abc123",
                            "reason": "Reset demo state before replay.",
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_environment_clear": {
                    "consumes": ["synthetic_environment.environment"],
                    "produces": [
                        "synthetic_environment.effect",
                        "authority_operation_receipt",
                        "authority_event.synthetic_environment.cleared",
                    ],
                }
            },
        },
    ),
    "praxis_synthetic_environment_reset": (
        tool_praxis_synthetic_environment_reset,
        {
            "kind": "write",
            "operation_names": ["synthetic_environment_reset"],
            "description": "Reset a Synthetic Environment back to its seed state with a recorded effect.",
            "inputSchema": {
                "type": "object",
                "required": ["environment_ref"],
                "properties": {
                    "environment_ref": {"type": "string"},
                    "reason": {"type": "string"},
                    "actor_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-environment-reset",
                "when_to_use": "Use to return a mutated or cleared world to its original synthetic seed.",
                "when_not_to_use": "Do not use to regenerate names or replace the source Synthetic Data dataset.",
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Reset environment to seed",
                        "input": {
                            "environment_ref": "synthetic_environment:renewal_risk_demo:abc123",
                            "reason": "Replay from deterministic seed.",
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_environment_reset": {
                    "consumes": ["synthetic_environment.environment"],
                    "produces": [
                        "synthetic_environment.effect",
                        "authority_operation_receipt",
                        "authority_event.synthetic_environment.reset",
                    ],
                }
            },
        },
    ),
    "praxis_synthetic_environment_event_inject": (
        tool_praxis_synthetic_environment_event_inject,
        {
            "kind": "write",
            "operation_names": ["synthetic_environment_event_inject"],
            "description": (
                "Inject a deterministic outside event into a Synthetic Environment "
                "and persist the exact effect on current state."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["environment_ref", "event_type"],
                "properties": {
                    "environment_ref": {"type": "string"},
                    "reason": {"type": "string"},
                    "actor_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "event_type": {
                        "type": "string",
                        "description": "Examples: crm.owner_changed, payment.failed, ticket.escalated, webhook.received.",
                    },
                    "event_payload": {"type": "object"},
                    "target_refs": {"type": "array", "items": {"type": "string"}},
                    "occurred_at": {"type": "string"},
                    "event_ref": {"type": "string"},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-environment-event-inject",
                "when_to_use": (
                    "Use to model external system changes such as owner changes, "
                    "payment failures, escalations, webhook arrivals, or identity merges."
                ),
                "when_not_to_use": "Do not use as a replacement for Virtual Lab consequence proof.",
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Inject an owner-change event",
                        "input": {
                            "environment_ref": "synthetic_environment:renewal_risk_demo:abc123",
                            "event_type": "crm.owner_changed",
                            "event_payload": {"owner_id": "synthetic:user:owner-2"},
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_environment_event_inject": {
                    "consumes": ["synthetic_environment.environment", "synthetic_environment.event"],
                    "produces": [
                        "synthetic_environment.effect",
                        "synthetic_environment.state_diff",
                        "authority_operation_receipt",
                        "authority_event.synthetic_environment.event_injected",
                    ],
                }
            },
        },
    ),
    "praxis_synthetic_environment_clock_advance": (
        tool_praxis_synthetic_environment_clock_advance,
        {
            "kind": "write",
            "operation_names": ["synthetic_environment_clock_advance"],
            "description": "Advance or set a Synthetic Environment clock with a recorded effect.",
            "inputSchema": {
                "type": "object",
                "required": ["environment_ref"],
                "properties": {
                    "environment_ref": {"type": "string"},
                    "reason": {"type": "string"},
                    "actor_ref": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "seconds": {"type": "integer"},
                    "set_time": {"type": "string"},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-environment-clock-advance",
                "when_to_use": "Use to simulate time passing without mutating seed data.",
                "when_not_to_use": "Do not use as a scheduler or workflow run clock.",
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Advance synthetic clock by one day",
                        "input": {
                            "environment_ref": "synthetic_environment:renewal_risk_demo:abc123",
                            "seconds": 86400,
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_environment_clock_advance": {
                    "consumes": ["synthetic_environment.environment"],
                    "produces": [
                        "synthetic_environment.effect",
                        "authority_operation_receipt",
                        "authority_event.synthetic_environment.clock_advanced",
                    ],
                }
            },
        },
    ),
    "praxis_synthetic_environment_read": (
        tool_praxis_synthetic_environment_read,
        {
            "kind": "analytics",
            "operation_names": ["synthetic_environment_read"],
            "description": (
                "Read Synthetic Environments, current state, seed state, effect "
                "ledger, and current-vs-seed diffs through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_environments", "describe_environment", "list_effects", "diff"],
                    },
                    "environment_ref": {"type": "string"},
                    "namespace": {"type": "string"},
                    "source_dataset_ref": {"type": "string"},
                    "lifecycle_state": {"type": "string"},
                    "effect_type": {"type": "string"},
                    "compare_to": {"type": "string"},
                    "include_state": {"type": "boolean"},
                    "include_effects": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 5000},
                },
            },
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "synthetic-environment-read",
                "when_to_use": (
                    "Use to inspect what changed, list effects, or compare current "
                    "environment state back to seed."
                ),
                "when_not_to_use": "Do not use to infer observed client truth.",
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Describe current environment",
                        "input": {
                            "action": "describe_environment",
                            "environment_ref": "synthetic_environment:renewal_risk_demo:abc123",
                            "include_state": True,
                            "include_effects": True,
                        },
                    }
                ],
            },
            "type_contract": {
                "synthetic_environment_read": {
                    "consumes": ["synthetic_environment.environment_ref"],
                    "produces": [
                        "synthetic_environment.environment",
                        "synthetic_environment.effect",
                        "synthetic_environment.state_diff",
                    ],
                }
            },
        },
    ),
}


__all__ = [
    "tool_praxis_synthetic_environment_clear",
    "tool_praxis_synthetic_environment_clock_advance",
    "tool_praxis_synthetic_environment_create",
    "tool_praxis_synthetic_environment_event_inject",
    "tool_praxis_synthetic_environment_read",
    "tool_praxis_synthetic_environment_reset",
]
