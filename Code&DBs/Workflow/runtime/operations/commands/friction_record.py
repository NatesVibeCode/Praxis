"""Gateway-dispatched command wrapper for recording friction events.

The JIT trigger-check hook (`.claude/hooks/preact_orient_friction.py` and the
sibling Codex / Gemini hooks) records friction evidence at the moment of any
matching agent action. Before this operation existed, those hooks were
shelling into ``praxis_friction --action=record`` — a write smuggled behind
the read-shaped friction tool. The catalog gateway never saw it, so each
firing was a stealth write with no receipt and no authority event, and the
tool itself had no ``record`` branch (silently returned an error the hook
treated as success).

This module is the gateway-friendly seam: a Pydantic input model + a
``(command, subsystems)`` handler that calls
``runtime.friction_ledger.FrictionLedger.record(...)``. Registration in
``operation_catalog_registry`` (migration 332) wires it up as
``operation_kind='command'`` with ``event_required=TRUE`` /
``event_type='friction.recorded'`` so each firing leaves both an
``authority_operation_receipts`` row and an ``authority_events`` row, linked
by ``receipt_id``.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, field_validator


_VALID_FRICTION_TYPES = {"GUARDRAIL_BOUNCE", "WARN_ONLY", "HARD_FAILURE"}


class FrictionRecordInput(BaseModel):
    """Input contract for the ``friction_record`` command operation.

    Mirrors the legacy hook payload so the trigger-check hooks across
    Claude Code / Codex / Gemini map 1:1 onto this model. Optional
    ``message`` lets callers override the default JSON-encoded structured
    payload (subject + matched decisions + harness + metadata) the ledger
    receives. ``task_mode`` is normalized lower-case by the ledger.
    """

    event_type: str = Field(
        ...,
        description="One of GUARDRAIL_BOUNCE, WARN_ONLY, HARD_FAILURE.",
    )
    source: str = Field(..., description="Origin tag, e.g. 'preact_orient_hook'.")
    subject_kind: str | None = Field(
        default=None,
        description="Optional kind label for the subject (e.g. 'agent_action').",
    )
    subject_ref: str | None = Field(
        default=None,
        description="Subject identifier — typically the tool name (Bash, Edit, Read).",
    )
    job_label: str | None = Field(
        default=None,
        description="Override for the friction_events.job_label column. Defaults to subject_ref.",
    )
    decision_keys: list[str] = Field(
        default_factory=list,
        description="Operator-decision keys this firing matched.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form context (subject text, harness, matched_decisions).",
    )
    message: str | None = Field(
        default=None,
        description=(
            "Optional explicit ledger message. When omitted, the handler "
            "JSON-encodes the structured payload so cluster_patterns "
            "fingerprinting still works."
        ),
    )
    task_mode: str | None = Field(
        default=None,
        description="Active task mode (chat / build / release / incident / ...).",
    )
    is_test: bool = Field(
        default=False,
        description="Mark synthetic events recorded by tests.",
    )

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        normalized = (value or "").strip().upper()
        if normalized not in _VALID_FRICTION_TYPES:
            raise ValueError(
                f"event_type must be one of {sorted(_VALID_FRICTION_TYPES)}; got {value!r}"
            )
        return normalized


def _structured_message(command: FrictionRecordInput) -> str:
    payload: dict[str, Any] = {
        "source": command.source,
        "subject_kind": command.subject_kind,
        "subject_ref": command.subject_ref,
        "decision_keys": list(command.decision_keys),
        "metadata": dict(command.metadata or {}),
    }
    if command.task_mode:
        payload["task_mode"] = command.task_mode
    return json.dumps(payload, sort_keys=True, default=str)


def handle_friction_record(
    command: FrictionRecordInput,
    subsystems: Any,
) -> dict[str, Any]:
    """Record a friction event through the canonical ledger.

    Returns ``ok=True`` plus the ledger row identifiers and an
    ``event_payload`` block the gateway hoists onto the
    ``authority_events`` row (per
    architecture-policy::platform-architecture::conceptual-events-register-
    through-operation-catalog-registry).
    """

    from runtime.friction_ledger import FrictionType

    ledger = subsystems.get_friction_ledger()
    job_label = command.job_label or command.subject_ref or command.source
    message = command.message or _structured_message(command)
    event = ledger.record(
        friction_type=FrictionType(command.event_type),
        source=command.source,
        job_label=job_label,
        message=message,
        is_test=command.is_test,
        task_mode=command.task_mode,
    )
    return {
        "ok": True,
        "event_id": event.event_id,
        "friction_type": event.friction_type.value,
        "source": event.source,
        "job_label": event.job_label,
        "task_mode": event.task_mode,
        "timestamp": event.timestamp.isoformat(),
        "event_payload": {
            "event_id": event.event_id,
            "friction_type": event.friction_type.value,
            "source": event.source,
            "job_label": event.job_label,
            "subject_kind": command.subject_kind,
            "subject_ref": command.subject_ref,
            "decision_keys": list(command.decision_keys),
            "task_mode": event.task_mode,
            "is_test": event.is_test,
            "decision_match_count": len(command.decision_keys),
            "ok": True,
        },
    }


__all__ = [
    "FrictionRecordInput",
    "handle_friction_record",
]
