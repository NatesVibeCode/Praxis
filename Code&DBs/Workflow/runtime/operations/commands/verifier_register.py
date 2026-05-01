"""Gateway-dispatched command: register a new verifier authority ref.

Mirrors the dogfooding pattern from runtime.operations.commands.integration_register
(migration 400) — instead of authoring a SQL migration to add a new
verifier_registry row, an operator or agent calls this command and the
runtime upserts the row through a receipt-backed gateway dispatch.

Per the CHECK constraint on verifier_registry, exactly one of builtin_ref
OR verification_ref must be set, matching verifier_kind:

    verifier_kind='builtin'          → builtin_ref required, verification_ref empty
    verifier_kind='verification_ref' → verification_ref required, builtin_ref empty

Optional bind_healer_refs lets a single call also create the
verifier_healer_bindings rows that bind the new verifier to existing
healers (each healer_ref must already exist in healer_registry).
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class VerifierRegisterCommand(BaseModel):
    """Input contract for the ``verifier.register`` command operation.

    Idempotent at the storage layer (ON CONFLICT (verifier_ref) DO UPDATE)
    so re-calling with the same payload is safe — fields refresh, no
    duplicate row.
    """

    verifier_ref: str = Field(..., description="Stable id, e.g. 'verifier.platform.foo'.")
    display_name: str = Field(..., description="Human-readable name shown in catalog reads.")
    description: str = Field(default="", description="What this verifier checks.")
    verifier_kind: Literal["builtin", "verification_ref"] = Field(
        ...,
        description="Builtin handler vs. verification_registry-backed verifier.",
    )
    builtin_ref: str | None = Field(
        default=None,
        description="Required when verifier_kind='builtin' (e.g. 'verify_schema_authority').",
    )
    verification_ref: str | None = Field(
        default=None,
        description="Required when verifier_kind='verification_ref' (FK to verification_registry).",
    )
    default_inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Default inputs merged onto every run of this verifier.",
    )
    enabled: bool = Field(default=True, description="Whether the runtime should execute this verifier.")
    decision_ref: str = Field(..., description="Operator decision ref anchoring why this verifier exists.")
    bind_healer_refs: list[str] = Field(
        default_factory=list,
        description=(
            "Optional list of existing healer_refs to bind via "
            "verifier_healer_bindings. Each must already exist in healer_registry."
        ),
    )

    @model_validator(mode="after")
    def _check_kind_target(self) -> "VerifierRegisterCommand":
        if self.verifier_kind == "builtin":
            if not (self.builtin_ref or "").strip():
                raise ValueError("verifier_kind='builtin' requires builtin_ref")
            if self.verification_ref:
                raise ValueError("verifier_kind='builtin' must not set verification_ref")
        else:  # 'verification_ref'
            if not (self.verification_ref or "").strip():
                raise ValueError("verifier_kind='verification_ref' requires verification_ref")
            if self.builtin_ref:
                raise ValueError("verifier_kind='verification_ref' must not set builtin_ref")
        return self


def handle_verifier_register(
    command: VerifierRegisterCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Upsert a verifier_registry row and any requested healer bindings.

    Returns ``ok=True`` on success plus the resolved verifier_ref and
    bound_healer_refs the runtime actually wrote.
    """

    import json
    import uuid

    conn = subsystems.get_pg_conn()
    conn.execute(
        """
        INSERT INTO verifier_registry (
            verifier_ref, display_name, description, verifier_kind,
            verification_ref, builtin_ref, default_inputs, enabled, decision_ref
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
        ON CONFLICT (verifier_ref) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            verifier_kind = EXCLUDED.verifier_kind,
            verification_ref = EXCLUDED.verification_ref,
            builtin_ref = EXCLUDED.builtin_ref,
            default_inputs = EXCLUDED.default_inputs,
            enabled = EXCLUDED.enabled,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        """,
        command.verifier_ref,
        command.display_name,
        command.description or "",
        command.verifier_kind,
        command.verification_ref,
        command.builtin_ref,
        json.dumps(command.default_inputs or {}),
        bool(command.enabled),
        command.decision_ref,
    )

    bound: list[str] = []
    for healer_ref in command.bind_healer_refs or []:
        if not healer_ref:
            continue
        binding_ref = f"binding.{command.verifier_ref}.{healer_ref}"
        conn.execute(
            """
            INSERT INTO verifier_healer_bindings (
                binding_ref, verifier_ref, healer_ref, enabled,
                binding_revision, decision_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (verifier_ref, healer_ref) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                binding_revision = EXCLUDED.binding_revision,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            binding_ref,
            command.verifier_ref,
            healer_ref,
            True,
            f"binding.{uuid.uuid4().hex[:12]}",
            command.decision_ref,
        )
        bound.append(healer_ref)

    return {
        "ok": True,
        "operation": "verifier.registered",
        "verifier_ref": command.verifier_ref,
        "verifier_kind": command.verifier_kind,
        "enabled": bool(command.enabled),
        "bound_healer_refs": bound,
        "event_payload": {
            "verifier_ref": command.verifier_ref,
            "verifier_kind": command.verifier_kind,
            "enabled": bool(command.enabled),
            "bound_healer_refs": list(bound),
            "decision_ref": command.decision_ref,
        },
    }


__all__ = [
    "VerifierRegisterCommand",
    "handle_verifier_register",
]
