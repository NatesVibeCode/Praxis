"""Gateway-dispatched command: register a new healer authority ref.

Mirror of verifier_register. Adds a healer_registry row through a
receipt-backed gateway dispatch — no migration needed for new healers.

Healers always have executor_kind='builtin' today (the registry's CHECK
constraint enforces it; new executor kinds would need a schema change
first). action_ref names a built-in handler from
runtime.verifier_builtins.run_builtin_healer.

auto_mode controls when the runtime auto-fires this healer:
- 'manual'    — only when explicitly invoked (default — safest)
- 'assisted'  — fires after operator review of the bug it's bound to
- 'automatic' — fires on every bound-verifier failure (use sparingly)

safety_mode is a coarse risk classifier:
- 'guarded'   — heal can be replayed safely; no destructive side effects
- 'unsafe'    — heal may be destructive; requires explicit operator gate
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class HealerRegisterCommand(BaseModel):
    """Input contract for the ``healer.register`` command operation.

    Idempotent at the storage layer (ON CONFLICT (healer_ref) DO UPDATE).
    """

    healer_ref: str = Field(..., description="Stable id, e.g. 'healer.platform.foo'.")
    display_name: str = Field(..., description="Human-readable name shown in catalog reads.")
    description: str = Field(default="", description="What this healer repairs.")
    executor_kind: Literal["builtin"] = Field(
        default="builtin",
        description="Executor backend. Only 'builtin' is supported today (registry CHECK).",
    )
    action_ref: str = Field(
        ...,
        description="Built-in handler ref from runtime.verifier_builtins (e.g. 'heal_schema_bootstrap').",
    )
    auto_mode: Literal["manual", "assisted", "automatic"] = Field(
        default="manual",
        description="When the runtime auto-fires this healer.",
    )
    safety_mode: Literal["guarded", "unsafe"] = Field(
        default="guarded",
        description="Risk classifier for replay safety.",
    )
    enabled: bool = Field(default=True, description="Whether the runtime should execute this healer.")
    decision_ref: str = Field(..., description="Operator decision ref anchoring why this healer exists.")


def handle_healer_register(
    command: HealerRegisterCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Upsert a healer_registry row.

    Returns ``ok=True`` plus the resolved healer_ref and effective auto_mode/safety_mode.
    """

    conn = subsystems.get_pg_conn()
    conn.execute(
        """
        INSERT INTO healer_registry (
            healer_ref, display_name, description, executor_kind,
            action_ref, auto_mode, safety_mode, enabled, decision_ref
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (healer_ref) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            executor_kind = EXCLUDED.executor_kind,
            action_ref = EXCLUDED.action_ref,
            auto_mode = EXCLUDED.auto_mode,
            safety_mode = EXCLUDED.safety_mode,
            enabled = EXCLUDED.enabled,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        """,
        command.healer_ref,
        command.display_name,
        command.description or "",
        command.executor_kind,
        command.action_ref,
        command.auto_mode,
        command.safety_mode,
        bool(command.enabled),
        command.decision_ref,
    )
    return {
        "ok": True,
        "operation": "healer.registered",
        "healer_ref": command.healer_ref,
        "executor_kind": command.executor_kind,
        "auto_mode": command.auto_mode,
        "safety_mode": command.safety_mode,
        "enabled": bool(command.enabled),
        "event_payload": {
            "healer_ref": command.healer_ref,
            "executor_kind": command.executor_kind,
            "action_ref": command.action_ref,
            "auto_mode": command.auto_mode,
            "safety_mode": command.safety_mode,
            "enabled": bool(command.enabled),
            "decision_ref": command.decision_ref,
        },
    }


__all__ = [
    "HealerRegisterCommand",
    "handle_healer_register",
]
