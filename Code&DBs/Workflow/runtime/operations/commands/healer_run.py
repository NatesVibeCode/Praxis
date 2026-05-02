"""Gateway-dispatched command wrapper for running a registered healer.

Mirrors the verifier_run command. Healers are the repair side of the
verifier subsystem: a healer is bound to one or more verifiers and runs
on demand to attempt a fix when the verifier fails. The handler delegates
to ``runtime.verifier_authority.run_registered_healer`` which:

1. Resolves the healer (auto-picks if exactly one is bound to the
   verifier, errors otherwise).
2. Runs the healer's action_ref (built-in healers: schema_bootstrap,
   receipt_provenance_backfill, proof_backfill).
3. Re-runs the bound verifier as post-verification.
4. Records the healing_runs row.
5. May promote/resolve a control-plane bug when ``promote_bug`` and the
   run-id chain are intact.

Without this command, the only user-visible path to a healer was the
internal scheduler (``runtime.verifier_authority.run_due_platform_verifications``).
With it, operators and workflow packets can manually trigger a heal and
get a receipt-backed result.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class HealerRunCommand(BaseModel):
    """Input contract for the ``healer.run`` command operation.

    ``verifier_ref`` is required — every healer is invoked in the context
    of a verifier whose result it tries to fix. ``healer_ref`` is
    optional: when omitted, the runtime auto-resolves from the verifier's
    bound healers (errors if zero or multiple are bound).
    """

    verifier_ref: str = Field(
        ...,
        description="Verifier whose failure this heal is trying to repair.",
    )
    healer_ref: str | None = Field(
        default=None,
        description="Specific healer to run. Optional — when omitted, runtime resolves from verifier bindings (errors if 0 or >1 bound).",
    )
    target_kind: Literal["platform", "receipt", "run", "path"] = Field(
        default="platform",
        description="Target kind. Must match what the underlying verifier accepts.",
    )
    target_ref: str = Field(
        default="",
        description="Target reference. Defaults to the verifier_ref's normalized fallback.",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Per-call input overrides merged onto verifier+healer defaults.",
    )
    record_run: bool = Field(
        default=True,
        description="Write the healing_runs row. False = dry-run.",
    )


def handle_healer_run(
    command: HealerRunCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Run the named (or auto-resolved) healer and return the outcome.

    The runtime reruns the bound verifier as post-verification — a
    "succeeded" status here means BOTH the healer action returned
    succeeded AND the post-verification passed.
    """

    from runtime.verifier_authority import run_registered_healer

    outcome = run_registered_healer(
        healer_ref=command.healer_ref,
        verifier_ref=command.verifier_ref,
        inputs=dict(command.inputs or {}),
        target_kind=command.target_kind,
        target_ref=command.target_ref,
        conn=subsystems.get_pg_conn(),
        record_run=command.record_run,
    )
    status = str(outcome.get("status") or "error")
    healing_run_id = outcome.get("healing_run_id")
    return {
        "ok": status == "succeeded",
        "operation": "healer.run.completed",
        "verifier_ref": command.verifier_ref,
        "healer_ref": command.healer_ref or (outcome.get("healer") or {}).get("healer_ref"),
        "healing_run_id": healing_run_id,
        "status": status,
        "target_kind": outcome.get("target_kind"),
        "target_ref": outcome.get("target_ref"),
        "inputs": outcome.get("inputs"),
        "outputs": outcome.get("outputs"),
        "duration_ms": outcome.get("duration_ms"),
        "bug_id": outcome.get("bug_id"),
        "resolved_bug_id": outcome.get("resolved_bug_id"),
        "event_payload": {
            "verifier_ref": command.verifier_ref,
            "healer_ref": command.healer_ref or (outcome.get("healer") or {}).get("healer_ref"),
            "healing_run_id": healing_run_id,
            "status": status,
            "target_kind": outcome.get("target_kind"),
            "target_ref": outcome.get("target_ref"),
            "duration_ms": outcome.get("duration_ms"),
            "bug_id": outcome.get("bug_id"),
            "resolved_bug_id": outcome.get("resolved_bug_id"),
            "succeeded": status == "succeeded",
        },
    }


__all__ = [
    "HealerRunCommand",
    "handle_healer_run",
]
