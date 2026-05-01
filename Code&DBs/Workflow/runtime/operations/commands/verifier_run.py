"""Gateway-dispatched command wrapper for running a registered verifier.

The verifier subsystem (``runtime.verifier_authority``) has been internally
complete for some time — `verifier_registry`, `healer_registry`,
`verification_runs`, six built-in verifiers, three healers, full bug-bridge.
What was missing: a first-class write surface that lets workflow packets,
operators, or the LLM-first plan composer **run** a verifier directly,
without going through the bug-resolve flow.

Without this command, the only user-visible path to a verifier run was
``praxis_bugs action=resolve --verifier-ref ...`` (which conflates "I want
to verify X" with "I want to flip a bug to FIXED"). With it, a workflow
packet can declare ``integration_id=praxis_verifier_run, integration_action=run``
and get a deterministic verify gate — receipt-backed, replayable, and
linked to the resulting verification_runs row.

The handler dispatches to ``run_registered_verifier`` which records the
run in ``verification_runs`` (canonical ledger) and may promote a control-
plane bug when ``promote_bug=True`` and the run failed. For most callers
the safe default is ``promote_bug=False`` — they want to run the
verifier as a gate, not as a bug-discovery surface.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class VerifierRunCommand(BaseModel):
    """Input contract for the ``verifier.run`` command operation.

    Only ``verifier_ref`` is required. ``target_kind`` and ``target_ref``
    default to platform-level (matching the verifier registry's most
    common shape). ``inputs`` overrides the verifier's default_inputs
    on a per-call basis. ``record_run=True`` (the default) writes a
    verification_runs row; pass False for dry-runs that should not
    appear in the ledger. ``promote_bug=False`` (the default) avoids
    auto-filing a control-plane bug on failure — leave that on for
    canonical scheduler runs only.
    """

    verifier_ref: str = Field(
        ...,
        description="Verifier authority ref to run (e.g. verifier.job.python.pytest_file).",
    )
    target_kind: Literal["platform", "receipt", "run", "path"] = Field(
        default="platform",
        description="Target kind. Must match what the verifier accepts.",
    )
    target_ref: str = Field(
        default="",
        description="Target reference — absolute path for path-kind, receipt_id for receipt-kind, run_id for run-kind, or empty for platform.",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Per-call input overrides merged onto verifier.default_inputs.",
    )
    record_run: bool = Field(
        default=True,
        description="Write the verification_runs row. False = dry-run, no ledger entry.",
    )
    promote_bug: bool = Field(
        default=False,
        description="On failed/error runs, file/promote a control-plane bug. Default False — most callers should not auto-file.",
    )


def handle_verifier_run(
    command: VerifierRunCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Run the named verifier and return the outcome.

    Returns ``ok=True`` only when ``status == 'passed'``. The
    ``event_payload`` block is what the gateway hoists onto the
    ``authority_events`` row when the command's event contract is
    enabled.
    """

    from runtime.verifier_authority import run_registered_verifier

    outcome = run_registered_verifier(
        command.verifier_ref,
        inputs=dict(command.inputs or {}),
        target_kind=command.target_kind,
        target_ref=command.target_ref,
        conn=subsystems.get_pg_conn(),
        record_run=command.record_run,
        promote_bug=command.promote_bug,
    )
    status = str(outcome.get("status") or "error")
    verification_run_id = outcome.get("verification_run_id")
    return {
        "ok": status == "passed",
        "operation": "verifier.run.completed",
        "verifier_ref": command.verifier_ref,
        "verification_run_id": verification_run_id,
        "status": status,
        "target_kind": outcome.get("target_kind"),
        "target_ref": outcome.get("target_ref"),
        "inputs": outcome.get("inputs"),
        "outputs": outcome.get("outputs"),
        "duration_ms": outcome.get("duration_ms"),
        "suggested_healer_ref": outcome.get("suggested_healer_ref"),
        "bug_id": outcome.get("bug_id"),
        "event_payload": {
            "verifier_ref": command.verifier_ref,
            "verification_run_id": verification_run_id,
            "status": status,
            "target_kind": outcome.get("target_kind"),
            "target_ref": outcome.get("target_ref"),
            "duration_ms": outcome.get("duration_ms"),
            "suggested_healer_ref": outcome.get("suggested_healer_ref"),
            "bug_id": outcome.get("bug_id"),
            "passed": status == "passed",
        },
    }


__all__ = [
    "VerifierRunCommand",
    "handle_verifier_run",
]
