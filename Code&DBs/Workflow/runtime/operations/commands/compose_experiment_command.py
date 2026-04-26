"""Compose-experiment command — gateway-friendly handler for the matrix runner.

Registered by migration 274. Fires the parallel
``runtime.compose_experiment.run_compose_experiment`` and lets
``operation_catalog_gateway`` write the parent receipt + the
``compose.experiment.completed`` event as a side-effect of dispatch —
honouring ``architecture-policy::platform-architecture::conceptual-events-
register-through-operation-catalog-registry``.

The handler is thin: it validates the input, mints a connection factory
that the runner can call once per worker (since asyncpg connections are
not thread-safe), invokes the runner, and returns the dict-shaped
report. The gateway owns receipt writing + event emission.
"""
from __future__ import annotations

import hashlib
from typing import Any

from pydantic import BaseModel, Field


class ComposeExperimentCommand(BaseModel):
    """Input contract for ``compose_experiment``.

    ``intent`` — the prose intent forwarded to every child compose call.
    ``configs`` — list of override dicts (each may contain
    ``provider_slug``, ``model_slug``, ``temperature``, ``max_tokens``).
    ``plan_name`` / ``why`` — optional caller labels passed to each child.
    ``concurrency`` — PER-CHILD fork-out concurrency (default 5; lower
    than the bare compose default of 20 because matrix × per-child
    fan-out compounds against rate limits).
    ``max_workers`` — PARENT fan-out cap (default 8). Children run in
    parallel up to this number.
    ``caller_ref`` — surface subsystem that fired the request.
    """

    intent: str
    configs: list[dict[str, Any]] = Field(default_factory=list)
    plan_name: str | None = None
    why: str | None = None
    concurrency: int = 5
    max_workers: int = 8
    caller_ref: str = "platform.compose_experiment"


def _intent_fingerprint(intent: str) -> str:
    return hashlib.sha256((intent or "").strip().encode("utf-8")).hexdigest()


def handle_compose_experiment(
    command: ComposeExperimentCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Run the parallel matrix and return a dict ready for the gateway
    to wrap into a receipt + event payload."""
    from runtime.compose_experiment import run_compose_experiment

    # The runner needs ``subsystems`` (not just a conn factory) because
    # each child dispatches through ``execute_operation_from_subsystems``
    # so it produces its own ``compose-plan-via-llm`` receipt +
    # ``plan.composed`` event. The shared subsystems instance is
    # thread-safe via lazy lookup; each thread gets its own DB conn.
    report = run_compose_experiment(
        command.intent,
        list(command.configs or []),
        subsystems=subsystems,
        plan_name=command.plan_name,
        concurrency=int(command.concurrency or 5),
        max_workers=int(command.max_workers or 8),
    )

    winner = report.winner()
    success_count = sum(
        1 for r in report.runs
        if r.ok and r.result is not None and r.result.ok
    )
    fingerprint = _intent_fingerprint(command.intent)

    # Compact summary for the event payload — keeps the row small while
    # carrying enough to rank / replay later.
    matrix_summary = [run.summary_row() for run in report.runs]

    event_payload = {
        "intent_fingerprint": fingerprint,
        "config_count": len(report.runs),
        "success_count": success_count,
        "winning_config_index": winner.config_index if winner is not None else None,
        "winning_wall_seconds": (
            round(winner.wall_seconds, 3) if winner is not None else None
        ),
        "total_wall_seconds": round(report.total_wall_seconds, 3),
        "matrix_summary": matrix_summary,
        "caller_ref": command.caller_ref,
    }

    # The gateway reads ``ok`` + the rest as the receipt's result_payload.
    # Children already produced their own receipts via compose_plan_via_llm;
    # this parent receipt links them by intent_fingerprint + summary table.
    return {
        "ok": True,
        "report": report.to_dict(),
        "winning_config_index": event_payload["winning_config_index"],
        "winning_wall_seconds": event_payload["winning_wall_seconds"],
        "success_count": success_count,
        "total_wall_seconds": event_payload["total_wall_seconds"],
        "intent_fingerprint": fingerprint,
        "caller_ref": command.caller_ref,
        # The gateway hoists this dict onto the authority_event row when
        # event_required=TRUE on the operation_catalog_registry binding.
        "event_payload": event_payload,
    }


__all__ = [
    "ComposeExperimentCommand",
    "handle_compose_experiment",
]
