"""Process-local projection of claim lifecycle transition authority.

The durable authority lives in the canonical workflow migration file and its
Postgres materialization. This module generates the in-process mirror from the
same migration text so execution validation does not drift from the schema.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from functools import lru_cache

from storage.migrations import WorkflowMigrationError, workflow_migration_sql_text

from .domain import RunState, RuntimeBoundaryError, RuntimeLifecycleError

_CLAIM_LIFECYCLE_MIGRATION_FILENAME = "135_claim_lifecycle_transition_authority.sql"
_TRANSITION_ROW_RE = re.compile(
    r"\(\s*'[^']*'\s*,\s*'(?P<from_state>[^']+)'\s*,\s*'(?P<to_state>[^']+)'",
    re.MULTILINE,
)


def _parse_transition_rows(sql_text: str) -> Mapping[RunState, frozenset[RunState]]:
    insert_start = sql_text.find("INSERT INTO workflow_claim_lifecycle_transition_authority")
    if insert_start == -1:
        raise RuntimeBoundaryError(
            "claim lifecycle transition authority migration is missing the authority table insert"
        )

    values_start = sql_text.find("VALUES", insert_start)
    conflict_start = sql_text.find("ON CONFLICT", values_start)
    if values_start == -1 or conflict_start == -1 or conflict_start <= values_start:
        raise RuntimeBoundaryError(
            "claim lifecycle transition authority migration is missing its VALUES block"
        )

    grouped: dict[RunState, set[RunState]] = {}
    values_block = sql_text[values_start:conflict_start]
    for match in _TRANSITION_ROW_RE.finditer(values_block):
        try:
            from_state = RunState(match.group("from_state"))
            to_state = RunState(match.group("to_state"))
        except ValueError as exc:
            raise RuntimeBoundaryError(
                "claim lifecycle transition authority migration contains an unknown run state"
            ) from exc
        grouped.setdefault(from_state, set()).add(to_state)

    if not grouped:
        raise RuntimeBoundaryError(
            "claim lifecycle transition authority migration did not define any transitions"
        )

    return {
        from_state: frozenset(sorted(to_states, key=lambda state: state.value))
        for from_state, to_states in grouped.items()
    }


@lru_cache(maxsize=1)
def _load_allowed_transitions() -> Mapping[RunState, frozenset[RunState]]:
    try:
        sql_text = workflow_migration_sql_text(_CLAIM_LIFECYCLE_MIGRATION_FILENAME)
    except WorkflowMigrationError as exc:
        raise RuntimeBoundaryError(
            "claim lifecycle transition authority migration is unavailable"
        ) from exc
    return _parse_transition_rows(sql_text)


ALLOWED_TRANSITIONS: Mapping[RunState, frozenset[RunState]] = _load_allowed_transitions()


def validate_claim_lifecycle_transition(*, from_state: RunState, to_state: RunState) -> None:
    allowed_targets = ALLOWED_TRANSITIONS.get(from_state, frozenset())
    if to_state not in allowed_targets:
        raise RuntimeLifecycleError(
            f"invalid claim/lease/proposal transition: {from_state.value} -> {to_state.value}"
        )


__all__ = [
    "ALLOWED_TRANSITIONS",
    "validate_claim_lifecycle_transition",
]
