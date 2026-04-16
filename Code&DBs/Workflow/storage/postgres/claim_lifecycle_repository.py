"""Read DB-backed claim lifecycle transition authority."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast

import asyncpg

from runtime.domain import RunState


class ClaimLifecycleAuthorityError(RuntimeError):
    """Raised when claim lifecycle transition authority cannot be read safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class ClaimLifecycleTransitionAuthorityRecord:
    workflow_claim_lifecycle_transition_id: str
    from_state: RunState
    to_state: RunState
    rationale: str
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ClaimLifecycleAuthorityError(
            "claim_lifecycle.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ClaimLifecycleAuthorityError(
            "claim_lifecycle.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ClaimLifecycleAuthorityError(
            "claim_lifecycle.invalid_row",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _normalize_as_of(value: datetime) -> datetime:
    return _require_datetime(value, field_name="as_of")


def _record_from_row(row: asyncpg.Record) -> ClaimLifecycleTransitionAuthorityRecord:
    try:
        from_state = RunState(_require_text(row["from_state"], field_name="from_state"))
        to_state = RunState(_require_text(row["to_state"], field_name="to_state"))
    except ValueError as exc:
        raise ClaimLifecycleAuthorityError(
            "claim_lifecycle.invalid_row",
            "claim lifecycle authority row contains an unknown run state",
        ) from exc
    return ClaimLifecycleTransitionAuthorityRecord(
        workflow_claim_lifecycle_transition_id=_require_text(
            row["workflow_claim_lifecycle_transition_id"],
            field_name="workflow_claim_lifecycle_transition_id",
        ),
        from_state=from_state,
        to_state=to_state,
        rationale=_require_text(row["rationale"], field_name="rationale"),
        effective_from=_require_datetime(row["effective_from"], field_name="effective_from"),
        effective_to=(
            _require_datetime(row["effective_to"], field_name="effective_to")
            if row["effective_to"] is not None
            else None
        ),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


class PostgresClaimLifecycleRepository:
    """Explicit Postgres reader for claim lifecycle transition authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_transition_records(
        self,
        *,
        as_of: datetime,
    ) -> tuple[ClaimLifecycleTransitionAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    workflow_claim_lifecycle_transition_id,
                    from_state,
                    to_state,
                    rationale,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM workflow_claim_lifecycle_transition_authority
                WHERE effective_from <= $1
                  AND (effective_to IS NULL OR effective_to > $1)
                ORDER BY from_state, effective_from DESC, created_at DESC, workflow_claim_lifecycle_transition_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise ClaimLifecycleAuthorityError(
                "claim_lifecycle.read_failed",
                "failed to read claim lifecycle transition authority",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        records = tuple(_record_from_row(row) for row in rows)
        if not records:
            raise ClaimLifecycleAuthorityError(
                "claim_lifecycle.empty",
                "claim lifecycle transition authority returned no active rows",
            )
        return records

    async def load_allowed_transitions(
        self,
        *,
        as_of: datetime,
    ) -> Mapping[RunState, frozenset[RunState]]:
        seen_pairs: dict[tuple[RunState, RunState], ClaimLifecycleTransitionAuthorityRecord] = {}
        grouped: dict[RunState, set[RunState]] = defaultdict(set)
        for record in await self.fetch_transition_records(as_of=as_of):
            pair = (record.from_state, record.to_state)
            previous = seen_pairs.get(pair)
            if previous is not None:
                raise ClaimLifecycleAuthorityError(
                    "claim_lifecycle.ambiguous",
                    "claim lifecycle transition authority contains overlapping active rows",
                    details={
                        "from_state": record.from_state.value,
                        "to_state": record.to_state.value,
                        "existing_transition_id": previous.workflow_claim_lifecycle_transition_id,
                        "conflicting_transition_id": (
                            record.workflow_claim_lifecycle_transition_id
                        ),
                    },
                )
            seen_pairs[pair] = record
            grouped[record.from_state].add(record.to_state)
        return {
            from_state: frozenset(sorted(to_states, key=lambda state: state.value))
            for from_state, to_states in grouped.items()
        }


__all__ = [
    "ClaimLifecycleAuthorityError",
    "ClaimLifecycleTransitionAuthorityRecord",
    "PostgresClaimLifecycleRepository",
]
