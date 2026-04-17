from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class QueryOperatorDecisions(BaseModel):
    as_of: datetime | None = None
    decision_kind: str | None = None
    decision_source: str | None = None
    decision_scope_kind: str | None = None
    decision_scope_ref: str | None = None
    active_only: bool = True
    limit: int = 100

    @field_validator(
        "decision_kind",
        "decision_source",
        "decision_scope_kind",
        "decision_scope_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("operator decision filters must be non-empty strings when provided")
        return value.strip()


def _resolved_env(subsystems: Any) -> dict[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


def handle_query_operator_decisions(
    query: QueryOperatorDecisions,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().list_operator_decisions(
        as_of=query.as_of,
        decision_kind=query.decision_kind,
        decision_source=query.decision_source,
        decision_scope_kind=query.decision_scope_kind,
        decision_scope_ref=query.decision_scope_ref,
        active_only=query.active_only,
        limit=query.limit,
        env=_resolved_env(subsystems),
    )


__all__ = ["QueryOperatorDecisions", "handle_query_operator_decisions"]
