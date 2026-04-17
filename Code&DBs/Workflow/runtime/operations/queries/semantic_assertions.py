from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class QuerySemanticAssertions(BaseModel):
    predicate_slug: str | None = None
    subject_kind: str | None = None
    subject_ref: str | None = None
    object_kind: str | None = None
    object_ref: str | None = None
    source_kind: str | None = None
    source_ref: str | None = None
    active_only: bool = True
    as_of: datetime | None = None
    limit: int = 100

    @field_validator(
        "predicate_slug",
        "subject_kind",
        "subject_ref",
        "object_kind",
        "object_ref",
        "source_kind",
        "source_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("semantic assertion filters must be non-empty strings when provided")
        return value.strip()


def _resolved_env(subsystems: Any) -> dict[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


async def handle_query_semantic_assertions(
    query: QuerySemanticAssertions,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.semantic_assertions import SemanticAssertionFrontdoor

    return await SemanticAssertionFrontdoor().list_assertions_async(
        predicate_slug=query.predicate_slug,
        subject_kind=query.subject_kind,
        subject_ref=query.subject_ref,
        object_kind=query.object_kind,
        object_ref=query.object_ref,
        source_kind=query.source_kind,
        source_ref=query.source_ref,
        active_only=query.active_only,
        as_of=query.as_of,
        limit=query.limit,
        env=_resolved_env(subsystems),
    )


__all__ = ["QuerySemanticAssertions", "handle_query_semantic_assertions"]
