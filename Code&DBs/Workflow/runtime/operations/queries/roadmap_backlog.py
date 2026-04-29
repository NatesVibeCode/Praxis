from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class QueryRoadmapBacklog(BaseModel):
    """List open roadmap items as a flat backlog.

    The roadmap "tree" is keyed by ``roadmap_item_id`` prefix
    (``root.%``) and most items live as flat siblings — selecting one
    root via ``LIMIT 1`` masks the actual backlog. This query is the
    canonical CQRS-shaped entry point for the open backlog.
    """

    limit: int = Field(default=200, ge=1, le=1000)
    open_only: bool = True
    lifecycle: str | None = None
    status: str | None = None
    priority: str | None = None
    roots_only: bool = False

    @field_validator("lifecycle", "status", "priority", mode="before")
    @classmethod
    def _normalize_optional(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise ValueError("filter values must be strings")
        return value.strip().lower() or None


def handle_query_roadmap_backlog(
    query: QueryRoadmapBacklog,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_read import NativeOperatorQueryFrontdoor

    env = getattr(subsystems, "_postgres_env", None)
    resolved_env = env() if callable(env) else None
    return NativeOperatorQueryFrontdoor().query_roadmap_backlog(
        limit=query.limit,
        open_only=query.open_only,
        lifecycle=query.lifecycle,
        status=query.status,
        priority=query.priority,
        roots_only=query.roots_only,
        env=resolved_env,
    )


__all__ = ["QueryRoadmapBacklog", "handle_query_roadmap_backlog"]
