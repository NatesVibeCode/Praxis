"""CQRS query: list chat routing candidates for the operator console picker drawer.

Backs the StrategyConsole picker. Replaces the hardcoded OPERATOR_CHAT_ENGINE
constants with a live query over task_type_routing rows that:

  - filter to ``permitted=true`` rows by default (enabled-only, clean)
  - surface ``transport_type`` per candidate (anticipates the future
    CLI-in-chat direction recorded under
    ``project_chat_cli_agent_direction.md`` in operator memory)
  - sort by rank ascending then route_health_score descending
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from storage.postgres.task_type_routing_repository import (
    PostgresTaskTypeRoutingRepository,
)


class QueryChatRoutingOptions(BaseModel):
    task_slug: str = Field(
        default="auto/chat",
        description=(
            "Task slug to look up routes for. Accepts 'auto/<task_type>' or"
            " '<task_type>'; the 'auto/' prefix is stripped before querying"
            " task_type_routing."
        ),
    )
    include_disabled: bool = Field(
        default=False,
        description=(
            "When false (default), only permitted=true rows are returned."
            " When true, disabled candidates are returned with their"
            " permitted flag and route_health_score so callers can render"
            " disable reasons."
        ),
    )
    include_cli: bool = Field(
        default=False,
        description=(
            "When false (default), only transport_type='API' rows are returned."
            " CLI from chat is currently broken end-to-end (see"
            " project_chat_api_only_for_now memory): the runtime hardcodes"
            " transport_type='CLI' in upsert_derived_route, which trigger"
            " 378 then rejects for HTTP-only providers, breaking dispatch."
            " Set include_cli=true only for diagnostic purposes."
        ),
    )

    @field_validator("task_slug", mode="before")
    @classmethod
    def _normalize_task_slug(cls, value: object) -> str:
        if value is None:
            return "auto/chat"
        if not isinstance(value, str):
            raise ValueError("task_slug must be a string")
        cleaned = value.strip()
        if not cleaned:
            return "auto/chat"
        return cleaned


def _row_to_candidate(row: Any) -> dict[str, Any]:
    return {
        "provider_slug": row.get("provider_slug"),
        "model_slug": row.get("model_slug"),
        "transport_type": row.get("transport_type"),
        "task_type": row.get("task_type"),
        "sub_task_type": row.get("sub_task_type"),
        "rank": row.get("rank"),
        "permitted": row.get("permitted"),
        "route_health_score": row.get("route_health_score"),
        "benchmark_score": row.get("benchmark_score"),
        "route_tier": row.get("route_tier"),
        "latency_class": row.get("latency_class"),
        "cost_per_m_tokens": row.get("cost_per_m_tokens"),
    }


def handle_query_chat_routing_options(
    query: QueryChatRoutingOptions,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    task_type = query.task_slug.removeprefix("auto/")
    rows = repo.load_routes(task_type=task_type)

    candidates = [_row_to_candidate(row) for row in rows]
    if not query.include_disabled:
        candidates = [c for c in candidates if c.get("permitted")]
    if not query.include_cli:
        candidates = [c for c in candidates if c.get("transport_type") == "API"]

    candidates.sort(
        key=lambda c: (
            c.get("rank") if c.get("rank") is not None else 9999,
            -(c.get("route_health_score") or 0.0),
        )
    )

    return {
        "ok": True,
        "task_slug": query.task_slug,
        "task_type": task_type,
        "include_disabled": query.include_disabled,
        "include_cli": query.include_cli,
        "candidates": candidates,
        "candidate_count": len(candidates),
    }


__all__ = [
    "QueryChatRoutingOptions",
    "handle_query_chat_routing_options",
]
