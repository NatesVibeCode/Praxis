"""Gateway query for compile/materialize preview."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class CompilePreviewQuery(BaseModel):
    """Read-only preview of an operator intent before materialization."""

    intent: str = Field(..., description="Operator prose to preview.")
    match_limit: int = Field(
        default=5,
        description="Maximum authority candidates per recognized span.",
    )

    @field_validator("intent", mode="before")
    @classmethod
    def _intent_required(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("intent must be a non-empty string")
        return value.strip()

    @field_validator("match_limit", mode="before")
    @classmethod
    def _match_limit_int(cls, value: object) -> int:
        if value in (None, ""):
            return 5
        if isinstance(value, bool):
            raise ValueError("match_limit must be an integer")
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("match_limit must be an integer") from exc
        if normalized < 1 or normalized > 100:
            raise ValueError("match_limit must be in [1, 100]")
        return normalized


def handle_compile_preview(query: CompilePreviewQuery, subsystems: Any) -> dict[str, Any]:
    from runtime.compile_cqrs import preview_compile

    return preview_compile(
        query.intent,
        conn=subsystems.get_pg_conn(),
        match_limit=query.match_limit,
    ).to_dict()


__all__ = ["CompilePreviewQuery", "handle_compile_preview"]
