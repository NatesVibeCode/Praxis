from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.platform_patterns import PlatformPatternAuthority


class PatternMaterializeCandidatesCommand(BaseModel):
    sources: list[str] | None = None
    limit: int = 20
    threshold: int = 3
    since_hours: float | None = None
    include_test: bool = False
    candidate_keys: list[str] | None = None
    promotion_only: bool = True
    status: str = "confirmed"
    created_by: str = "gateway.pattern_materialize_candidates"

    @field_validator("sources", "candidate_keys", mode="before")
    @classmethod
    def _normalize_optional_list(cls, value: object) -> list[str] | None:
        if value in (None, ""):
            return None
        if not isinstance(value, list):
            raise ValueError("list fields must be lists")
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("limit", "threshold", mode="before")
    @classmethod
    def _normalize_int(cls, value: object) -> int:
        if value in (None, ""):
            return 20
        if isinstance(value, bool):
            raise ValueError("numeric fields must not be booleans")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("numeric fields must be integers") from exc

    @field_validator("since_hours", mode="before")
    @classmethod
    def _normalize_since_hours(cls, value: object) -> float | None:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            raise ValueError("since_hours must be a number")
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("since_hours must be a number") from exc

    @field_validator("status", "created_by", mode="before")
    @classmethod
    def _normalize_text(cls, value: object) -> str:
        if value is None:
            return ""
        if not isinstance(value, str) or not value.strip():
            raise ValueError("text fields must be non-empty strings")
        return value.strip()


def handle_pattern_materialize_candidates(
    command: PatternMaterializeCandidatesCommand,
    subsystems: Any,
) -> dict[str, Any]:
    authority = PlatformPatternAuthority(subsystems.get_pg_conn())
    return authority.materialize_candidates(
        sources=command.sources,
        limit=command.limit,
        threshold=command.threshold,
        since_hours=command.since_hours,
        include_test=command.include_test,
        candidate_keys=command.candidate_keys,
        promotion_only=command.promotion_only,
        status=command.status,
        created_by=command.created_by,
    )
