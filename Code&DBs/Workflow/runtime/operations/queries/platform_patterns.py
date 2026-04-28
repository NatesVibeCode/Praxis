from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator

from runtime.platform_patterns import PlatformPatternAuthority


class OperatorPatternsQuery(BaseModel):
    action: Literal["list", "candidates", "evidence"] = "list"
    pattern_ref: str | None = None
    pattern_kind: str | None = None
    status: str | None = None
    sources: list[str] | None = None
    limit: int = 20
    threshold: int = 3
    since_hours: float | None = None
    include_test: bool = False
    include_evidence: bool = False
    include_hydration: bool = False

    @field_validator("pattern_ref", "pattern_kind", "status", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("text filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("sources", mode="before")
    @classmethod
    def _normalize_sources(cls, value: object) -> list[str] | None:
        if value in (None, ""):
            return None
        if not isinstance(value, list):
            raise ValueError("sources must be a list")
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

    @model_validator(mode="after")
    def _validate_action_requirements(self) -> "OperatorPatternsQuery":
        if self.action == "evidence" and not self.pattern_ref:
            raise ValueError("pattern_ref is required for action=evidence")
        return self


def handle_operator_patterns(
    query: OperatorPatternsQuery,
    subsystems: Any,
) -> dict[str, Any]:
    authority = PlatformPatternAuthority(subsystems.get_pg_conn())
    if query.action == "candidates":
        return authority.candidate_bundle(
            sources=query.sources,
            limit=query.limit,
            threshold=query.threshold,
            since_hours=query.since_hours,
            include_test=query.include_test,
            include_hydration=query.include_hydration,
        )
    if query.action == "evidence":
        return {
            "ok": True,
            "pattern_ref": query.pattern_ref,
            "evidence": authority.list_evidence(str(query.pattern_ref), limit=query.limit),
        }
    patterns = authority.list_patterns(
        pattern_kind=query.pattern_kind,
        status=query.status,
        limit=query.limit,
        include_evidence=query.include_evidence,
        include_hydration=query.include_hydration,
    )
    return {"ok": True, "count": len(patterns), "patterns": patterns}
