"""CQRS query for latest Object Truth version lookup."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.object_truth_repository import load_latest_object_truth_version


class QueryObjectTruthLatestVersionRead(BaseModel):
    """Read the latest trusted Object Truth version without a digest."""

    system_ref: str | None = None
    object_ref: str | None = None
    identity_digest: str | None = None
    client_ref: str | None = None
    trusted_only: bool = True
    max_age_seconds: int | None = Field(default=None, ge=1)
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("system_ref", "object_ref", "identity_digest", "client_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("latest-version filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _require_filter(self) -> "QueryObjectTruthLatestVersionRead":
        if not (self.system_ref or self.object_ref or self.identity_digest or self.client_ref):
            raise ValueError("provide at least one latest-version filter")
        return self


def handle_object_truth_latest_version_read(
    query: QueryObjectTruthLatestVersionRead,
    subsystems: Any,
) -> dict[str, Any]:
    """Return latest trusted Object Truth version and no-go states."""

    result = load_latest_object_truth_version(
        subsystems.get_pg_conn(),
        system_ref=query.system_ref,
        object_ref=query.object_ref,
        identity_digest=query.identity_digest,
        client_ref=query.client_ref,
        trusted_only=query.trusted_only,
        max_age_seconds=query.max_age_seconds,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "object_truth_latest_version_read",
        **result,
    }


__all__ = [
    "QueryObjectTruthLatestVersionRead",
    "handle_object_truth_latest_version_read",
]
