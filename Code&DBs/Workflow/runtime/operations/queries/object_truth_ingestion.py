"""CQRS queries for Object Truth ingestion evidence."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.object_truth_repository import (
    list_ingestion_samples,
    load_ingestion_sample,
)


ReadAction = Literal["list", "describe"]


class QueryObjectTruthIngestionSampleRead(BaseModel):
    """Read Object Truth ingestion sample evidence."""

    action: ReadAction = "list"
    client_ref: str | None = None
    system_ref: str | None = None
    object_ref: str | None = None
    sample_id: str | None = None
    include_payload_references: bool = True
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("client_ref", "system_ref", "object_ref", "sample_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QueryObjectTruthIngestionSampleRead":
        if self.action == "describe" and not self.sample_id:
            raise ValueError("sample_id is required for describe")
        return self


def handle_object_truth_ingestion_sample_read(
    query: QueryObjectTruthIngestionSampleRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe":
        sample = load_ingestion_sample(
            conn,
            sample_id=str(query.sample_id),
            include_payload_references=query.include_payload_references,
        )
        return {
            "ok": sample is not None,
            "operation": "object_truth_ingestion_sample_read",
            "action": "describe",
            "sample_id": query.sample_id,
            "sample": sample,
            "error_code": None if sample is not None else "object_truth.ingestion_sample_not_found",
        }

    items = list_ingestion_samples(
        conn,
        client_ref=query.client_ref,
        system_ref=query.system_ref,
        object_ref=query.object_ref,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "object_truth_ingestion_sample_read",
        "action": "list",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryObjectTruthIngestionSampleRead",
    "handle_object_truth_ingestion_sample_read",
]
