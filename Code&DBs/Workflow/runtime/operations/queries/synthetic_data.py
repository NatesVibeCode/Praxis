"""CQRS queries for Synthetic Data authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.synthetic_data_repository import (
    list_synthetic_datasets,
    list_synthetic_records,
    load_synthetic_dataset,
)


ReadAction = Literal["list_datasets", "describe_dataset", "list_records"]


class QuerySyntheticDataRead(BaseModel):
    """Read synthetic datasets, naming plans, quality reports, and records."""

    action: ReadAction = "list_datasets"
    dataset_ref: str | None = None
    namespace: str | None = None
    source_context_ref: str | None = None
    quality_state: str | None = None
    object_kind: str | None = None
    include_records: bool = True
    limit: int = Field(default=50, ge=1, le=5000)

    @field_validator("dataset_ref", "namespace", "source_context_ref", "quality_state", "object_kind", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QuerySyntheticDataRead":
        if self.action in {"describe_dataset", "list_records"} and not self.dataset_ref:
            raise ValueError(f"dataset_ref is required for {self.action}")
        return self


def handle_synthetic_data_read(
    query: QuerySyntheticDataRead,
    subsystems: Any,
) -> dict[str, Any]:
    """Read Synthetic Data authority records."""

    conn = subsystems.get_pg_conn()
    if query.action == "describe_dataset":
        dataset = load_synthetic_dataset(
            conn,
            dataset_ref=str(query.dataset_ref),
            include_records=query.include_records,
            limit=query.limit,
        )
        return {
            "ok": dataset is not None,
            "operation": "synthetic_data_read",
            "action": "describe_dataset",
            "dataset_ref": query.dataset_ref,
            "dataset": dataset,
            "error_code": None if dataset is not None else "synthetic_data.dataset_not_found",
        }
    if query.action == "list_records":
        records = list_synthetic_records(
            conn,
            dataset_ref=str(query.dataset_ref),
            object_kind=query.object_kind,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "synthetic_data_read",
            "action": "list_records",
            "dataset_ref": query.dataset_ref,
            "count": len(records),
            "records": records,
        }
    datasets = list_synthetic_datasets(
        conn,
        namespace=query.namespace,
        source_context_ref=query.source_context_ref,
        quality_state=query.quality_state,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "synthetic_data_read",
        "action": "list_datasets",
        "count": len(datasets),
        "datasets": datasets,
        "filters": {
            "namespace": query.namespace,
            "source_context_ref": query.source_context_ref,
            "quality_state": query.quality_state,
            "limit": query.limit,
        },
    }


__all__ = [
    "QuerySyntheticDataRead",
    "handle_synthetic_data_read",
]
