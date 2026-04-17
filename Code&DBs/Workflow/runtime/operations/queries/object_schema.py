from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator

from runtime.object_lifecycle import get_object_type, list_object_fields, list_object_types


class QueryObjectTypes(BaseModel):
    q: str = ""
    limit: int = 100

    @field_validator("q", mode="before")
    @classmethod
    def _normalize_query(cls, value: object) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("q must be a string")
        return value.strip()


class QueryObjectType(BaseModel):
    type_id: str

    @field_validator("type_id", mode="before")
    @classmethod
    def _normalize_type_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("type_id is required")
        return value.strip()


class QueryObjectFields(BaseModel):
    type_id: str
    include_retired: bool = False

    @field_validator("type_id", mode="before")
    @classmethod
    def _normalize_type_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("type_id is required")
        return value.strip()


def handle_query_object_types(
    query: QueryObjectTypes,
    subsystems: Any,
) -> dict[str, Any]:
    return list_object_types(
        subsystems.get_pg_conn(),
        query=query.q,
        limit=query.limit,
    )


def handle_query_object_type(
    query: QueryObjectType,
    subsystems: Any,
) -> dict[str, Any]:
    return {"type": get_object_type(subsystems.get_pg_conn(), type_id=query.type_id)}


def handle_query_object_fields(
    query: QueryObjectFields,
    subsystems: Any,
) -> dict[str, Any]:
    return list_object_fields(
        subsystems.get_pg_conn(),
        type_id=query.type_id,
        include_retired=query.include_retired,
    )


__all__ = [
    "QueryObjectFields",
    "QueryObjectType",
    "QueryObjectTypes",
    "handle_query_object_fields",
    "handle_query_object_type",
    "handle_query_object_types",
]
