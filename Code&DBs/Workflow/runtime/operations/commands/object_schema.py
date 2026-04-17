from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator

from runtime.object_lifecycle import (
    create_object_type,
    delete_object_type,
    retire_object_field,
    upsert_object_field,
    upsert_object_type,
)


class UpsertObjectTypeCommand(BaseModel):
    type_id: str | None = None
    name: str
    description: str = ""
    icon: str = ""
    fields: list[dict[str, Any]] = []


class DeleteObjectTypeCommand(BaseModel):
    type_id: str

    @field_validator("type_id", mode="before")
    @classmethod
    def _normalize_type_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("type_id is required")
        return value.strip()


class UpsertObjectFieldCommand(BaseModel):
    type_id: str
    field_name: str
    field_kind: str
    label: str = ""
    description: str = ""
    required: bool = False
    default_value: Any = None
    options: list[Any] = []
    display_order: int = 100

    @field_validator("type_id", "field_name", "field_kind", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("required text field is missing")
        return value.strip()


class RetireObjectFieldCommand(BaseModel):
    type_id: str
    field_name: str

    @field_validator("type_id", "field_name", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("required text field is missing")
        return value.strip()


def handle_upsert_object_type(
    command: UpsertObjectTypeCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if command.type_id:
        return {
            "type": upsert_object_type(
                conn,
                type_id=command.type_id,
                name=command.name,
                description=command.description,
                icon=command.icon,
                fields=command.fields,
            )
        }
    return {
        "type": create_object_type(
            conn,
            name=command.name,
            description=command.description,
            icon=command.icon,
            fields=command.fields,
        )
    }


def handle_delete_object_type(
    command: DeleteObjectTypeCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return delete_object_type(
        subsystems.get_pg_conn(),
        type_id=command.type_id,
    )


def handle_upsert_object_field(
    command: UpsertObjectFieldCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return upsert_object_field(
        subsystems.get_pg_conn(),
        type_id=command.type_id,
        field_name=command.field_name,
        field_kind=command.field_kind,
        label=command.label,
        description=command.description,
        required=command.required,
        default_value=command.default_value,
        options=command.options,
        display_order=command.display_order,
    )


def handle_retire_object_field(
    command: RetireObjectFieldCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return retire_object_field(
        subsystems.get_pg_conn(),
        type_id=command.type_id,
        field_name=command.field_name,
    )


__all__ = [
    "DeleteObjectTypeCommand",
    "RetireObjectFieldCommand",
    "UpsertObjectFieldCommand",
    "UpsertObjectTypeCommand",
    "handle_delete_object_type",
    "handle_retire_object_field",
    "handle_upsert_object_field",
    "handle_upsert_object_type",
]
