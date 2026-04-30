"""CQRS queries for portable cartridge deployment contract authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.portable_cartridge_repository import (
    list_portable_cartridge_assets,
    list_portable_cartridge_bindings,
    list_portable_cartridge_dependencies,
    list_portable_cartridge_drift_hooks,
    list_portable_cartridge_records,
    list_portable_cartridge_verifiers,
    load_portable_cartridge_record,
)


ReadAction = Literal[
    "list_records",
    "describe_record",
    "list_dependencies",
    "list_assets",
    "list_bindings",
    "list_verifiers",
    "list_drift_hooks",
]


class ReadPortableCartridgeQuery(BaseModel):
    """Read persisted portable cartridge deployment contract records."""

    action: ReadAction = "list_records"
    cartridge_record_id: str | None = None
    cartridge_id: str | None = None
    readiness_status: str | None = None
    deployment_mode: str | None = None
    manifest_digest: str | None = None
    source_ref: str | None = None
    dependency_id: str | None = None
    dependency_class: str | None = None
    authority_source: str | None = None
    asset_role: str | None = None
    binding_kind: str | None = None
    verifier_category: str | None = None
    hook_point: str | None = None
    required: bool | None = None
    include_dependencies: bool = True
    include_assets: bool = True
    include_bindings: bool = True
    include_verifiers: bool = True
    include_drift_hooks: bool = True
    limit: int = Field(default=50, ge=1, le=500)

    @field_validator(
        "cartridge_record_id",
        "cartridge_id",
        "readiness_status",
        "deployment_mode",
        "manifest_digest",
        "source_ref",
        "dependency_id",
        "dependency_class",
        "authority_source",
        "asset_role",
        "binding_kind",
        "verifier_category",
        "hook_point",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "ReadPortableCartridgeQuery":
        if self.action == "describe_record" and not self.cartridge_record_id:
            raise ValueError("cartridge_record_id is required for describe_record")
        return self


def handle_read_portable_cartridge(
    query: ReadPortableCartridgeQuery,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe_record":
        record = load_portable_cartridge_record(
            conn,
            cartridge_record_id=str(query.cartridge_record_id),
            include_dependencies=query.include_dependencies,
            include_assets=query.include_assets,
            include_bindings=query.include_bindings,
            include_verifiers=query.include_verifiers,
            include_drift_hooks=query.include_drift_hooks,
        )
        return {
            "ok": record is not None,
            "operation": "authority.portable_cartridge.read",
            "action": "describe_record",
            "cartridge_record_id": query.cartridge_record_id,
            "record": record,
            "error_code": None if record is not None else "portable_cartridge.record_not_found",
        }
    if query.action == "list_dependencies":
        items = list_portable_cartridge_dependencies(
            conn,
            cartridge_record_id=query.cartridge_record_id,
            dependency_id=query.dependency_id,
            dependency_class=query.dependency_class,
            authority_source=query.authority_source,
            required=query.required,
            limit=query.limit,
        )
        return _list_result(query.action, items)
    if query.action == "list_assets":
        items = list_portable_cartridge_assets(
            conn,
            cartridge_record_id=query.cartridge_record_id,
            role=query.asset_role,
            required=query.required,
            limit=query.limit,
        )
        return _list_result(query.action, items)
    if query.action == "list_bindings":
        items = list_portable_cartridge_bindings(
            conn,
            cartridge_record_id=query.cartridge_record_id,
            binding_kind=query.binding_kind,
            required=query.required,
            limit=query.limit,
        )
        return _list_result(query.action, items)
    if query.action == "list_verifiers":
        items = list_portable_cartridge_verifiers(
            conn,
            cartridge_record_id=query.cartridge_record_id,
            verifier_category=query.verifier_category,
            required=query.required,
            limit=query.limit,
        )
        return _list_result(query.action, items)
    if query.action == "list_drift_hooks":
        items = list_portable_cartridge_drift_hooks(
            conn,
            cartridge_record_id=query.cartridge_record_id,
            hook_point=query.hook_point,
            required=query.required,
            limit=query.limit,
        )
        return _list_result(query.action, items)

    items = list_portable_cartridge_records(
        conn,
        cartridge_id=query.cartridge_id,
        readiness_status=query.readiness_status,
        deployment_mode=query.deployment_mode,
        manifest_digest=query.manifest_digest,
        source_ref=query.source_ref,
        limit=query.limit,
    )
    return _list_result("list_records", items)


def _list_result(action: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "ok": True,
        "operation": "authority.portable_cartridge.read",
        "action": action,
        "count": len(items),
        "items": items,
    }


__all__ = [
    "ReadPortableCartridgeQuery",
    "handle_read_portable_cartridge",
]
