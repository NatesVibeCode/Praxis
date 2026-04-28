"""CQRS query for authority-domain registration previews."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class QueryAuthorityDomainForge(BaseModel):
    authority_domain_ref: str = Field(description="Authority domain ref, e.g. authority.object_truth.")
    owner_ref: str = "praxis.engine"
    event_stream_ref: str | None = None
    current_projection_ref: str | None = None
    storage_target_ref: str = "praxis.primary_postgres"
    decision_ref: str | None = None

    @field_validator(
        "authority_domain_ref",
        "owner_ref",
        "event_stream_ref",
        "current_projection_ref",
        "storage_target_ref",
        "decision_ref",
        mode="before",
    )
    @classmethod
    def _normalize_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("authority-domain forge text fields must be non-empty strings")
        return value.strip()

    @field_validator("authority_domain_ref")
    @classmethod
    def _check_authority_domain_ref(cls, value: str) -> str:
        if not value.startswith("authority."):
            raise ValueError("authority_domain_ref must start with 'authority.'")
        return value

    @model_validator(mode="after")
    def _apply_defaults(self) -> "QueryAuthorityDomainForge":
        if self.owner_ref is None:
            self.owner_ref = "praxis.engine"
        if self.storage_target_ref is None:
            self.storage_target_ref = "praxis.primary_postgres"
        if self.event_stream_ref is None:
            self.event_stream_ref = f"stream.{self.authority_domain_ref}"
        return self


def _as_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _rows(rows: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in rows or []]


def _compact(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


def _shell_json(payload: dict[str, Any]) -> str:
    return "'" + json.dumps(_compact(payload), sort_keys=True) + "'"


def handle_query_authority_domain_forge(
    query: QueryAuthorityDomainForge,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    existing = _as_dict(
        conn.fetchrow(
            """
            SELECT authority_domain_ref, owner_ref, event_stream_ref,
                   current_projection_ref, storage_target_ref, enabled,
                   decision_ref, created_at, updated_at
              FROM authority_domains
             WHERE authority_domain_ref = $1
            """,
            query.authority_domain_ref,
        )
    )
    storage_target = _as_dict(
        conn.fetchrow(
            """
            SELECT storage_target_ref
              FROM authority_storage_targets
             WHERE storage_target_ref = $1
            """,
            query.storage_target_ref,
        )
    )
    operations = _rows(
        conn.fetch(
            """
            SELECT operation_ref, operation_name, operation_kind, posture,
                   idempotency_policy, enabled
              FROM operation_catalog_registry
             WHERE authority_domain_ref = $1
             ORDER BY operation_name
             LIMIT 25
            """,
            query.authority_domain_ref,
        )
    )
    authority_objects = _rows(
        conn.fetch(
            """
            SELECT object_ref, object_kind, object_name, lifecycle_status,
                   data_dictionary_object_kind
              FROM authority_object_registry
             WHERE authority_domain_ref = $1
             ORDER BY object_kind, object_name
             LIMIT 25
            """,
            query.authority_domain_ref,
        )
    )
    near_matches = _rows(
        conn.fetch(
            """
            SELECT authority_domain_ref, owner_ref, enabled
              FROM authority_domains
             WHERE authority_domain_ref <> $1
               AND authority_domain_ref ILIKE $2
             ORDER BY authority_domain_ref
             LIMIT 10
            """,
            query.authority_domain_ref,
            f"%{query.authority_domain_ref.removeprefix('authority.')}%",
        )
    )

    register_payload = {
        "authority_domain_ref": query.authority_domain_ref,
        "owner_ref": query.owner_ref,
        "event_stream_ref": query.event_stream_ref,
        "current_projection_ref": query.current_projection_ref,
        "storage_target_ref": query.storage_target_ref,
        "decision_ref": query.decision_ref,
    }
    missing = []
    if not query.decision_ref and not existing:
        missing.append("decision_ref")
    if not storage_target:
        missing.append("storage_target_ref")

    return {
        "operation": "authority_domain_forge",
        "view": "authority_domain_forge",
        "authority": "authority_domains",
        "state": "existing_domain" if existing else "new_domain",
        "existing_domain": existing or None,
        "proposed_domain": _compact(register_payload),
        "attached_operations": operations,
        "authority_objects": authority_objects,
        "near_matches": near_matches,
        "register_authority_domain_payload": register_payload,
        "register_authority_domain_payload_compact": _compact(register_payload),
        "next_action_packet": {
            "write_order": [
                "Confirm this is the one authority domain that should own the state.",
                "Register the domain through authority_domain_register before registering operations or tables.",
                "Register operations through praxis_register_operation only after the domain exists.",
                "Add MCP/CLI wrappers only after gateway execution is receipt-backed.",
            ],
            "register_command": (
                "praxis workflow tools call praxis_register_authority_domain --input-json "
                f"{_shell_json(register_payload)} --yes"
            ),
            "success_evidence": [
                "authority_domains row exists and is enabled",
                "operation_catalog_registry rows point at the intended authority_domain_ref",
                "authority_object_registry rows do not split ownership for the same object",
                "data_dictionary_objects rows describe the registered operation/table categories",
            ],
        },
        "missing_inputs": missing,
        "reject_paths": [
            "Do not register work under a nearby existing authority domain just to satisfy a foreign key.",
            "Do not create operation rows before the owning authority domain exists.",
            "Do not use authority.workflow_runs as a parking lot for unrelated product truth.",
            "Do not hand-edit only one registry table; use the registered gateway commands.",
        ],
        "ok_to_register": not existing and not missing,
    }


__all__ = [
    "QueryAuthorityDomainForge",
    "handle_query_authority_domain_forge",
]
