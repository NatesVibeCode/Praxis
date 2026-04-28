"""CQRS command for registering authority domains."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class RegisterAuthorityDomainCommand(BaseModel):
    authority_domain_ref: str = Field(description="Authority domain ref, e.g. authority.object_truth.")
    owner_ref: str = "praxis.engine"
    event_stream_ref: str | None = None
    current_projection_ref: str | None = None
    storage_target_ref: str = "praxis.primary_postgres"
    decision_ref: str = Field(description="Decision or policy ref that justifies this authority boundary.")
    enabled: bool = True

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
            raise ValueError("authority-domain register text fields must be non-empty strings")
        return value.strip()

    @field_validator("authority_domain_ref")
    @classmethod
    def _check_authority_domain_ref(cls, value: str) -> str:
        if not value.startswith("authority."):
            raise ValueError("authority_domain_ref must start with 'authority.'")
        return value

    @model_validator(mode="after")
    def _apply_defaults(self) -> "RegisterAuthorityDomainCommand":
        if self.owner_ref is None:
            self.owner_ref = "praxis.engine"
        if self.storage_target_ref is None:
            self.storage_target_ref = "praxis.primary_postgres"
        if self.event_stream_ref is None:
            self.event_stream_ref = f"stream.{self.authority_domain_ref}"
        if not self.decision_ref:
            raise ValueError("decision_ref is required")
        return self


def handle_register_authority_domain(
    command: RegisterAuthorityDomainCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    storage_target = conn.fetchrow(
        """
        SELECT storage_target_ref
          FROM authority_storage_targets
         WHERE storage_target_ref = $1
        """,
        command.storage_target_ref,
    )
    if storage_target is None:
        return {
            "ok": False,
            "error_code": "authority_domain_register.storage_target_not_found",
            "error": f"storage_target_ref {command.storage_target_ref!r} is not registered",
            "authority_domain_ref": command.authority_domain_ref,
            "storage_target_ref": command.storage_target_ref,
        }

    conn.execute(
        """
        INSERT INTO authority_domains (
            authority_domain_ref,
            owner_ref,
            event_stream_ref,
            current_projection_ref,
            storage_target_ref,
            enabled,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
        ON CONFLICT (authority_domain_ref) DO UPDATE SET
            owner_ref = EXCLUDED.owner_ref,
            event_stream_ref = EXCLUDED.event_stream_ref,
            current_projection_ref = EXCLUDED.current_projection_ref,
            storage_target_ref = EXCLUDED.storage_target_ref,
            enabled = EXCLUDED.enabled,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        """,
        command.authority_domain_ref,
        command.owner_ref,
        command.event_stream_ref,
        command.current_projection_ref,
        command.storage_target_ref,
        command.enabled,
        command.decision_ref,
    )
    row = conn.fetchrow(
        """
        SELECT authority_domain_ref, owner_ref, event_stream_ref,
               current_projection_ref, storage_target_ref, enabled,
               decision_ref, created_at, updated_at
          FROM authority_domains
         WHERE authority_domain_ref = $1
        """,
        command.authority_domain_ref,
    )
    domain = dict(row) if row is not None else None
    return {
        "ok": True,
        "action": "register",
        "authority_domain": domain,
        "event_payload": {
            "authority_domain_ref": command.authority_domain_ref,
            "owner_ref": command.owner_ref,
            "event_stream_ref": command.event_stream_ref,
            "storage_target_ref": command.storage_target_ref,
            "decision_ref": command.decision_ref,
            "enabled": command.enabled,
        },
    }


__all__ = [
    "RegisterAuthorityDomainCommand",
    "handle_register_authority_domain",
]
