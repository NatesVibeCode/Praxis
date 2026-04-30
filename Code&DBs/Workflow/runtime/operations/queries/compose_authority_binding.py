"""Gateway query for compose-time canonical authority resolution."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.workflow.compose_authority_binding import (
    resolve_compose_authority_binding,
)


class ResolveComposeAuthorityBinding(BaseModel):
    """Input for `authority.compose_binding.resolve`."""

    targets: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("targets", mode="before")
    @classmethod
    def _normalize_targets(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, dict):
            return [value]
        if isinstance(value, (list, tuple)):
            return [dict(item) if isinstance(item, dict) else item for item in value]
        raise ValueError("targets must be a list of {unit_kind, unit_ref} objects")


def handle_resolve_compose_authority_binding(
    command: ResolveComposeAuthorityBinding,
    subsystems: Any,
) -> dict[str, Any]:
    """Resolve the canonical write scope + predecessor obligation pack."""

    conn = subsystems.get_pg_conn()
    try:
        binding = resolve_compose_authority_binding(conn, raw_targets=command.targets)
    except ValueError as exc:
        return {
            "ok": False,
            "reason_code": "authority.compose_binding.invalid_input",
            "error": str(exc),
        }
    payload = binding.to_dict()
    payload["ok"] = True
    payload["target_count"] = len(command.targets)
    payload["canonical_count"] = len(binding.canonical_write_scope)
    payload["predecessor_count"] = len(binding.predecessor_obligations)
    payload["blocked_compat_count"] = len(binding.blocked_compat_units)
    payload["redirected_count"] = sum(
        1 for unit in binding.canonical_write_scope if unit.was_redirected
    )
    return payload


__all__ = [
    "ResolveComposeAuthorityBinding",
    "handle_resolve_compose_authority_binding",
]
