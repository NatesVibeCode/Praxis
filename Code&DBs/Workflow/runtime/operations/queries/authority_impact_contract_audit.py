"""Gateway query for impact contract drift audit."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.workflow.authority_impact_contract_audit import (
    audit_authority_impact_contract_coverage,
)


class ScanAuthorityImpactContractAudit(BaseModel):
    """Input for `authority.impact_contract_audit.scan`."""

    paths: list[str] = Field(default_factory=list)

    @field_validator("paths", mode="before")
    @classmethod
    def _normalize_paths(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if isinstance(value, (list, tuple)):
            out: list[str] = []
            for entry in value:
                text = str(entry or "").strip()
                if text:
                    out.append(text)
            return out
        raise ValueError("paths must be a list of strings")


def handle_scan_authority_impact_contract_audit(
    command: ScanAuthorityImpactContractAudit,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    result = audit_authority_impact_contract_coverage(conn, paths=command.paths)
    payload = result.to_dict()
    payload["ok"] = True
    return payload


__all__ = [
    "ScanAuthorityImpactContractAudit",
    "handle_scan_authority_impact_contract_audit",
]
