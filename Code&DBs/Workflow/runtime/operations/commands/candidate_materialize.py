"""Gateway command for materializing code-change candidates."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.workflow.candidate_materialization import materialize_candidate


class MaterializeCodeChangeCandidate(BaseModel):
    """Input for `code_change_candidate.materialize`."""

    candidate_id: str = Field(..., min_length=1)
    materialized_by: str = Field(default="system:code_change_candidate.materialize", min_length=1)
    repo_root: str | None = None

    @field_validator("candidate_id", "materialized_by", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        return str(value or "").strip()

    @field_validator("repo_root", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


def handle_materialize_candidate(
    command: MaterializeCodeChangeCandidate,
    subsystems: Any,
) -> dict[str, Any]:
    """Run guarded source materialization for one code-change candidate."""

    repo_root = Path(command.repo_root).resolve() if command.repo_root else None
    return materialize_candidate(
        subsystems.get_pg_conn(),
        candidate_id=command.candidate_id,
        materialized_by=command.materialized_by,
        repo_root=repo_root,
    )


__all__ = [
    "MaterializeCodeChangeCandidate",
    "handle_materialize_candidate",
]
