"""Legacy import path for materialize artifact persistence.

The table authority is `materialize_artifacts`. This module exists only so
older imports land on the same repository instead of querying retired
`compile_artifacts` storage.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .materialize_artifact_repository import (
    PostgresCompileArtifactRepository as _MaterializeArtifactRepository,
)
from .validators import _require_text


def _with_compile_id_alias(row: dict[str, Any]) -> dict[str, Any]:
    if "materialize_artifact_id" in row and "compile_artifact_id" not in row:
        row["compile_artifact_id"] = row["materialize_artifact_id"]
    return row


class PostgresCompileArtifactRepository(_MaterializeArtifactRepository):
    """Compatibility adapter over the materialize artifact repository."""

    def upsert_compile_artifact(
        self,
        *,
        compile_artifact_id: str | None = None,
        materialize_artifact_id: str | None = None,
        artifact_kind: str,
        artifact_ref: str,
        revision_ref: str,
        parent_artifact_ref: str | None,
        input_fingerprint: str,
        content_hash: str,
        authority_refs: Sequence[str],
        payload: Mapping[str, Any],
        decision_ref: str,
    ) -> str:
        artifact_id = materialize_artifact_id or compile_artifact_id
        return super().upsert_compile_artifact(
            materialize_artifact_id=_require_text(
                artifact_id,
                field_name="materialize_artifact_id",
            ),
            artifact_kind=artifact_kind,
            artifact_ref=artifact_ref,
            revision_ref=revision_ref,
            parent_artifact_ref=parent_artifact_ref,
            input_fingerprint=input_fingerprint,
            content_hash=content_hash,
            authority_refs=authority_refs,
            payload=payload,
            decision_ref=decision_ref,
        )

    def load_compile_artifacts_for_input(
        self,
        *,
        artifact_kind: str,
        input_fingerprint: str,
    ) -> list[dict[str, Any]]:
        return [
            _with_compile_id_alias(dict(row))
            for row in super().load_compile_artifacts_for_input(
                artifact_kind=artifact_kind,
                input_fingerprint=input_fingerprint,
            )
        ]

    def load_compile_artifact_by_revision(
        self,
        *,
        artifact_kind: str,
        revision_ref: str,
    ) -> dict[str, Any] | None:
        row = super().load_compile_artifact_by_revision(
            artifact_kind=artifact_kind,
            revision_ref=revision_ref,
        )
        return None if row is None else _with_compile_id_alias(dict(row))

    def load_compile_artifact_history(
        self,
        *,
        artifact_kind: str,
        artifact_ref: str | None = None,
        input_fingerprint: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return [
            _with_compile_id_alias(dict(row))
            for row in super().load_compile_artifact_history(
                artifact_kind=artifact_kind,
                artifact_ref=artifact_ref,
                input_fingerprint=input_fingerprint,
                limit=limit,
            )
        ]

    def load_compile_artifact_lineage(
        self,
        *,
        artifact_kind: str,
        revision_ref: str,
    ) -> list[dict[str, Any]]:
        return [
            _with_compile_id_alias(dict(row))
            for row in super().load_compile_artifact_lineage(
                artifact_kind=artifact_kind,
                revision_ref=revision_ref,
            )
        ]


__all__ = ["PostgresCompileArtifactRepository"]
