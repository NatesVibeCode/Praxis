"""CQRS query handler for Client Operating Model operator read models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.operator_surfaces.client_operating_model import (
    OperatorSurfaceValidationError,
    build_cartridge_status_view,
    build_identity_authority_view,
    build_managed_runtime_accounting_summary,
    build_next_safe_actions_view,
    build_object_truth_view,
    build_sandbox_drift_view,
    build_simulation_timeline_view,
    build_system_census_view,
    build_verifier_results_view,
    validate_workflow_builder_graph,
)
from storage.postgres.client_operating_model_repository import (
    list_operator_view_snapshots,
)


ClientOperatingModelView = Literal[
    "system_census",
    "object_truth",
    "identity_authority",
    "simulation_timeline",
    "verifier_results",
    "sandbox_drift",
    "cartridge_status",
    "managed_runtime",
    "next_safe_actions",
    "workflow_builder_validation",
]


_VIEW_BUILDERS = {
    "system_census": build_system_census_view,
    "object_truth": build_object_truth_view,
    "identity_authority": build_identity_authority_view,
    "simulation_timeline": build_simulation_timeline_view,
    "verifier_results": build_verifier_results_view,
    "sandbox_drift": build_sandbox_drift_view,
    "cartridge_status": build_cartridge_status_view,
    "managed_runtime": build_managed_runtime_accounting_summary,
    "next_safe_actions": build_next_safe_actions_view,
    "workflow_builder_validation": validate_workflow_builder_graph,
}


class QueryClientOperatingModelView(BaseModel):
    """Build one bounded operator read model from provided evidence payloads."""

    view: ClientOperatingModelView = Field(
        description="Operator read model to build.",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="View-specific evidence payload. This operation does not persist or mutate it.",
    )
    generated_at: str | None = Field(
        default=None,
        description="Optional timestamp to make generated payloads deterministic.",
    )
    permission_scope: dict[str, Any] = Field(
        default_factory=dict,
        description="Permission scope envelope; supports visibility and redacted_fields.",
    )
    correlation_ids: list[str] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)

    @field_validator("inputs", "permission_scope", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("inputs and permission_scope must be JSON objects")

    @field_validator("correlation_ids", "evidence_refs", mode="before")
    @classmethod
    def _normalize_string_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("correlation_ids and evidence_refs must be lists")
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("generated_at", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("generated_at must be a non-empty string when provided")
        return value.strip()


class QueryClientOperatingModelSnapshotRead(BaseModel):
    """Read persisted Client Operating Model operator-view snapshots."""

    snapshot_ref: str | None = Field(
        default=None,
        description="Exact stored snapshot ref.",
    )
    snapshot_digest: str | None = Field(
        default=None,
        description="Exact stored snapshot digest.",
    )
    view: ClientOperatingModelView | None = Field(
        default=None,
        description="Optional view filter for latest/read-list queries.",
    )
    scope_ref: str | None = Field(
        default=None,
        description="Optional permission scope filter.",
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum snapshots to return.",
    )

    @field_validator("snapshot_ref", "snapshot_digest", "scope_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("snapshot filters must be non-empty strings when provided")
        return value.strip()

    @model_validator(mode="after")
    def _require_useful_filter(self) -> "QueryClientOperatingModelSnapshotRead":
        if not (self.snapshot_ref or self.snapshot_digest or self.view):
            raise ValueError("provide snapshot_ref, snapshot_digest, or view")
        return self


def handle_client_operating_model_view(
    query: QueryClientOperatingModelView,
    subsystems: Any,
) -> dict[str, Any]:
    """Return one read-only Client Operating Model operator view."""

    _ = subsystems
    builder = _VIEW_BUILDERS[query.view]
    payload = dict(query.inputs)
    payload.update(
        {
            "generated_at": query.generated_at,
            "permission_scope": query.permission_scope,
            "correlation_ids": query.correlation_ids,
            "evidence_refs": query.evidence_refs,
        }
    )
    try:
        view = builder(**payload)
    except OperatorSurfaceValidationError as exc:
        return {
            "ok": False,
            "operation": "client_operating_model_operator_view",
            "view": query.view,
            "error_code": exc.reason_code,
            "error": str(exc),
            "details": exc.details,
        }
    except (TypeError, ValueError) as exc:
        return {
            "ok": False,
            "operation": "client_operating_model_operator_view",
            "view": query.view,
            "error_code": "client_operating_model.invalid_view_inputs",
            "error": str(exc),
        }

    return {
        "ok": True,
        "operation": "client_operating_model_operator_view",
        "view": query.view,
        "view_id": view.get("view_id"),
        "state": view.get("state"),
        "freshness": view.get("freshness"),
        "operator_view": view,
    }


def handle_client_operating_model_snapshot_read(
    query: QueryClientOperatingModelSnapshotRead,
    subsystems: Any,
) -> dict[str, Any]:
    """Read stored Client Operating Model operator-view snapshots."""

    snapshots = list_operator_view_snapshots(
        subsystems.get_pg_conn(),
        snapshot_ref=query.snapshot_ref,
        snapshot_digest=query.snapshot_digest,
        view=query.view,
        scope_ref=query.scope_ref,
        limit=query.limit,
    )
    exact_lookup = bool(query.snapshot_ref or query.snapshot_digest)
    return {
        "ok": True,
        "operation": "client_operating_model_operator_view_snapshot_read",
        "count": len(snapshots),
        "snapshot": snapshots[0] if snapshots and exact_lookup else None,
        "snapshots": snapshots,
        "filters": {
            "snapshot_ref": query.snapshot_ref,
            "snapshot_digest": query.snapshot_digest,
            "view": query.view,
            "scope_ref": query.scope_ref,
            "limit": query.limit,
        },
    }


__all__ = [
    "QueryClientOperatingModelView",
    "QueryClientOperatingModelSnapshotRead",
    "handle_client_operating_model_snapshot_read",
    "handle_client_operating_model_view",
]
