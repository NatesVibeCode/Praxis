from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator
from surfaces.api.operator_read import TransportSupportFrontdoor


class QueryTransportSupport(BaseModel):
    provider_slug: str | None = None
    model_slug: str | None = None
    runtime_profile_ref: str = "praxis"
    jobs: list[dict[str, Any]] | None = None

    @field_validator("provider_slug", "model_slug", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("transport-support filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("runtime_profile_ref", mode="before")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: object) -> str:
        if value is None:
            return "praxis"
        if not isinstance(value, str) or not value.strip():
            return "praxis"
        return value.strip()


class QueryWorkAssignmentMatrix(BaseModel):
    status: str | None = None
    audit_group: str | None = None
    recommended_model_tier: str | None = None
    open_only: bool = True
    limit: int = 100

    @field_validator("status", "audit_group", "recommended_model_tier", mode="before")
    @classmethod
    def _normalize_optional_filter(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("work-assignment filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value is None:
            return 100
        if isinstance(value, bool):
            raise ValueError("limit must be an integer")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc
        if parsed < 1 or parsed > 500:
            raise ValueError("limit must be between 1 and 500")
        return parsed


class QueryModelAccessControlMatrix(BaseModel):
    runtime_profile_ref: str = "praxis"
    job_type: str | None = None
    transport_type: str | None = None
    provider_slug: str | None = None
    model_slug: str | None = None
    control_state: str | None = None
    limit: int = 200

    @field_validator("runtime_profile_ref", mode="before")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: object) -> str:
        if value is None:
            return "praxis"
        if not isinstance(value, str) or not value.strip():
            raise ValueError("runtime_profile_ref must be a non-empty string")
        return value.strip()

    @field_validator("job_type", "provider_slug", "model_slug", mode="before")
    @classmethod
    def _normalize_optional_text_filter(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("model-access filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("transport_type", mode="before")
    @classmethod
    def _normalize_transport_type(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("transport_type must be CLI or API when provided")
        normalized = value.strip().upper()
        if normalized not in {"CLI", "API"}:
            raise ValueError("transport_type must be CLI or API")
        return normalized

    @field_validator("control_state", mode="before")
    @classmethod
    def _normalize_control_state(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("control_state must be on or off when provided")
        normalized = value.strip().lower()
        if normalized not in {"on", "off"}:
            raise ValueError("control_state must be on or off")
        return normalized

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_model_access_limit(cls, value: object) -> int:
        if value is None:
            return 200
        if isinstance(value, bool):
            raise ValueError("limit must be an integer")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc
        if parsed < 1 or parsed > 1000:
            raise ValueError("limit must be between 1 and 1000")
        return parsed


def handle_query_transport_support(
    query: QueryTransportSupport,
    subsystems: Any,
) -> dict[str, Any]:
    return TransportSupportFrontdoor().query_transport_support(
        health_mod=subsystems.get_health_mod(),
        pg=subsystems.get_pg_conn(),
        provider_filter=query.provider_slug,
        model_filter=query.model_slug,
        runtime_profile_ref=query.runtime_profile_ref,
        jobs=query.jobs,
    )


def _row_dict(row: object) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "items"):
        return dict(row.items())
    return dict(row)  # type: ignore[arg-type]


def handle_query_work_assignment_matrix(
    query: QueryWorkAssignmentMatrix,
    subsystems: Any,
) -> dict[str, Any]:
    """Read the DB-backed model-tier assignment matrix for open work."""

    conn = subsystems.get_pg_conn()
    rows = [
        _row_dict(row)
        for row in conn.execute(
            """
            SELECT
                item_kind,
                item_id,
                item_key,
                title,
                status,
                severity,
                priority,
                category,
                audit_group,
                group_sort_order,
                recommended_model_tier,
                recommended_model_tier_group,
                suggested_sequence,
                assignment_reason,
                task_type,
                can_delegate_to_less_than_frontier,
                grouping_source,
                implementation_status,
                visibility_state,
                updated_at,
                source_ref
            FROM work_item_assignment_matrix
            WHERE ($1::text IS NULL OR status = $1)
              AND ($2::text IS NULL OR audit_group = $2)
              AND (
                    $3::text IS NULL
                 OR recommended_model_tier = $3
                 OR recommended_model_tier_group = $3
              )
              AND (
                    $4::boolean IS NOT TRUE
                 OR visibility_state = 'active'
              )
            ORDER BY
                group_sort_order,
                recommended_model_tier_rank,
                suggested_sequence NULLS LAST,
                severity,
                priority,
                updated_at DESC,
                item_id
            LIMIT $5
            """,
            query.status,
            query.audit_group,
            query.recommended_model_tier,
            query.open_only,
            query.limit,
        )
        or ()
    ]

    group_counts: dict[str, dict[str, Any]] = {}
    tier_counts: dict[str, int] = {}
    for row in rows:
        group = str(row.get("audit_group") or "unassigned")
        tier = str(row.get("recommended_model_tier_group") or "unclassified")
        group_payload = group_counts.setdefault(
            group,
            {"audit_group": group, "count": 0, "tiers": {}},
        )
        group_payload["count"] += 1
        group_payload["tiers"][tier] = int(group_payload["tiers"].get(tier, 0)) + 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    return {
        "operation": "operator.work_assignment_matrix",
        "authority": "view.work_item_assignment_matrix",
        "filters": {
            "status": query.status,
            "audit_group": query.audit_group,
            "recommended_model_tier": query.recommended_model_tier,
            "open_only": query.open_only,
            "limit": query.limit,
        },
        "rows": rows,
        "groups": sorted(
            group_counts.values(),
            key=lambda item: (str(item.get("audit_group") or ""),),
        ),
        "tier_counts": dict(sorted(tier_counts.items())),
        "count": len(rows),
        "columns": [
            "audit_group",
            "recommended_model_tier",
            "recommended_model_tier_group",
            "suggested_sequence",
            "assignment_reason",
            "task_type",
            "can_delegate_to_less_than_frontier",
        ],
    }


def handle_query_model_access_control_matrix(
    query: QueryModelAccessControlMatrix,
    subsystems: Any,
) -> dict[str, Any]:
    """Read the live control-panel switchboard for model access."""

    conn = subsystems.get_pg_conn()
    rows = [
        _row_dict(row)
        for row in conn.execute(
            """
            SELECT
                runtime_profile_ref,
                job_type,
                transport_type,
                adapter_type,
                access_method,
                provider_slug,
                model_slug,
                model_version,
                cost_structure,
                cost_metadata,
                control_enabled,
                control_state,
                control_scope,
                control_is_explicit,
                control_reason_code,
                control_operator_message,
                control_decision_ref,
                candidate_ref,
                provider_ref,
                source_refs,
                projected_at,
                projection_ref
            FROM private_model_access_control_matrix
            WHERE runtime_profile_ref = $1
              AND ($2::text IS NULL OR job_type = $2)
              AND ($3::text IS NULL OR transport_type = $3)
              AND ($4::text IS NULL OR provider_slug = $4)
              AND ($5::text IS NULL OR model_slug = $5)
              AND ($6::text IS NULL OR control_state = $6)
            ORDER BY job_type, transport_type, provider_slug, model_slug, adapter_type
            LIMIT $7
            """,
            query.runtime_profile_ref,
            query.job_type,
            query.transport_type,
            query.provider_slug,
            query.model_slug,
            query.control_state,
            query.limit,
        )
        or ()
    ]

    by_state: dict[str, int] = {}
    by_job_type: dict[str, dict[str, int]] = {}
    by_transport: dict[str, dict[str, int]] = {}
    for row in rows:
        state = str(row.get("control_state") or "unknown")
        job_type = str(row.get("job_type") or "unknown")
        transport_type = str(row.get("transport_type") or "unknown")
        by_state[state] = by_state.get(state, 0) + 1
        job_counts = by_job_type.setdefault(job_type, {})
        job_counts[state] = job_counts.get(state, 0) + 1
        transport_counts = by_transport.setdefault(transport_type, {})
        transport_counts[state] = transport_counts.get(state, 0) + 1

    return {
        "operation": "operator.model_access_control_matrix",
        "authority": "view.private_model_access_control_matrix",
        "filters": {
            "runtime_profile_ref": query.runtime_profile_ref,
            "job_type": query.job_type,
            "transport_type": query.transport_type,
            "provider_slug": query.provider_slug,
            "model_slug": query.model_slug,
            "control_state": query.control_state,
            "limit": query.limit,
        },
        "rows": rows,
        "count": len(rows),
        "counts": {
            "by_control_state": dict(sorted(by_state.items())),
            "by_job_type": dict(sorted(by_job_type.items())),
            "by_transport_type": dict(sorted(by_transport.items())),
        },
        "columns": [
            "job_type",
            "transport_type",
            "adapter_type",
            "access_method",
            "provider_slug",
            "model_slug",
            "model_version",
            "cost_structure",
            "control_enabled",
            "control_state",
            "control_scope",
            "control_reason_code",
            "control_decision_ref",
        ],
    }


__all__ = [
    "QueryModelAccessControlMatrix",
    "QueryTransportSupport",
    "QueryWorkAssignmentMatrix",
    "handle_query_model_access_control_matrix",
    "handle_query_transport_support",
    "handle_query_work_assignment_matrix",
]
