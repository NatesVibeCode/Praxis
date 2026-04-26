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


__all__ = [
    "QueryTransportSupport",
    "QueryWorkAssignmentMatrix",
    "handle_query_transport_support",
    "handle_query_work_assignment_matrix",
]
