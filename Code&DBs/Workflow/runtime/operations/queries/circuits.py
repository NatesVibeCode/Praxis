from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class QueryCircuitStates(BaseModel):
    provider_slug: str | None = None

    @field_validator("provider_slug", mode="before")
    @classmethod
    def _normalize_optional_provider_slug(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("provider_slug must be a non-empty string when provided")
        return value.strip().lower()


class QueryCircuitHistory(BaseModel):
    provider_slug: str | None = None

    @field_validator("provider_slug", mode="before")
    @classmethod
    def _normalize_optional_provider_slug(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("provider_slug must be a non-empty string when provided")
        return value.strip().lower()


class QueryProviderControlPlane(BaseModel):
    runtime_profile_ref: str = "praxis"
    provider_slug: str | None = None
    job_type: str | None = None
    transport_type: str | None = None
    model_slug: str | None = None

    @field_validator("runtime_profile_ref", mode="before")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: object) -> str:
        if value is None:
            return "praxis"
        if not isinstance(value, str) or not value.strip():
            raise ValueError("runtime_profile_ref must be a non-empty string")
        return value.strip()

    @field_validator("provider_slug", "job_type", "model_slug", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("control-plane filters must be non-empty strings when provided")
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


def handle_query_circuit_states(
    query: QueryCircuitStates,
    _subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_read import ProviderControlPlaneFrontdoor

    return ProviderControlPlaneFrontdoor().query_circuit_states(
        pg=_subsystems.get_pg_conn(),
        provider_slug=query.provider_slug,
    )


def _iso(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def handle_query_circuit_history(
    query: QueryCircuitHistory,
    subsystems: Any,
) -> dict[str, Any]:
    rows = subsystems.get_pg_conn().execute(
        """
        SELECT
            operator_decision_id,
            decision_key,
            decision_kind,
            decision_status,
            rationale,
            decided_by,
            decision_source,
            effective_from,
            effective_to,
            decided_at,
            created_at,
            updated_at,
            decision_scope_kind,
            decision_scope_ref
        FROM operator_decisions
        WHERE decision_scope_kind = 'provider'
          AND decision_kind IN (
                'circuit_breaker_reset',
                'circuit_breaker_force_open',
                'circuit_breaker_force_closed'
          )
          AND ($1::text = '' OR decision_scope_ref = $1)
        ORDER BY decided_at DESC, created_at DESC, operator_decision_id DESC
        """,
        query.provider_slug or "",
    )
    history: list[dict[str, object]] = []
    for row in rows:
        provider_slug = str(row.get("decision_scope_ref") or "").strip().lower()
        history.append(
            {
                "provider_slug": provider_slug,
                "operator_decision_id": str(row.get("operator_decision_id") or ""),
                "decision_key": str(row.get("decision_key") or ""),
                "decision_kind": str(row.get("decision_kind") or ""),
                "decision_status": str(row.get("decision_status") or ""),
                "rationale": str(row.get("rationale") or ""),
                "decided_by": str(row.get("decided_by") or ""),
                "decision_source": str(row.get("decision_source") or ""),
                "effective_from": _iso(row.get("effective_from")),
                "effective_to": _iso(row.get("effective_to")),
                "decided_at": _iso(row.get("decided_at")),
                "created_at": _iso(row.get("created_at")),
                "updated_at": _iso(row.get("updated_at")),
                "decision_scope_kind": str(row.get("decision_scope_kind") or ""),
                "decision_scope_ref": provider_slug,
            }
        )
    return {"history": history}


def _row_dict(row: object) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if is_dataclass(row):
        return asdict(row)
    if hasattr(row, "items"):
        return dict(row.items())
    return dict(row)  # type: ignore[arg-type]


def _query_transport_admissions(
    conn: Any,
    *,
    provider_slug: str | None,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            provider_slug,
            adapter_type,
            status,
            admitted_by_policy,
            policy_reason,
            decision_ref,
            credential_sources,
            probe_contract,
            updated_at
        FROM provider_transport_admissions
        WHERE ($1::text IS NULL OR provider_slug = $1)
        ORDER BY provider_slug, adapter_type
        """,
        provider_slug,
    )
    return [_row_dict(row) for row in rows or ()]


def _query_task_routes(
    conn: Any,
    *,
    provider_slug: str | None,
    job_type: str | None,
    model_slug: str | None,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            task_type,
            provider_slug,
            model_slug,
            permitted,
            rank,
            temperature,
            max_tokens,
            reasoning_control,
            route_source,
            route_tier,
            latency_class,
            cost_per_m_tokens,
            rationale,
            updated_at
        FROM task_type_routing
        WHERE ($1::text IS NULL OR provider_slug = $1)
          AND ($2::text IS NULL OR task_type = $2)
          AND ($3::text IS NULL OR model_slug = $3)
        ORDER BY task_type, rank, provider_slug, model_slug
        """,
        provider_slug,
        job_type,
        model_slug,
    )
    return [_row_dict(row) for row in rows or ()]


def _circuit_state_for(
    circuits: dict[str, Any],
    provider_slug: str,
) -> dict[str, Any]:
    state = circuits.get(provider_slug)
    return dict(state) if isinstance(state, dict) else {}


def _circuit_blocks_provider(circuit: dict[str, Any]) -> bool:
    state = str(circuit.get("state") or "").strip().upper()
    manual = circuit.get("manual_override")
    manual_state = ""
    if isinstance(manual, dict):
        manual_state = str(
            manual.get("state")
            or manual.get("override_state")
            or manual.get("decision_kind")
            or ""
        ).strip().upper()
    return state == "OPEN" or manual_state in {
        "OPEN",
        "FORCE_OPEN",
        "CIRCUIT_BREAKER_FORCE_OPEN",
    }


def _route_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("job_type") or row.get("task_type") or ""),
        str(row.get("provider_slug") or ""),
        str(row.get("model_slug") or ""),
    )


def _task_route_rank_index(task_routes: list[dict[str, Any]]) -> dict[tuple[str, str, str], int]:
    index: dict[tuple[str, str, str], int] = {}
    for route in task_routes:
        try:
            rank = int(route.get("rank") or 999)
        except (TypeError, ValueError):
            rank = 999
        index[_route_key(route)] = rank
    return index


def _capability_matrix(
    catalog_rows: list[dict[str, Any]],
    *,
    circuits: dict[str, Any],
) -> list[dict[str, Any]]:
    matrix: list[dict[str, Any]] = []
    for row in catalog_rows:
        provider_slug = str(row.get("provider_slug") or "")
        catalog_state = str(row.get("availability_state") or "available")
        circuit = _circuit_state_for(circuits, provider_slug)
        blocked_reasons: list[str] = []
        if catalog_state != "available":
            blocked_reasons.append(str(row.get("reason_code") or "catalog.unavailable"))
        if _circuit_blocks_provider(circuit):
            blocked_reasons.append("circuit_breaker.open")
        effective_state = "blocked" if blocked_reasons else "available"
        matrix.append(
            {
                "job_type": row.get("job_type"),
                "type": row.get("transport_type"),
                "transport_type": row.get("transport_type"),
                "adapter_type": row.get("adapter_type"),
                "provider": provider_slug,
                "provider_slug": provider_slug,
                "model": row.get("model_slug"),
                "model_slug": row.get("model_slug"),
                "model_version": row.get("model_version"),
                "cost_structure": row.get("cost_structure"),
                "cost_metadata": row.get("cost_metadata") or {},
                "catalog_availability_state": catalog_state,
                "effective_availability_state": effective_state,
                "blocked_reasons": blocked_reasons,
                "reason_code": row.get("reason_code"),
                "candidate_ref": row.get("candidate_ref"),
                "provider_ref": row.get("provider_ref"),
                "source_refs": row.get("source_refs") or [],
                "projection_ref": row.get("projection_ref"),
                "projected_at": row.get("projected_at"),
                "circuit": circuit,
            }
        )
    return matrix


def _route_explanation(
    *,
    catalog_rows: list[dict[str, Any]],
    task_routes: list[dict[str, Any]],
    circuits: dict[str, Any],
) -> list[dict[str, Any]]:
    rank_by_key = _task_route_rank_index(task_routes)
    catalog_by_key = {_route_key(row): row for row in catalog_rows}
    keys = set(catalog_by_key)
    keys.update(rank_by_key)
    rows: list[dict[str, Any]] = []
    for key in sorted(keys):
        job_type, provider_slug, model_slug = key
        catalog_row = catalog_by_key.get(key)
        circuit = _circuit_state_for(circuits, provider_slug)
        removed_reasons: list[str] = []
        adapter_type = None
        transport_type = None
        cost_structure = None
        model_version = None
        if catalog_row is None:
            removed_reasons.append("catalog.missing")
        else:
            adapter_type = catalog_row.get("adapter_type")
            transport_type = catalog_row.get("transport_type")
            cost_structure = catalog_row.get("cost_structure")
            model_version = catalog_row.get("model_version")
            if str(catalog_row.get("availability_state") or "") != "available":
                removed_reasons.append(
                    str(catalog_row.get("reason_code") or "catalog.unavailable")
                )
        if _circuit_blocks_provider(circuit):
            removed_reasons.append("circuit_breaker.open")
        if key not in rank_by_key:
            removed_reasons.append("task_type_routing.missing")
        rows.append(
            {
                "job_type": job_type,
                "provider_slug": provider_slug,
                "model_slug": model_slug,
                "model_version": model_version,
                "transport_type": transport_type,
                "adapter_type": adapter_type,
                "cost_structure": cost_structure,
                "rank": rank_by_key.get(key),
                "considered": True,
                "available": not removed_reasons,
                "removed_reasons": removed_reasons,
                "selected": False,
                "circuit": circuit,
            }
        )

    selected_by_job_type: set[str] = set()
    for row in sorted(
        rows,
        key=lambda item: (
            str(item.get("job_type") or ""),
            int(item.get("rank") or 999),
            str(item.get("provider_slug") or ""),
            str(item.get("model_slug") or ""),
        ),
    ):
        job_type = str(row.get("job_type") or "")
        if row.get("available") and job_type not in selected_by_job_type:
            row["selected"] = True
            selected_by_job_type.add(job_type)
    return rows


def handle_query_provider_control_plane(
    query: QueryProviderControlPlane,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_read import ProviderControlPlaneFrontdoor

    return ProviderControlPlaneFrontdoor().query_provider_control_plane(
        pg=subsystems.get_pg_conn(),
        runtime_profile_ref=query.runtime_profile_ref,
        provider_slug=query.provider_slug,
        job_type=query.job_type,
        transport_type=query.transport_type,
        model_slug=query.model_slug,
    )


__all__ = [
    "QueryCircuitHistory",
    "QueryCircuitStates",
    "QueryProviderControlPlane",
    "handle_query_circuit_history",
    "handle_query_circuit_states",
    "handle_query_provider_control_plane",
]
