"""CQRS queries for execution target authority and dispatch options."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.execution_targets import (
    attach_candidate_set_hash,
    candidate_set_hash,
    enrich_dispatch_candidate,
    execution_profiles_list,
    execution_targets_list,
    resolve_target_for_transport,
)
from storage.postgres.task_type_routing_repository import (
    PostgresTaskTypeRoutingRepository,
)


class QueryExecutionTargetsList(BaseModel):
    include_disabled: bool = Field(
        default=False,
        description="Include non-admitted targets such as process_sandbox.",
    )


class QueryExecutionTargetsResolve(BaseModel):
    transport_type: str | None = Field(default=None)
    sandbox_provider: str | None = Field(default=None)
    workspace_materialization: str | None = Field(default=None)
    explicit_target_ref: str | None = Field(default=None)
    explicit_profile_ref: str | None = Field(default=None)
    fallback_allowed: bool = Field(default=False)

    @field_validator(
        "transport_type",
        "sandbox_provider",
        "workspace_materialization",
        "explicit_target_ref",
        "explicit_profile_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("target resolution fields must be strings")
        cleaned = value.strip()
        return cleaned or None


class QueryDispatchOptionsList(BaseModel):
    task_slug: str = Field(default="auto/chat")
    workload_kind: str = Field(default="chat")
    include_disabled: bool = Field(default=False)
    include_cli: bool = Field(default=True)

    @field_validator("task_slug", "workload_kind", mode="before")
    @classmethod
    def _normalize_text(cls, value: object) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("dispatch option filters must be strings")
        return value.strip()


def _row_to_candidate(row: Any) -> dict[str, Any]:
    return enrich_dispatch_candidate(
        {
            "provider_slug": row.get("provider_slug"),
            "model_slug": row.get("model_slug"),
            "transport_type": row.get("transport_type"),
            "task_type": row.get("task_type"),
            "sub_task_type": row.get("sub_task_type"),
            "rank": row.get("rank"),
            "permitted": row.get("permitted"),
            "route_health_score": row.get("route_health_score"),
            "benchmark_score": row.get("benchmark_score"),
            "route_tier": row.get("route_tier"),
            "latency_class": row.get("latency_class"),
            "cost_per_m_tokens": row.get("cost_per_m_tokens"),
        }
    )


def handle_query_execution_targets_list(
    query: QueryExecutionTargetsList,
    subsystems: Any,
) -> dict[str, Any]:
    return {
        "ok": True,
        "operation": "execution.targets.list",
        "targets": execution_targets_list(include_disabled=query.include_disabled),
        "profiles": execution_profiles_list(include_disabled=query.include_disabled),
    }


def handle_query_execution_targets_resolve(
    query: QueryExecutionTargetsResolve,
    subsystems: Any,
) -> dict[str, Any]:
    try:
        resolution = resolve_target_for_transport(
            transport_type=query.transport_type,
            sandbox_provider=query.sandbox_provider,
            workspace_materialization=query.workspace_materialization,
            explicit_target_ref=query.explicit_target_ref,
            explicit_profile_ref=query.explicit_profile_ref,
        )
    except ValueError as exc:
        if not query.fallback_allowed:
            return {
                "ok": False,
                "operation": "execution.targets.resolve",
                "error_code": "execution_target_resolution.rejected",
                "error": str(exc),
                "fallback_allowed": False,
            }
        resolution = resolve_target_for_transport(
            transport_type=query.transport_type,
            sandbox_provider=query.sandbox_provider,
            workspace_materialization=query.workspace_materialization,
        )
        payload = resolution.to_dict()
        payload["target_resolution_reason"] = "fallback_after_explicit_rejected"
        return {
            "ok": True,
            "operation": "execution.targets.resolve",
            "fallback_allowed": True,
            "resolution": payload,
        }

    return {
        "ok": True,
        "operation": "execution.targets.resolve",
        "fallback_allowed": query.fallback_allowed,
        "resolution": resolution.to_dict(),
    }


def handle_query_dispatch_options_list(
    query: QueryDispatchOptionsList,
    subsystems: Any,
) -> dict[str, Any]:
    if (query.workload_kind or "chat") != "chat":
        return {
            "ok": False,
            "operation": "execution.dispatch_options.list",
            "error_code": "dispatch_options.workload_kind_unsupported",
            "error": "dispatch options currently support workload_kind=chat",
        }

    conn = subsystems.get_pg_conn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    task_slug = query.task_slug or "auto/chat"
    task_type = task_slug.removeprefix("auto/")
    rows = repo.load_routes(task_type=task_type)
    candidates = [_row_to_candidate(row) for row in rows]
    if not query.include_disabled:
        candidates = [candidate for candidate in candidates if candidate.get("permitted")]
    if not query.include_cli:
        candidates = [
            candidate
            for candidate in candidates
            if candidate.get("transport_type") == "API"
        ]
    candidates.sort(
        key=lambda candidate: (
            candidate.get("rank") if candidate.get("rank") is not None else 9999,
            -(candidate.get("route_health_score") or 0.0),
        )
    )
    candidates = attach_candidate_set_hash(candidates)
    digest = candidate_set_hash(candidates)

    return {
        "ok": True,
        "operation": "execution.dispatch_options.list",
        "workload_kind": "chat",
        "task_slug": task_slug,
        "task_type": task_type,
        "include_disabled": query.include_disabled,
        "include_cli": query.include_cli,
        "candidate_set_hash": digest,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }


__all__ = [
    "QueryDispatchOptionsList",
    "QueryExecutionTargetsList",
    "QueryExecutionTargetsResolve",
    "handle_query_dispatch_options_list",
    "handle_query_execution_targets_list",
    "handle_query_execution_targets_resolve",
]
