"""CQRS queries for Synthetic Environment authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.synthetic_environment import SyntheticEnvironmentError, diff_synthetic_environment
from storage.postgres.synthetic_environment_repository import (
    list_synthetic_environment_effects,
    list_synthetic_environments,
    load_synthetic_environment,
)


ReadAction = Literal["list_environments", "describe_environment", "list_effects", "diff"]


class QuerySyntheticEnvironmentRead(BaseModel):
    """Read Synthetic Environments, effects, current state, and diffs."""

    action: ReadAction = "list_environments"
    environment_ref: str | None = None
    namespace: str | None = None
    source_dataset_ref: str | None = None
    lifecycle_state: str | None = None
    effect_type: str | None = None
    compare_to: str | None = "seed"
    include_state: bool = True
    include_effects: bool = False
    limit: int = Field(default=50, ge=1, le=5000)

    @field_validator(
        "environment_ref",
        "namespace",
        "source_dataset_ref",
        "lifecycle_state",
        "effect_type",
        "compare_to",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QuerySyntheticEnvironmentRead":
        if self.action in {"describe_environment", "list_effects", "diff"} and not self.environment_ref:
            raise ValueError(f"environment_ref is required for {self.action}")
        if not self.compare_to:
            self.compare_to = "seed"
        return self


def _without_state(environment: dict[str, Any]) -> dict[str, Any]:
    if "seed_state" not in environment and "current_state" not in environment:
        return environment
    return {
        key: value
        for key, value in environment.items()
        if key not in {"seed_state", "current_state"}
    }


def handle_synthetic_environment_read(
    query: QuerySyntheticEnvironmentRead,
    subsystems: Any,
) -> dict[str, Any]:
    """Read Synthetic Environment authority records."""

    conn = subsystems.get_pg_conn()
    if query.action == "describe_environment":
        environment = load_synthetic_environment(conn, environment_ref=str(query.environment_ref))
        effects = (
            list_synthetic_environment_effects(
                conn,
                environment_ref=str(query.environment_ref),
                limit=query.limit,
            )
            if environment and query.include_effects
            else []
        )
        if environment and not query.include_state:
            environment = _without_state(environment)
        return {
            "ok": environment is not None,
            "operation": "synthetic_environment_read",
            "action": "describe_environment",
            "environment_ref": query.environment_ref,
            "environment": environment,
            "effects": effects,
            "error_code": None if environment is not None else "synthetic_environment.environment_not_found",
        }
    if query.action == "list_effects":
        effects = list_synthetic_environment_effects(
            conn,
            environment_ref=str(query.environment_ref),
            effect_type=query.effect_type,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "synthetic_environment_read",
            "action": "list_effects",
            "environment_ref": query.environment_ref,
            "count": len(effects),
            "effects": effects,
        }
    if query.action == "diff":
        environment = load_synthetic_environment(conn, environment_ref=str(query.environment_ref))
        if environment is None:
            return {
                "ok": False,
                "operation": "synthetic_environment_read",
                "action": "diff",
                "environment_ref": query.environment_ref,
                "error_code": "synthetic_environment.environment_not_found",
                "error": "environment_ref not found",
            }
        try:
            diff = diff_synthetic_environment(
                environment,
                compare_to=query.compare_to or "seed",
                limit=query.limit,
            )
        except SyntheticEnvironmentError as exc:
            return {
                "ok": False,
                "operation": "synthetic_environment_read",
                "action": "diff",
                "environment_ref": query.environment_ref,
                "error_code": exc.reason_code,
                "error": str(exc),
                "details": exc.details,
            }
        return {
            "ok": True,
            "operation": "synthetic_environment_read",
            "action": "diff",
            "environment_ref": query.environment_ref,
            "diff": diff,
        }
    environments = list_synthetic_environments(
        conn,
        namespace=query.namespace,
        source_dataset_ref=query.source_dataset_ref,
        lifecycle_state=query.lifecycle_state,
        limit=query.limit,
    )
    if not query.include_state:
        environments = [_without_state(environment) for environment in environments]
    return {
        "ok": True,
        "operation": "synthetic_environment_read",
        "action": "list_environments",
        "count": len(environments),
        "environments": environments,
        "filters": {
            "namespace": query.namespace,
            "source_dataset_ref": query.source_dataset_ref,
            "lifecycle_state": query.lifecycle_state,
            "limit": query.limit,
        },
    }


__all__ = [
    "QuerySyntheticEnvironmentRead",
    "handle_synthetic_environment_read",
]
