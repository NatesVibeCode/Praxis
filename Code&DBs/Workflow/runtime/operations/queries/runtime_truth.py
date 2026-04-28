"""CQRS query handlers for runtime truth and remediation planning."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.runtime_truth import (
    build_firecheck,
    build_remediation_plan,
    build_runtime_truth_snapshot,
)


class QueryRuntimeTruthSnapshot(BaseModel):
    run_id: str | None = None
    since_minutes: int = 60
    heartbeat_fresh_seconds: int = 60
    manifest_audit_limit: int = 10

    @field_validator("run_id", mode="before")
    @classmethod
    def _normalize_run_id(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("run_id must be a non-empty string when provided")
        return value.strip()

    @field_validator("since_minutes", "heartbeat_fresh_seconds", mode="before")
    @classmethod
    def _normalize_positive_int(cls, value: object) -> int:
        if value in (None, ""):
            return 60
        if isinstance(value, bool):
            raise ValueError("numeric fields must be positive integers")
        try:
            return max(1, min(int(value), 24 * 60))
        except (TypeError, ValueError) as exc:
            raise ValueError("numeric fields must be positive integers") from exc

    @field_validator("manifest_audit_limit", mode="before")
    @classmethod
    def _normalize_manifest_audit_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 10
        if isinstance(value, bool):
            raise ValueError("manifest_audit_limit must be a positive integer")
        try:
            return max(1, min(int(value), 100))
        except (TypeError, ValueError) as exc:
            raise ValueError("manifest_audit_limit must be a positive integer") from exc


class QueryFirecheck(BaseModel):
    run_id: str | None = None
    since_minutes: int = 60
    heartbeat_fresh_seconds: int = 60

    @field_validator("run_id", mode="before")
    @classmethod
    def _normalize_run_id(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("run_id must be a non-empty string when provided")
        return value.strip()

    @field_validator("since_minutes", "heartbeat_fresh_seconds", mode="before")
    @classmethod
    def _normalize_positive_int(cls, value: object) -> int:
        if value in (None, ""):
            return 60
        if isinstance(value, bool):
            raise ValueError("numeric fields must be positive integers")
        try:
            return max(1, min(int(value), 24 * 60))
        except (TypeError, ValueError) as exc:
            raise ValueError("numeric fields must be positive integers") from exc


class QueryRemediationPlan(BaseModel):
    failure_type: str | None = None
    failure_code: str | None = None
    stderr: str | None = Field(default=None, max_length=4000)
    run_id: str | None = None

    @field_validator("failure_type", "failure_code", "stderr", "run_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("text fields must be non-empty strings when provided")
        return value.strip()


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    return getter() if callable(getter) else None


def handle_query_runtime_truth_snapshot(
    query: QueryRuntimeTruthSnapshot,
    subsystems: Any,
) -> dict[str, Any]:
    return build_runtime_truth_snapshot(
        _pg_conn(subsystems),
        run_id=query.run_id,
        since_minutes=query.since_minutes,
        heartbeat_fresh_seconds=query.heartbeat_fresh_seconds,
        manifest_audit_limit=query.manifest_audit_limit,
    )


def handle_query_firecheck(
    query: QueryFirecheck,
    subsystems: Any,
) -> dict[str, Any]:
    return build_firecheck(
        _pg_conn(subsystems),
        run_id=query.run_id,
        since_minutes=query.since_minutes,
        heartbeat_fresh_seconds=query.heartbeat_fresh_seconds,
    )


def handle_query_remediation_plan(
    query: QueryRemediationPlan,
    subsystems: Any,
) -> dict[str, Any]:
    return build_remediation_plan(
        _pg_conn(subsystems),
        failure_type=query.failure_type,
        failure_code=query.failure_code,
        stderr=query.stderr,
        run_id=query.run_id,
    )


__all__ = [
    "QueryFirecheck",
    "QueryRemediationPlan",
    "QueryRuntimeTruthSnapshot",
    "handle_query_firecheck",
    "handle_query_remediation_plan",
    "handle_query_runtime_truth_snapshot",
]
