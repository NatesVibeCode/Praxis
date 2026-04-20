from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.primitive_contracts import bug_query_default_open_only_backlog
from runtime.semantic_projection_subscriber import consume_semantic_projection_events
from storage.postgres.workflow_runtime_repository import reset_observability_metrics


def _resolved_env(subsystems: Any) -> dict[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


class ResetMetricsCommand(BaseModel):
    confirm: bool = False
    before_date: str | None = None


class BackfillBugReplayProvenanceCommand(BaseModel):
    limit: int | None = None
    open_only: bool = Field(default_factory=bug_query_default_open_only_backlog)
    receipt_limit: int = 1

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int | None:
        if value in (None, ""):
            return None
        try:
            return max(0, int(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer when provided") from exc

    @field_validator("receipt_limit", mode="before")
    @classmethod
    def _normalize_receipt_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 1
        try:
            return max(1, int(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("receipt_limit must be an integer") from exc


class BackfillSemanticBridgesCommand(BaseModel):
    include_object_relations: bool = True
    include_operator_decisions: bool = True
    include_roadmap_items: bool = True
    as_of: datetime | None = None


class RefreshSemanticProjectionCommand(BaseModel):
    limit: int = 100
    as_of: datetime | None = None

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 100
        try:
            return max(1, int(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc


def handle_reset_metrics(
    command: ResetMetricsCommand,
    subsystems: Any,
) -> dict[str, Any]:
    if not command.confirm:
        return {
            "error": (
                "Pass confirm=true to reset metrics. This truncates quality_rollups, "
                "agent_profiles, failure_catalog and zeros routing counters."
            )
        }
    return reset_observability_metrics(
        subsystems.get_pg_conn(),
        before_date=command.before_date,
    )


def handle_backfill_bug_replay_provenance(
    command: BackfillBugReplayProvenanceCommand,
    subsystems: Any,
) -> dict[str, Any]:
    bug_tracker = subsystems.get_bug_tracker()
    return {
        "backfill": bug_tracker.bulk_backfill_replay_provenance(
            limit=command.limit,
            open_only=command.open_only,
            receipt_limit=command.receipt_limit,
        )
    }


def handle_backfill_semantic_bridges(
    command: BackfillSemanticBridgesCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().backfill_semantic_bridges(
        include_object_relations=command.include_object_relations,
        include_operator_decisions=command.include_operator_decisions,
        include_roadmap_items=command.include_roadmap_items,
        as_of=command.as_of,
        env=_resolved_env(subsystems),
    )


def handle_refresh_semantic_projection(
    command: RefreshSemanticProjectionCommand,
    _subsystems: Any,
) -> dict[str, Any]:
    return {
        "semantic_projection_refresh": consume_semantic_projection_events(
            limit=command.limit,
            as_of=command.as_of,
        )
    }


__all__ = [
    "BackfillBugReplayProvenanceCommand",
    "BackfillSemanticBridgesCommand",
    "RefreshSemanticProjectionCommand",
    "ResetMetricsCommand",
    "handle_backfill_bug_replay_provenance",
    "handle_backfill_semantic_bridges",
    "handle_refresh_semantic_projection",
    "handle_reset_metrics",
]
