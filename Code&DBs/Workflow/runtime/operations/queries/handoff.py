from __future__ import annotations

from dataclasses import asdict
from typing import Any

from pydantic import BaseModel, Field

from runtime.materialize_artifacts import MaterializeArtifactError, MaterializeArtifactStore
from storage.postgres.materialize_artifact_repository import PostgresCompileArtifactRepository
from storage.postgres.subscription_repository import PostgresSubscriptionRepository


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    if not callable(getter):
        raise RuntimeError("handoff queries require Postgres authority")
    conn = getter()
    if conn is None:
        raise RuntimeError("handoff queries require Postgres authority")
    return conn


def _record_payload(record: Any) -> dict[str, Any]:
    return asdict(record)


class QueryHandoffLatestArtifact(BaseModel):
    artifact_kind: str
    artifact_ref: str | None = None
    input_fingerprint: str | None = None


class QueryHandoffArtifactLineage(BaseModel):
    artifact_kind: str
    revision_ref: str


class QueryHandoffConsumerStatus(BaseModel):
    subscription_id: str
    run_id: str
    limit: int = Field(default=20, ge=1)


class QueryHandoffArtifactHistory(BaseModel):
    artifact_kind: str
    artifact_ref: str | None = None
    input_fingerprint: str | None = None
    limit: int = Field(default=20, ge=1)


def handle_query_handoff_latest(
    query: QueryHandoffLatestArtifact,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    repository = PostgresCompileArtifactRepository(conn)
    if query.input_fingerprint:
        store = MaterializeArtifactStore(conn)
        try:
            reusable = store.load_reusable_artifact(
                artifact_kind=query.artifact_kind,
                input_fingerprint=query.input_fingerprint,
            )
        except MaterializeArtifactError as exc:
            raise RuntimeError(str(exc)) from exc
        if reusable is None:
            return {
                "artifact": None,
                "history": [],
                "count": 0,
                "scope": "reusable",
            }
        return {
            "artifact": _record_payload(reusable),
            "history": [_record_payload(reusable)],
            "count": 1,
            "scope": "reusable",
        }

    history = repository.load_compile_artifact_history(
        artifact_kind=query.artifact_kind,
        artifact_ref=query.artifact_ref,
        limit=1,
    )
    latest = history[0] if history else None
    return {
        "artifact": latest,
        "history": history,
        "count": len(history),
        "scope": "latest",
    }


def handle_query_handoff_artifact_lineage(
    query: QueryHandoffArtifactLineage,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    repository = PostgresCompileArtifactRepository(conn)
    lineage = repository.load_compile_artifact_lineage(
        artifact_kind=query.artifact_kind,
        revision_ref=query.revision_ref,
    )
    return {
        "lineage": lineage,
        "count": len(lineage),
        "root": lineage[0] if lineage else None,
        "latest": lineage[-1] if lineage else None,
    }


def handle_query_handoff_consumer_status(
    query: QueryHandoffConsumerStatus,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    repository = PostgresSubscriptionRepository(conn)
    subscription = repository.load_event_subscription(subscription_id=query.subscription_id)
    checkpoint = repository.load_subscription_checkpoint(
        subscription_id=query.subscription_id,
        run_id=query.run_id,
    )
    checkpoints = repository.list_subscription_checkpoints(
        subscription_id=query.subscription_id,
        run_id=query.run_id,
        limit=query.limit,
    )
    return {
        "subscription": subscription,
        "checkpoint": checkpoint,
        "checkpoints": checkpoints,
        "watermark": None if checkpoint is None else checkpoint.get("last_evidence_seq"),
        "last_authority_id": None if checkpoint is None else checkpoint.get("last_authority_id"),
    }


def handle_query_handoff_artifact_history(
    query: QueryHandoffArtifactHistory,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    repository = PostgresCompileArtifactRepository(conn)
    history = repository.load_compile_artifact_history(
        artifact_kind=query.artifact_kind,
        artifact_ref=query.artifact_ref,
        input_fingerprint=query.input_fingerprint,
        limit=query.limit,
    )
    return {
        "history": history,
        "count": len(history),
        "latest": history[0] if history else None,
    }


__all__ = [
    "QueryHandoffArtifactHistory",
    "QueryHandoffArtifactLineage",
    "QueryHandoffConsumerStatus",
    "QueryHandoffLatestArtifact",
    "handle_query_handoff_artifact_history",
    "handle_query_handoff_artifact_lineage",
    "handle_query_handoff_consumer_status",
    "handle_query_handoff_latest",
]
