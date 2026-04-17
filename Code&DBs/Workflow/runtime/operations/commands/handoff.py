from __future__ import annotations

from dataclasses import asdict
from typing import Any

from pydantic import BaseModel, Field

from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from storage.postgres.compile_artifact_repository import PostgresCompileArtifactRepository
from storage.postgres.subscription_repository import PostgresSubscriptionRepository


class HandoffCommandError(RuntimeError):
    """Raised when a handoff command cannot be committed cleanly."""


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    if not callable(getter):
        raise HandoffCommandError("handoff commands require Postgres authority")
    conn = getter()
    if conn is None:
        raise HandoffCommandError("handoff commands require Postgres authority")
    return conn


def _payload_input_fingerprint(payload: dict[str, Any]) -> str | None:
    compile_provenance = payload.get("compile_provenance")
    if not isinstance(compile_provenance, dict):
        return None
    candidate = compile_provenance.get("input_fingerprint")
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    return None


def _record_payload(record: Any) -> dict[str, Any]:
    return asdict(record)


def _selected_artifact_record(
    *,
    store: CompileArtifactStore,
    repository: Any | None = None,
    artifact_kind: str,
    revision_ref: str | None,
    input_fingerprint: str | None,
) -> dict[str, Any]:
    if input_fingerprint:
        try:
            reusable = store.load_reusable_artifact(
                artifact_kind=artifact_kind,
                input_fingerprint=input_fingerprint,
            )
        except CompileArtifactError as exc:
            raise HandoffCommandError(str(exc)) from exc
        if reusable is None:
            raise HandoffCommandError(
                f"no reusable handoff artifact found for {artifact_kind} and input fingerprint",
            )
        if revision_ref is not None and reusable.revision_ref != revision_ref:
            raise HandoffCommandError(
                "requested handoff revision does not match the reusable artifact",
            )
        return _record_payload(reusable)

    if revision_ref is None:
        raise HandoffCommandError("handoff consume requires revision_ref or input_fingerprint")

    if repository is None:
        raise HandoffCommandError("handoff consume requires repository authority")
    row = repository.load_compile_artifact_by_revision(
        artifact_kind=artifact_kind,
        revision_ref=revision_ref,
    )
    if row is None:
        raise HandoffCommandError(
            f"no handoff artifact found for {artifact_kind}:{revision_ref}",
        )
    return dict(row)


class PublishHandoffArtifactCommand(BaseModel):
    artifact_kind: str
    payload: dict[str, Any]
    decision_ref: str
    authority_refs: tuple[str, ...] = ()
    parent_artifact_ref: str | None = None
    input_fingerprint: str | None = None


class BindHandoffArtifactCommand(BaseModel):
    packet: dict[str, Any]
    decision_ref: str
    authority_refs: tuple[str, ...] = ()
    parent_artifact_ref: str | None = None
    input_fingerprint: str | None = None


class ConsumeHandoffArtifactCommand(BaseModel):
    subscription_id: str
    run_id: str
    artifact_kind: str
    revision_ref: str | None = None
    input_fingerprint: str | None = None
    through_evidence_seq: int | None = None
    checkpoint_status: str = "committed"
    metadata: dict[str, Any] = Field(default_factory=dict)


def handle_publish_handoff_artifact(
    command: PublishHandoffArtifactCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    store = CompileArtifactStore(conn)
    normalized_input_fingerprint = command.input_fingerprint or _payload_input_fingerprint(
        command.payload,
    )

    if normalized_input_fingerprint:
        try:
            reusable = store.load_reusable_artifact(
                artifact_kind=command.artifact_kind,
                input_fingerprint=normalized_input_fingerprint,
            )
        except CompileArtifactError as exc:
            raise HandoffCommandError(str(exc)) from exc
        if reusable is not None:
            return {"artifact": _record_payload(reusable), "reused": True}

    try:
        if command.artifact_kind == "definition":
            record = store.record_definition(
                definition=command.payload,
                authority_refs=command.authority_refs,
                decision_ref=command.decision_ref,
                input_fingerprint=normalized_input_fingerprint,
            )
        elif command.artifact_kind == "plan":
            record = store.record_plan(
                plan=command.payload,
                authority_refs=command.authority_refs,
                decision_ref=command.decision_ref,
                parent_artifact_ref=command.parent_artifact_ref,
                input_fingerprint=normalized_input_fingerprint,
            )
        elif command.artifact_kind == "packet_lineage":
            record = store.record_packet_lineage(
                packet=command.payload,
                authority_refs=command.authority_refs,
                decision_ref=command.decision_ref,
                parent_artifact_ref=command.parent_artifact_ref,
                input_fingerprint=normalized_input_fingerprint,
            )
        else:
            raise HandoffCommandError(
                "artifact_kind must be one of: definition, plan, packet_lineage",
            )
    except CompileArtifactError as exc:
        raise HandoffCommandError(str(exc)) from exc

    return {"artifact": _record_payload(record), "reused": False}


def handle_bind_handoff_artifact(
    command: BindHandoffArtifactCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    store = CompileArtifactStore(conn)
    normalized_input_fingerprint = command.input_fingerprint or _payload_input_fingerprint(
        command.packet,
    )
    if normalized_input_fingerprint:
        try:
            reusable = store.load_reusable_artifact(
                artifact_kind="packet_lineage",
                input_fingerprint=normalized_input_fingerprint,
            )
        except CompileArtifactError as exc:
            raise HandoffCommandError(str(exc)) from exc
        if reusable is not None:
            return {"artifact": _record_payload(reusable), "reused": True}

    try:
        record = store.record_packet_lineage(
            packet=command.packet,
            authority_refs=command.authority_refs,
            decision_ref=command.decision_ref,
            parent_artifact_ref=command.parent_artifact_ref,
            input_fingerprint=normalized_input_fingerprint,
        )
    except CompileArtifactError as exc:
        raise HandoffCommandError(str(exc)) from exc

    return {"artifact": _record_payload(record), "reused": False}


def handle_consume_handoff_artifact(
    command: ConsumeHandoffArtifactCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = _pg_conn(subsystems)
    store = CompileArtifactStore(conn)
    subscription_repository = PostgresSubscriptionRepository(conn)
    artifact_repository = PostgresCompileArtifactRepository(conn)

    existing_checkpoint = subscription_repository.load_subscription_checkpoint(
        subscription_id=command.subscription_id,
        run_id=command.run_id,
    )
    if existing_checkpoint is not None:
        raise HandoffCommandError(
            "handoff consume is already committed for this subscription/run pair",
        )

    artifact = _selected_artifact_record(
        store=store,
        repository=artifact_repository,
        artifact_kind=command.artifact_kind,
        revision_ref=command.revision_ref,
        input_fingerprint=command.input_fingerprint,
    )

    metadata = dict(command.metadata)
    metadata.setdefault("artifact_kind", command.artifact_kind)
    metadata.setdefault("artifact_ref", artifact.get("artifact_ref"))
    metadata.setdefault("revision_ref", artifact.get("revision_ref"))
    metadata.setdefault("content_hash", artifact.get("content_hash"))
    metadata.setdefault("decision_ref", artifact.get("decision_ref"))
    if command.input_fingerprint:
        metadata.setdefault("input_fingerprint", command.input_fingerprint)
    if command.through_evidence_seq is not None:
        metadata.setdefault("through_evidence_seq", command.through_evidence_seq)

    try:
        checkpoint = subscription_repository.upsert_subscription_checkpoint(
            subscription_id=command.subscription_id,
            run_id=command.run_id,
            last_evidence_seq=command.through_evidence_seq,
            last_authority_id=str(artifact.get("decision_ref") or artifact.get("revision_ref") or ""),
            checkpoint_status=command.checkpoint_status,
            metadata=metadata,
        )
    except Exception as exc:  # pragma: no cover - repository already validates
        raise HandoffCommandError(str(exc)) from exc

    return {
        "artifact": artifact,
        "checkpoint": checkpoint,
    }


__all__ = [
    "BindHandoffArtifactCommand",
    "ConsumeHandoffArtifactCommand",
    "HandoffCommandError",
    "PublishHandoffArtifactCommand",
    "handle_bind_handoff_artifact",
    "handle_consume_handoff_artifact",
    "handle_publish_handoff_artifact",
]
