from __future__ import annotations

from dataclasses import asdict
from types import SimpleNamespace

import pytest

from runtime.compile_artifacts import CompileArtifactError, CompileArtifactRecord
from runtime.operations.commands import handoff as handoff_commands
from runtime.operations.queries import handoff as handoff_queries


def _publish_command() -> handoff_commands.PublishHandoffArtifactCommand:
    return handoff_commands.PublishHandoffArtifactCommand(
        artifact_kind="definition",
        payload={
            "definition_revision": "definition-1",
            "compile_provenance": {"input_fingerprint": "fingerprint-1"},
        },
        decision_ref="decision-1",
        authority_refs=("authority-1",),
    )


def _artifact_record() -> CompileArtifactRecord:
    return CompileArtifactRecord(
        compile_artifact_id="compile_artifact.definition.deadbeef",
        artifact_kind="definition",
        artifact_ref="definition-1",
        revision_ref="definition-1",
        parent_artifact_ref=None,
        input_fingerprint="fingerprint-1",
        content_hash="deadbeef",
        authority_refs=("authority-1",),
        payload={
            "definition_revision": "definition-1",
            "compile_provenance": {"input_fingerprint": "fingerprint-1"},
        },
        decision_ref="decision-1",
    )


def _packet_record() -> CompileArtifactRecord:
    return CompileArtifactRecord(
        compile_artifact_id="compile_artifact.packet_lineage.cafebabe",
        artifact_kind="packet_lineage",
        artifact_ref="packet-1",
        revision_ref="packet-1",
        parent_artifact_ref="definition-1",
        input_fingerprint="fingerprint-2",
        content_hash="cafebabe",
        authority_refs=("authority-1",),
        payload={
            "packet_revision": "packet-1",
            "packet_hash": "hash-1",
            "run_id": "run-1",
            "workflow_id": "workflow-1",
            "spec_name": "spec",
            "source_kind": "stage",
        },
        decision_ref="decision-2",
    )


def test_publish_handoff_requires_postgres_authority() -> None:
    with pytest.raises(handoff_commands.HandoffCommandError):
        handoff_commands.handle_publish_handoff_artifact(_publish_command(), object())


def test_publish_handoff_rejects_ambiguous_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeStore:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_reusable_artifact(self, **kwargs):
            raise CompileArtifactError("conflicting reusable compile artifacts detected")

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_commands, "CompileArtifactStore", _FakeStore)

    with pytest.raises(handoff_commands.HandoffCommandError):
        handoff_commands.handle_publish_handoff_artifact(_publish_command(), subsystems)


def test_bind_handoff_records_packet_lineage(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakeStore:
        def __init__(self, conn: object) -> None:
            captured["conn"] = conn

        def load_reusable_artifact(self, **kwargs):
            return None

        def record_packet_lineage(self, **kwargs):
            captured["kwargs"] = dict(kwargs)
            return _packet_record()

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_commands, "CompileArtifactStore", _FakeStore)

    result = handoff_commands.handle_bind_handoff_artifact(
        handoff_commands.BindHandoffArtifactCommand(
            packet={
                "packet_revision": "packet-1",
                "packet_hash": "hash-1",
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "spec_name": "spec",
                "source_kind": "stage",
            },
            decision_ref="decision-1",
            authority_refs=("authority-1",),
            parent_artifact_ref="parent-1",
        ),
        subsystems,
    )

    assert result["reused"] is False
    assert result["artifact"]["revision_ref"] == "packet-1"
    assert result["artifact"]["artifact_kind"] == "packet_lineage"
    assert captured["kwargs"]["parent_artifact_ref"] == "parent-1"
    assert captured["kwargs"]["decision_ref"] == "decision-1"


def test_consume_handoff_rejects_duplicate_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeStore:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_reusable_artifact(self, **kwargs):
            return _artifact_record()

    class _FakeRepository:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_subscription_checkpoint(self, **kwargs):
            return {"checkpoint_id": "checkpoint-1"}

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_commands, "CompileArtifactStore", _FakeStore)
    monkeypatch.setattr(handoff_commands, "PostgresSubscriptionRepository", _FakeRepository)
    monkeypatch.setattr(handoff_commands, "PostgresCompileArtifactRepository", _FakeRepository)

    with pytest.raises(handoff_commands.HandoffCommandError):
        handoff_commands.handle_consume_handoff_artifact(
            handoff_commands.ConsumeHandoffArtifactCommand(
                subscription_id="subscription-1",
                run_id="run-1",
                artifact_kind="definition",
                input_fingerprint="fingerprint-1",
            ),
            subsystems,
        )


def test_consume_handoff_commits_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakeStore:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_reusable_artifact(self, **kwargs):
            return None

    class _FakeRepository:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_subscription_checkpoint(self, **kwargs):
            return None

        def load_compile_artifact_by_revision(self, **kwargs):
            return {
                "compile_artifact_id": "compile_artifact.plan.deadbeef",
                "artifact_kind": kwargs["artifact_kind"],
                "artifact_ref": kwargs["revision_ref"],
                "revision_ref": kwargs["revision_ref"],
                "parent_artifact_ref": None,
                "input_fingerprint": "fingerprint-2",
                "content_hash": "deadbeef",
                "authority_refs": ["authority-1"],
                "payload": {"plan_revision": kwargs["revision_ref"]},
                "decision_ref": "decision-2",
            }

        def upsert_subscription_checkpoint(self, **kwargs):
            captured["kwargs"] = dict(kwargs)
            return {
                "checkpoint_id": "checkpoint-1",
                "subscription_id": kwargs["subscription_id"],
                "run_id": kwargs["run_id"],
                "last_evidence_seq": kwargs["last_evidence_seq"],
                "last_authority_id": kwargs["last_authority_id"],
                "checkpoint_status": kwargs["checkpoint_status"],
                "checkpointed_at": "2026-04-16T00:00:00+00:00",
                "metadata": kwargs["metadata"],
            }

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_commands, "CompileArtifactStore", _FakeStore)
    monkeypatch.setattr(handoff_commands, "PostgresSubscriptionRepository", _FakeRepository)
    monkeypatch.setattr(handoff_commands, "PostgresCompileArtifactRepository", _FakeRepository)

    result = handoff_commands.handle_consume_handoff_artifact(
        handoff_commands.ConsumeHandoffArtifactCommand(
            subscription_id="subscription-1",
            run_id="run-1",
            artifact_kind="plan",
            revision_ref="plan-1",
            through_evidence_seq=7,
            metadata={"source": "cli"},
        ),
        subsystems,
    )

    assert result["checkpoint"]["last_evidence_seq"] == 7
    assert result["artifact"]["revision_ref"] == "plan-1"
    assert captured["kwargs"]["last_authority_id"] == "decision-2"
    assert captured["kwargs"]["metadata"]["revision_ref"] == "plan-1"
    assert captured["kwargs"]["metadata"]["source"] == "cli"


def test_query_handoff_latest_and_lineage(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRepository:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_compile_artifact_history(self, **kwargs):
            assert kwargs["artifact_kind"] == "definition"
            return [
                {
                    "compile_artifact_id": "artifact-2",
                    "artifact_kind": "definition",
                    "artifact_ref": "definition-1",
                    "revision_ref": "definition-2",
                    "parent_artifact_ref": "definition-1",
                    "input_fingerprint": "fingerprint-1",
                    "content_hash": "hash-2",
                    "authority_refs": ["authority-1"],
                    "payload": {"definition_revision": "definition-2"},
                    "decision_ref": "decision-2",
                }
            ]

        def load_compile_artifact_lineage(self, **kwargs):
            assert kwargs["artifact_kind"] == "definition"
            return [
                {
                    "compile_artifact_id": "artifact-1",
                    "artifact_kind": "definition",
                    "artifact_ref": "definition-1",
                    "revision_ref": "definition-1",
                    "parent_artifact_ref": None,
                    "input_fingerprint": "fingerprint-1",
                    "content_hash": "hash-1",
                    "authority_refs": ["authority-1"],
                    "payload": {"definition_revision": "definition-1"},
                    "decision_ref": "decision-1",
                },
                {
                    "compile_artifact_id": "artifact-2",
                    "artifact_kind": "definition",
                    "artifact_ref": "definition-1",
                    "revision_ref": "definition-2",
                    "parent_artifact_ref": "definition-1",
                    "input_fingerprint": "fingerprint-1",
                    "content_hash": "hash-2",
                    "authority_refs": ["authority-1"],
                    "payload": {"definition_revision": "definition-2"},
                    "decision_ref": "decision-2",
                },
            ]

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_queries, "PostgresCompileArtifactRepository", _FakeRepository)

    latest = handoff_queries.handle_query_handoff_latest(
        handoff_queries.QueryHandoffLatestArtifact(artifact_kind="definition"),
        subsystems,
    )
    assert latest["artifact"]["revision_ref"] == "definition-2"

    lineage = handoff_queries.handle_query_handoff_artifact_lineage(
        handoff_queries.QueryHandoffArtifactLineage(
            artifact_kind="definition",
            revision_ref="definition-2",
        ),
        subsystems,
    )
    assert lineage["count"] == 2
    assert lineage["latest"]["revision_ref"] == "definition-2"


def test_query_handoff_consumer_status(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRepository:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_event_subscription(self, **kwargs):
            return {
                "subscription_id": kwargs["subscription_id"],
                "subscription_name": "consumer-1",
                "consumer_kind": "worker",
                "envelope_kind": "artifact",
                "workflow_id": "workflow-1",
                "run_id": kwargs.get("run_id"),
                "cursor_scope": "run",
                "status": "active",
                "delivery_policy": {},
                "filter_policy": {},
                "created_at": "2026-04-16T00:00:00+00:00",
            }

        def load_subscription_checkpoint(self, **kwargs):
            return {
                "checkpoint_id": "checkpoint-1",
                "subscription_id": kwargs["subscription_id"],
                "run_id": kwargs["run_id"],
                "last_evidence_seq": 9,
                "last_authority_id": "decision-9",
                "checkpoint_status": "committed",
                "checkpointed_at": "2026-04-16T00:00:00+00:00",
                "metadata": {"artifact_revision_ref": "revision-9"},
            }

        def list_subscription_checkpoints(self, **kwargs):
            return [
                {
                    "checkpoint_id": "checkpoint-1",
                    "subscription_id": kwargs["subscription_id"],
                    "run_id": kwargs["run_id"],
                    "last_evidence_seq": 9,
                    "last_authority_id": "decision-9",
                    "checkpoint_status": "committed",
                    "checkpointed_at": "2026-04-16T00:00:00+00:00",
                    "metadata": {"artifact_revision_ref": "revision-9"},
                }
            ]

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_queries, "PostgresSubscriptionRepository", _FakeRepository)

    payload = handoff_queries.handle_query_handoff_consumer_status(
        handoff_queries.QueryHandoffConsumerStatus(
            subscription_id="subscription-1",
            run_id="run-1",
            limit=5,
        ),
        subsystems,
    )

    assert payload["watermark"] == 9
    assert payload["checkpoint"]["last_authority_id"] == "decision-9"
    assert payload["subscription"]["subscription_name"] == "consumer-1"


def test_handoff_round_trip_write_and_read(monkeypatch: pytest.MonkeyPatch) -> None:
    backend: dict[str, object] = {
        "artifacts": [],
        "checkpoint": None,
    }

    def _artifact_row_from_record(record: CompileArtifactRecord) -> dict[str, object]:
        row = dict(asdict(record))
        row["created_at"] = "2026-04-16T00:00:00+00:00"
        return row

    class _FakeStore:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_reusable_artifact(self, *, artifact_kind: str, input_fingerprint: str):
            artifacts = [
                artifact
                for artifact in backend["artifacts"]
                if artifact.artifact_kind == artifact_kind and artifact.input_fingerprint == input_fingerprint
            ]
            if not artifacts:
                return None
            if len({artifact.revision_ref for artifact in artifacts}) > 1:
                raise CompileArtifactError("conflicting reusable compile artifacts detected")
            return artifacts[-1]

        def record_definition(self, **kwargs):
            payload = dict(kwargs["definition"])
            record = CompileArtifactRecord(
                compile_artifact_id="compile_artifact.definition.definition-1",
                artifact_kind="definition",
                artifact_ref=payload["definition_revision"],
                revision_ref=payload["definition_revision"],
                parent_artifact_ref=None,
                input_fingerprint=kwargs["input_fingerprint"] or "fingerprint-1",
                content_hash="hash-definition-1",
                authority_refs=tuple(kwargs["authority_refs"]),
                payload=payload,
                decision_ref=kwargs["decision_ref"],
            )
            backend["artifacts"].append(record)
            return record

    class _FakeArtifactRepository:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_compile_artifact_history(self, **kwargs):
            artifacts = [
                artifact
                for artifact in backend["artifacts"]
                if artifact.artifact_kind == kwargs["artifact_kind"]
                and (
                    kwargs["artifact_ref"] is None
                    or artifact.artifact_ref == kwargs["artifact_ref"]
                )
                and (
                    kwargs.get("input_fingerprint") is None
                    or artifact.input_fingerprint == kwargs.get("input_fingerprint")
                )
            ]
            return [_artifact_row_from_record(record) for record in reversed(artifacts[: kwargs["limit"]])]

        def load_compile_artifact_by_revision(self, **kwargs):
            for artifact in backend["artifacts"]:
                if artifact.artifact_kind == kwargs["artifact_kind"] and artifact.revision_ref == kwargs["revision_ref"]:
                    return _artifact_row_from_record(artifact)
            return None

        def load_compile_artifact_lineage(self, **kwargs):
            return self.load_compile_artifact_history(**kwargs)

    class _FakeSubscriptionRepository:
        def __init__(self, conn: object) -> None:
            self.conn = conn

        def load_event_subscription(self, **kwargs):
            return {
                "subscription_id": kwargs["subscription_id"],
                "subscription_name": "consumer-1",
                "consumer_kind": "worker",
                "envelope_kind": "artifact",
                "workflow_id": "workflow-1",
                "run_id": kwargs.get("run_id"),
                "cursor_scope": "run",
                "status": "active",
                "delivery_policy": {},
                "filter_policy": {},
                "created_at": "2026-04-16T00:00:00+00:00",
            }

        def load_subscription_checkpoint(self, **kwargs):
            return backend["checkpoint"]

        def list_subscription_checkpoints(self, **kwargs):
            checkpoint = backend["checkpoint"]
            return [] if checkpoint is None else [checkpoint]

        def upsert_subscription_checkpoint(self, **kwargs):
            checkpoint = {
                "checkpoint_id": "checkpoint-1",
                "subscription_id": kwargs["subscription_id"],
                "run_id": kwargs["run_id"],
                "last_evidence_seq": kwargs["last_evidence_seq"],
                "last_authority_id": kwargs["last_authority_id"],
                "checkpoint_status": kwargs["checkpoint_status"],
                "checkpointed_at": "2026-04-16T00:00:00+00:00",
                "metadata": kwargs["metadata"],
            }
            backend["checkpoint"] = checkpoint
            return checkpoint

    subsystems = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(handoff_commands, "CompileArtifactStore", _FakeStore)
    monkeypatch.setattr(handoff_commands, "PostgresSubscriptionRepository", _FakeSubscriptionRepository)
    monkeypatch.setattr(handoff_commands, "PostgresCompileArtifactRepository", _FakeArtifactRepository)
    monkeypatch.setattr(handoff_queries, "PostgresCompileArtifactRepository", _FakeArtifactRepository)
    monkeypatch.setattr(handoff_queries, "PostgresSubscriptionRepository", _FakeSubscriptionRepository)

    publish = handoff_commands.handle_publish_handoff_artifact(
        _publish_command(),
        subsystems,
    )
    assert publish["reused"] is False
    assert publish["artifact"]["revision_ref"] == "definition-1"

    latest = handoff_queries.handle_query_handoff_latest(
        handoff_queries.QueryHandoffLatestArtifact(artifact_kind="definition"),
        subsystems,
    )
    assert latest["artifact"]["revision_ref"] == "definition-1"

    consume = handoff_commands.handle_consume_handoff_artifact(
        handoff_commands.ConsumeHandoffArtifactCommand(
            subscription_id="subscription-1",
            run_id="run-1",
            artifact_kind="definition",
            revision_ref="definition-1",
            through_evidence_seq=1,
            metadata={"source": "round-trip"},
        ),
        subsystems,
    )
    assert consume["checkpoint"]["last_evidence_seq"] == 1

    status = handoff_queries.handle_query_handoff_consumer_status(
        handoff_queries.QueryHandoffConsumerStatus(
            subscription_id="subscription-1",
            run_id="run-1",
            limit=5,
        ),
        subsystems,
    )
    assert status["watermark"] == 1
    assert status["checkpoint"]["metadata"]["artifact_kind"] == "definition"

    with pytest.raises(handoff_commands.HandoffCommandError):
        handoff_commands.handle_consume_handoff_artifact(
            handoff_commands.ConsumeHandoffArtifactCommand(
                subscription_id="subscription-1",
                run_id="run-1",
                artifact_kind="definition",
                revision_ref="definition-1",
            ),
            subsystems,
        )
