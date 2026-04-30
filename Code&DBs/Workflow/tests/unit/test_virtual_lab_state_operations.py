from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime.operations.commands import virtual_lab_state as commands
from runtime.operations.queries import virtual_lab_state as queries
from runtime.virtual_lab.state import (
    ActorIdentity,
    apply_overlay_patch_command,
    build_environment_revision,
    build_seed_manifest,
    environment_revision_from_dict,
    object_states_from_seed_manifest,
)


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _packet() -> dict[str, object]:
    seed_manifest = build_seed_manifest(
        [
            {
                "object_id": "account:001",
                "object_truth_ref": "object_truth.account.acme",
                "object_truth_version": "version.7",
                "projection_version": "projection.account.v1",
                "seed_parameters": {"include_contacts": True},
                "base_state": {"name": "Acme", "status": "active"},
            }
        ]
    )
    revision = build_environment_revision(
        environment_id="virtual_lab.env.acme",
        revision_reason="root_seed",
        seed_manifest=seed_manifest,
        config={"simulation_engine": "virtual_lab.state", "version": "1"},
        policy={"promotion": "readback_required"},
        created_at="2026-04-30T16:00:00Z",
        created_by="agent.phase_06",
    )
    initial_state = object_states_from_seed_manifest(revision)[0]
    result = apply_overlay_patch_command(
        revision=revision,
        state=initial_state,
        patch={"status": "renewal"},
        actor=ActorIdentity(actor_id="agent.phase_06", actor_type="agent"),
        command_id="command.patch.account.001",
        occurred_at="2026-04-30T17:00:00Z",
        recorded_at="2026-04-30T17:00:01Z",
        expected_state_digest=initial_state.state_digest,
    )
    return {
        "environment_revision": revision.to_json(),
        "object_states": [result.state.to_json()],
        "events": [event.to_json() for event in result.events],
        "command_receipts": [result.receipt.to_json()],
    }


def test_virtual_lab_state_record_validates_replay_and_event_payload(monkeypatch) -> None:
    persist_calls: list[dict[str, object]] = []
    packet = _packet()

    def _persist(conn, **kwargs):
        persist_calls.append(kwargs)
        return {
            "environment_id": kwargs["environment_revision"]["environment_id"],
            "revision_id": kwargs["environment_revision"]["revision_id"],
            "object_state_count": len(kwargs["object_states"]),
            "event_count": len(kwargs["events"]),
            "receipt_count": len(kwargs["command_receipts"]),
            "typed_gap_count": len(kwargs["typed_gaps"]),
        }

    monkeypatch.setattr(commands, "persist_virtual_lab_state_packet", _persist)

    result = commands.handle_virtual_lab_state_record(
        commands.RecordVirtualLabStateCommand(
            **packet,
            typed_gaps=[
                {
                    "gap_id": "virtual_lab_gap.demo",
                    "gap_kind": "promotion_readback_missing",
                    "severity": "medium",
                    "related_ref": "virtual_lab.env.acme",
                    "disposition": "open",
                }
            ],
            observed_by_ref="operator:nate",
            source_ref="phase_06_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "virtual_lab_state_record"
    assert result["validation"]["object_state_count"] == 1
    assert result["validation"]["event_count"] == 1
    assert result["validation"]["event_chain_digest"].startswith("sha256:v1:")
    assert result["event_payload"]["object_state_count"] == 1
    assert result["event_payload"]["receipt_count"] == 1
    assert persist_calls[0]["observed_by_ref"] == "operator:nate"
    assert persist_calls[0]["event_chain_digest"].startswith("sha256:v1:")


def test_virtual_lab_state_record_rejects_projection_that_does_not_replay() -> None:
    packet = _packet()
    revision = environment_revision_from_dict(dict(packet["environment_revision"]))
    packet["object_states"] = [object_states_from_seed_manifest(revision)[0].to_json()]

    with pytest.raises(ValueError, match="object_state digest does not match deterministic replay"):
        commands.handle_virtual_lab_state_record(
            commands.RecordVirtualLabStateCommand(**packet),
            _subsystems(),
        )


def test_virtual_lab_state_read_lists_and_describes(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_environments",
        lambda conn, status=None, limit=50: [{"environment_id": "virtual_lab.env.acme", "status": status}],
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_revisions",
        lambda conn, environment_id=None, status=None, limit=50: [
            {"environment_id": environment_id, "revision_id": "virtual_lab_revision.demo"}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_virtual_lab_revision",
        lambda conn, environment_id, revision_id, **kwargs: {
            "environment_id": environment_id,
            "revision_id": revision_id,
            "events": [{}] if kwargs["include_events"] else [],
        },
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_events",
        lambda conn, environment_id, revision_id, stream_id=None, event_type=None, limit=50: [
            {"environment_id": environment_id, "revision_id": revision_id, "stream_id": stream_id}
        ],
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_command_receipts",
        lambda conn, environment_id, revision_id, status=None, limit=50: [
            {"environment_id": environment_id, "revision_id": revision_id, "status": status}
        ],
    )

    listed = queries.handle_virtual_lab_state_read(
        queries.QueryVirtualLabStateRead(action="list_environments", status="active"),
        _subsystems(),
    )
    revisions = queries.handle_virtual_lab_state_read(
        queries.QueryVirtualLabStateRead(action="list_revisions", environment_id="virtual_lab.env.acme"),
        _subsystems(),
    )
    described = queries.handle_virtual_lab_state_read(
        queries.QueryVirtualLabStateRead(
            action="describe_revision",
            environment_id="virtual_lab.env.acme",
            revision_id="virtual_lab_revision.demo",
        ),
        _subsystems(),
    )
    events = queries.handle_virtual_lab_state_read(
        queries.QueryVirtualLabStateRead(
            action="list_events",
            environment_id="virtual_lab.env.acme",
            revision_id="virtual_lab_revision.demo",
            stream_id="stream.1",
        ),
        _subsystems(),
    )
    receipts = queries.handle_virtual_lab_state_read(
        queries.QueryVirtualLabStateRead(
            action="list_receipts",
            environment_id="virtual_lab.env.acme",
            revision_id="virtual_lab_revision.demo",
            status="accepted",
        ),
        _subsystems(),
    )

    assert listed["items"][0]["status"] == "active"
    assert revisions["items"][0]["environment_id"] == "virtual_lab.env.acme"
    assert described["revision"]["revision_id"] == "virtual_lab_revision.demo"
    assert events["items"][0]["stream_id"] == "stream.1"
    assert receipts["items"][0]["status"] == "accepted"
