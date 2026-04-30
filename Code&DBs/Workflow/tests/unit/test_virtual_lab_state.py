from __future__ import annotations

import pytest

from runtime.virtual_lab.state import (
    ActorIdentity,
    VirtualLabStateError,
    apply_overlay_patch_command,
    build_environment_revision,
    build_event_envelope,
    build_seed_manifest,
    event_chain_digest,
    object_states_from_seed_manifest,
    replay_environment_state,
    replay_object_events,
    restore_object_command,
    tombstone_object_command,
)


def _seed_manifest():
    return build_seed_manifest(
        [
            {
                "object_id": "account:002",
                "object_truth_ref": "object_truth.account.beta",
                "object_truth_version": "version.1",
                "projection_version": "projection.account.v1",
                "seed_parameters": {"include_contacts": False},
                "base_state": {"name": "Beta", "status": "prospect"},
            },
            {
                "object_id": "account:001",
                "object_truth_ref": "object_truth.account.acme",
                "object_truth_version": "version.7",
                "projection_version": "projection.account.v1",
                "seed_parameters": {"include_contacts": True},
                "base_state": {
                    "name": "Acme",
                    "status": "active",
                    "billing": {"city": "Denver", "tier": "gold"},
                },
            },
        ]
    )


def _revision():
    return build_environment_revision(
        environment_id="virtual_lab.env.acme",
        revision_reason="root_seed",
        seed_manifest=_seed_manifest(),
        config={"simulation_engine": "virtual_lab.state", "version": "1"},
        policy={"promotion": "readback_required"},
        created_at="2026-04-30T12:00:00-04:00",
        created_by="agent.phase_06",
    )


def _actor() -> ActorIdentity:
    return ActorIdentity(actor_id="agent.phase_06", actor_type="agent")


def test_seed_manifest_and_revision_are_deterministic_from_object_truth_refs() -> None:
    manifest = _seed_manifest()
    same_manifest = build_seed_manifest(list(reversed(manifest.entries)))

    assert manifest.seed_digest == same_manifest.seed_digest
    assert [entry.object_id for entry in manifest.entries] == ["account:001", "account:002"]
    assert manifest.entries[0].source_ref == {
        "object_truth_ref": "object_truth.account.acme",
        "object_truth_version": "version.7",
        "projection_version": "projection.account.v1",
    }

    revision = _revision()
    same_revision = build_environment_revision(
        environment_id="virtual_lab.env.acme",
        revision_reason="root_seed",
        seed_manifest=same_manifest,
        config={"version": "1", "simulation_engine": "virtual_lab.state"},
        policy={"promotion": "readback_required"},
        created_at="2026-04-30T16:00:00Z",
        created_by="agent.phase_06",
    )

    assert revision.revision_id == same_revision.revision_id
    assert revision.created_at == "2026-04-30T16:00:00Z"
    assert revision.seed_digest.startswith("sha256:v1:")
    assert revision.revision_digest.startswith("sha256:v1:")


def test_overlay_patch_is_copy_on_write_and_records_pre_post_digests() -> None:
    revision = _revision()
    state = object_states_from_seed_manifest(revision)[0]

    result = apply_overlay_patch_command(
        revision=revision,
        state=state,
        patch={"billing": {"tier": "platinum"}, "status": "renewal"},
        actor=_actor(),
        command_id="command.patch.account.001",
        occurred_at="2026-04-30T17:00:00Z",
        recorded_at="2026-04-30T17:00:01Z",
        expected_state_digest=state.state_digest,
    )

    assert result.receipt.status == "accepted"
    assert result.events[0].sequence_number == 1
    assert result.events[0].pre_state_digest == state.state_digest
    assert result.events[0].post_state_digest == result.state.state_digest
    assert result.receipt.resulting_event_ids == (result.events[0].event_id,)
    assert result.receipt.result_digest == result.state.state_digest
    assert state.base_state["billing"]["tier"] == "gold"
    assert result.state.base_state["billing"]["tier"] == "gold"
    assert result.state.overlay_state == {"billing": {"tier": "platinum"}, "status": "renewal"}
    assert result.state.effective_state["billing"] == {"city": "Denver", "tier": "platinum"}
    assert result.state.effective_state["status"] == "renewal"


def test_replay_rebuilds_state_and_duplicate_command_returns_no_op() -> None:
    revision = _revision()
    initial_state = object_states_from_seed_manifest(revision)[0]
    first = apply_overlay_patch_command(
        revision=revision,
        state=initial_state,
        patch={"status": "renewal"},
        actor=_actor(),
        command_id="command.patch.account.001",
        occurred_at="2026-04-30T17:00:00Z",
        recorded_at="2026-04-30T17:00:01Z",
        expected_state_digest=initial_state.state_digest,
    )
    second = tombstone_object_command(
        revision=revision,
        state=first.state,
        actor=_actor(),
        command_id="command.tombstone.account.001",
        occurred_at="2026-04-30T17:01:00Z",
        recorded_at="2026-04-30T17:01:01Z",
        stream_events=first.events,
        expected_state_digest=first.state.state_digest,
    )

    third = restore_object_command(
        revision=revision,
        state=second.state,
        actor=_actor(),
        command_id="command.restore.account.001",
        occurred_at="2026-04-30T17:02:00Z",
        recorded_at="2026-04-30T17:02:01Z",
        stream_events=first.events + second.events,
        expected_state_digest=second.state.state_digest,
    )

    events = first.events + second.events + third.events
    replayed = replay_object_events(initial_state, events)

    assert second.receipt.status == "accepted"
    assert second.events[0].sequence_number == 2
    assert third.receipt.status == "accepted"
    assert third.events[0].sequence_number == 3
    assert replayed.state_digest == third.state.state_digest
    assert replayed.tombstone is False
    assert event_chain_digest(events) == event_chain_digest(tuple(reversed(events)))

    duplicate = apply_overlay_patch_command(
        revision=revision,
        state=third.state,
        patch={"status": "ignored"},
        actor=_actor(),
        command_id="command.patch.account.001",
        occurred_at="2026-04-30T17:03:00Z",
        recorded_at="2026-04-30T17:03:01Z",
        stream_events=events,
    )

    assert duplicate.receipt.status == "no_op"
    assert duplicate.events == ()
    assert duplicate.receipt.resulting_event_ids == (first.events[0].event_id,)


def test_closed_revision_rejects_write_with_terminal_receipt() -> None:
    revision = _revision()
    closed_revision = revision.close()
    state = object_states_from_seed_manifest(revision)[0]

    result = apply_overlay_patch_command(
        revision=closed_revision,
        state=state,
        patch={"status": "should_not_apply"},
        actor=_actor(),
        command_id="command.patch.closed",
        occurred_at="2026-04-30T18:00:00Z",
        recorded_at="2026-04-30T18:00:01Z",
    )

    assert result.receipt.status == "rejected"
    assert result.receipt.resulting_event_ids == ()
    assert result.receipt.errors[0]["reason_code"] == "virtual_lab.revision_closed"
    assert result.receipt.result_digest == state.state_digest
    assert result.state.state_digest == state.state_digest
    assert result.events == ()


def test_event_append_rejects_invalid_pre_state_digest_for_next_sequence() -> None:
    revision = _revision()
    state = object_states_from_seed_manifest(revision)[0]
    first = apply_overlay_patch_command(
        revision=revision,
        state=state,
        patch={"status": "renewal"},
        actor=_actor(),
        command_id="command.patch.account.001",
        occurred_at="2026-04-30T17:00:00Z",
        recorded_at="2026-04-30T17:00:01Z",
    )

    with pytest.raises(VirtualLabStateError) as excinfo:
        build_event_envelope(
            environment_id=revision.environment_id,
            revision_id=revision.revision_id,
            stream_id=state.stream_id,
            event_type="object.patched",
            actor=_actor(),
            command_id="command.patch.account.002",
            occurred_at="2026-04-30T17:01:00Z",
            recorded_at="2026-04-30T17:01:01Z",
            pre_state_digest="sha256:v1:not-the-current-state",
            post_state_digest=first.state.state_digest,
            payload={"overlay_patch": {"status": "late"}},
            stream_events=first.events,
        )

    assert excinfo.value.reason_code == "virtual_lab.pre_state_digest_mismatch"


def test_environment_replay_rejects_orphan_object_events() -> None:
    revision = _revision()
    state = object_states_from_seed_manifest(revision)[0]
    orphan_event = build_event_envelope(
        environment_id=revision.environment_id,
        revision_id=revision.revision_id,
        stream_id=f"{revision.environment_id}/{revision.revision_id}/objects/account:999/primary",
        event_type="object.patched",
        actor=_actor(),
        command_id="command.patch.orphan",
        occurred_at="2026-04-30T17:01:00Z",
        recorded_at="2026-04-30T17:01:01Z",
        pre_state_digest=state.state_digest,
        post_state_digest=state.state_digest,
        payload={"overlay_patch": {"status": "ghost"}},
    )

    with pytest.raises(VirtualLabStateError) as excinfo:
        replay_environment_state([state], [orphan_event])

    assert excinfo.value.reason_code == "virtual_lab.orphan_object_event"
