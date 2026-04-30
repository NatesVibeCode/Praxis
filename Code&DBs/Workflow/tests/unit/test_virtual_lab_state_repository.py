from __future__ import annotations

from storage.postgres import virtual_lab_state_repository as repo
from runtime.virtual_lab.state import (
    ActorIdentity,
    apply_overlay_patch_command,
    build_environment_revision,
    build_seed_manifest,
    object_states_from_seed_manifest,
)


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO virtual_lab_environment_revisions" in sql:
            return {
                "environment_id": args[0],
                "revision_id": args[1],
                "revision_digest": args[8],
                "revision_json": args[12],
            }
        if "INSERT INTO virtual_lab_environment_heads" in sql:
            return {
                "environment_id": args[0],
                "current_revision_id": args[1],
                "current_revision_digest": args[2],
                "object_state_count": args[5],
                "event_count": args[6],
                "receipt_count": args[7],
                "typed_gap_count": args[8],
            }
        return {}

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


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
        "typed_gaps": [
            {
                "gap_id": "virtual_lab_gap.demo",
                "gap_kind": "promotion_readback_missing",
                "severity": "medium",
                "related_ref": revision.revision_id,
                "disposition": "open",
            }
        ],
    }


def test_persist_virtual_lab_state_packet_writes_revision_scoped_records() -> None:
    conn = _RecordingConn()
    packet = _packet()

    result = repo.persist_virtual_lab_state_packet(
        conn,
        environment_revision=packet["environment_revision"],
        object_states=packet["object_states"],
        events=packet["events"],
        command_receipts=packet["command_receipts"],
        typed_gaps=packet["typed_gaps"],
        event_chain_digest="sha256:v1:event-chain",
        observed_by_ref="operator:nate",
        source_ref="phase_06_test",
    )

    assert "INSERT INTO virtual_lab_environment_revisions" in conn.fetchrow_calls[0][0]
    assert "INSERT INTO virtual_lab_environment_heads" in conn.fetchrow_calls[1][0]
    assert any("INSERT INTO virtual_lab_object_states" in call[0] for call in conn.fetchrow_calls)
    assert any("INSERT INTO virtual_lab_events" in call[0] for call in conn.fetchrow_calls)
    assert any("INSERT INTO virtual_lab_command_receipts" in call[0] for call in conn.fetchrow_calls)
    assert (
        "DELETE FROM virtual_lab_seed_entries WHERE environment_id = $1 AND revision_id = $2"
        in conn.execute_calls[0][0]
    )
    assert (
        "DELETE FROM virtual_lab_typed_gaps WHERE environment_id = $1 AND revision_id = $2"
        in conn.execute_calls[1][0]
    )
    assert any("INSERT INTO virtual_lab_seed_entries" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_typed_gaps" in call[0] for call in conn.batch_calls)
    assert result["environment_id"] == "virtual_lab.env.acme"
    assert result["seed_entry_count"] == 1
    assert result["object_state_count"] == 1
    assert result["event_count"] == 1
    assert result["receipt_count"] == 1
    assert result["typed_gap_count"] == 1
