from __future__ import annotations

from storage.postgres import integration_action_contract_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO integration_action_contract_heads" in sql:
            return {
                "action_contract_id": args[0],
                "action_id": args[1],
                "name": args[2],
                "owner_ref": args[3],
                "status": args[4],
                "source_system_ref": args[5],
                "target_system_ref": args[6],
                "current_revision_id": args[12],
                "current_contract_hash": args[13],
                "typed_gap_count": args[14],
                "contract_json": args[16],
            }
        if "INSERT INTO integration_action_contract_revisions" in sql:
            return {
                "action_contract_id": args[0],
                "revision_id": args[1],
                "revision_no": args[2],
                "contract_hash": args[5],
                "contract_json": args[8],
                "validation_gaps_json": args[9],
            }
        if "INSERT INTO integration_automation_rule_snapshot_heads" in sql:
            return {
                "automation_rule_id": args[0],
                "name": args[1],
                "status": args[2],
                "current_snapshot_id": args[5],
                "current_snapshot_hash": args[6],
                "linked_action_count": args[8],
                "typed_gap_count": args[9],
                "snapshot_json": args[10],
            }
        if "INSERT INTO integration_automation_rule_snapshot_revisions" in sql:
            return {
                "automation_rule_id": args[0],
                "snapshot_id": args[1],
                "snapshot_hash": args[2],
                "status": args[3],
                "snapshot_json": args[5],
                "validation_gaps_json": args[6],
            }
        return None

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def _gap() -> dict[str, object]:
    return {
        "gap_id": "typed_gap.integration_action_contract.demo",
        "gap_kind": "unknown_side_effects",
        "severity": "high",
        "related_ref": "integration_action.hubspot.create_contact",
        "disposition": "open",
        "description": "Side effects need evidence.",
    }


def _contract() -> dict[str, object]:
    return {
        "action_contract_id": "integration_action.hubspot.create_contact",
        "action_id": "integration_action.hubspot.create_contact",
        "name": "HubSpot / create contact",
        "owner": "owner.crm",
        "status": "draft",
        "systems": {
            "source": {"system_ref": "praxis.workflow", "provider": "praxis"},
            "target": {"system_ref": "integration.hubspot", "provider": "hubspot"},
        },
        "execution_mode": "sync",
        "idempotency": {"state": "unknown"},
        "rollback": {"rollback_class": "manual_only"},
        "side_effects": [{"kind": "external_mutation"}],
        "revision_id": "rev.integration_action_contract.demo",
        "revision_no": 1,
        "contract_hash": "contract.digest",
        "captured_at": "2026-04-30T18:00:00Z",
        "validation_gaps": [_gap()],
    }


def _automation_snapshot() -> dict[str, object]:
    return {
        "rule_id": "automation.hubspot.contact_sync",
        "name": "HubSpot contact sync",
        "owner": "owner.crm",
        "status": "active",
        "source_of_truth_ref": "hubspot.workflow.export.20260430",
        "snapshot_id": "snapshot.integration_automation_rule.demo",
        "snapshot_hash": "snapshot.digest",
        "capture_method": "structured_export",
        "snapshot_timestamp": "2026-04-30T18:01:00Z",
        "linked_action_ids": ["integration_action.hubspot.create_contact"],
        "validation_gaps": [_gap()],
    }


def test_persist_integration_action_contract_inventory_writes_revision_scoped_records() -> None:
    conn = _RecordingConn()

    result = repo.persist_integration_action_contract_inventory(
        conn,
        contracts=[_contract()],
        automation_snapshots=[_automation_snapshot()],
        observed_by_ref="operator:nate",
        source_ref="phase_05_test",
    )

    assert "INSERT INTO integration_action_contract_heads" in conn.fetchrow_calls[0][0]
    assert "INSERT INTO integration_action_contract_revisions" in conn.fetchrow_calls[1][0]
    assert "INSERT INTO integration_automation_rule_snapshot_heads" in conn.fetchrow_calls[2][0]
    assert "INSERT INTO integration_automation_rule_snapshot_revisions" in conn.fetchrow_calls[3][0]
    assert any("integration_action_contract_typed_gaps" in call[0] for call in conn.batch_calls)
    assert any("integration_automation_rule_snapshot_gaps" in call[0] for call in conn.batch_calls)
    assert any("integration_automation_action_links" in call[0] for call in conn.batch_calls)
    assert (
        "DELETE FROM integration_action_contract_typed_gaps WHERE action_contract_id = $1 AND revision_id = $2"
        in conn.execute_calls[0][0]
    )
    assert conn.execute_calls[0][1] == (
        "integration_action.hubspot.create_contact",
        "rev.integration_action_contract.demo",
    )
    assert conn.fetchrow_calls[0][1][15] == 1
    assert result["contract_count"] == 1
    assert result["contract_typed_gap_count"] == 1
    assert result["automation_snapshot_count"] == 1
    assert result["automation_action_link_count"] == 1
