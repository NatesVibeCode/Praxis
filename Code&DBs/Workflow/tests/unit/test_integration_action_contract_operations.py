from __future__ import annotations

from types import SimpleNamespace

from runtime.integrations.action_contracts import (
    AutomationRuleSnapshot,
    AutomationRuleStatus,
    SnapshotConfidence,
    draft_contract_from_registry_definition,
)
from runtime.operations.commands import integration_action_contracts as commands
from runtime.operations.queries import integration_action_contracts as queries


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _contract() -> dict[str, object]:
    return draft_contract_from_registry_definition(
        {
            "id": "hubspot",
            "name": "HubSpot",
            "provider": "hubspot",
            "auth_shape": {"kind": "oauth2", "credential_ref": "credential.hubspot"},
        },
        {
            "action": "create_contact",
            "method": "POST",
            "body_template": {"email": "{{email}}"},
        },
        captured_at="2026-04-30T18:00:00Z",
        owner="owner.crm",
    ).as_dict()


def _automation_snapshot(action_contract_id: str) -> dict[str, object]:
    return AutomationRuleSnapshot(
        rule_id="automation.hubspot.contact_sync",
        name="HubSpot contact sync",
        source_of_truth_ref="hubspot.workflow.export.20260430",
        snapshot_timestamp="2026-04-30T18:01:00Z",
        trigger_condition="contact.created",
        owner="owner.crm",
        status=AutomationRuleStatus.ACTIVE,
        linked_action_ids=(action_contract_id,),
        pause_disable_method="HubSpot workflows > Contact sync > disable",
        capture_method=SnapshotConfidence.STRUCTURED_EXPORT,
    ).as_dict()


def test_integration_action_contract_record_prepares_hashes_gaps_and_event(
    monkeypatch,
) -> None:
    persist_calls: list[dict[str, object]] = []
    contract = _contract()
    snapshot = _automation_snapshot(str(contract["action_id"]))

    def _persist(conn, *, contracts, automation_snapshots, observed_by_ref=None, source_ref=None):
        persist_calls.append(
            {
                "contracts": contracts,
                "automation_snapshots": automation_snapshots,
                "observed_by_ref": observed_by_ref,
                "source_ref": source_ref,
            }
        )
        return {
            "contract_count": len(contracts),
            "contract_typed_gap_count": sum(len(item["validation_gaps"]) for item in contracts),
            "automation_snapshot_count": len(automation_snapshots),
            "automation_snapshot_gap_count": sum(len(item["validation_gaps"]) for item in automation_snapshots),
            "automation_action_link_count": 1,
        }

    monkeypatch.setattr(commands, "persist_integration_action_contract_inventory", _persist)

    result = commands.handle_integration_action_contract_record(
        commands.RecordIntegrationActionContractCommand(
            contracts=[contract],
            automation_snapshots=[snapshot],
            observed_by_ref="operator:nate",
            source_ref="phase_05_test",
        ),
        _subsystems(),
    )

    prepared_contract = result["contracts"][0]
    prepared_snapshot = result["automation_snapshots"][0]
    assert result["ok"] is True
    assert result["operation"] == "integration_action_contract_record"
    assert prepared_contract["action_contract_id"] == "integration_action.hubspot.create_contact"
    assert prepared_contract["revision_id"].startswith("rev.integration_action_contract.")
    assert prepared_contract["contract_hash"]
    assert prepared_contract["validation_gaps"]
    assert prepared_snapshot["snapshot_id"].startswith("snapshot.integration_automation_rule.")
    assert prepared_snapshot["validation_gaps"] == []
    assert result["event_payload"]["contract_count"] == 1
    assert result["event_payload"]["automation_action_link_count"] == 1
    assert persist_calls[0]["observed_by_ref"] == "operator:nate"


def test_integration_action_contract_read_lists_and_describes(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_integration_action_contracts",
        lambda conn, target_system_ref=None, status=None, owner_ref=None, limit=50: [
            {"action_contract_id": "integration_action.hubspot.create_contact", "status": status}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_integration_action_contract",
        lambda conn, action_contract_id, include_history=True, include_automation=True: {
            "action_contract_id": action_contract_id,
            "revisions": [{}] if include_history else [],
            "automation_snapshots": [{}] if include_automation else [],
        },
    )
    monkeypatch.setattr(
        queries,
        "list_automation_rule_snapshots",
        lambda conn, status=None, owner_ref=None, limit=50: [
            {"automation_rule_id": "automation.hubspot.contact_sync", "status": status}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_automation_rule_snapshot",
        lambda conn, automation_rule_id, include_history=True: {
            "automation_rule_id": automation_rule_id,
            "revisions": [{}] if include_history else [],
        },
    )

    listed = queries.handle_integration_action_contract_read(
        queries.QueryIntegrationActionContractRead(action="list_contracts", status="draft"),
        _subsystems(),
    )
    described = queries.handle_integration_action_contract_read(
        queries.QueryIntegrationActionContractRead(
            action="describe_contract",
            action_contract_id="integration_action.hubspot.create_contact",
        ),
        _subsystems(),
    )
    automation_list = queries.handle_integration_action_contract_read(
        queries.QueryIntegrationActionContractRead(action="list_automation_snapshots", status="active"),
        _subsystems(),
    )
    automation_describe = queries.handle_integration_action_contract_read(
        queries.QueryIntegrationActionContractRead(
            action="describe_automation_snapshot",
            automation_rule_id="automation.hubspot.contact_sync",
        ),
        _subsystems(),
    )

    assert listed["count"] == 1
    assert described["contract"]["action_contract_id"] == "integration_action.hubspot.create_contact"
    assert automation_list["count"] == 1
    assert automation_describe["automation_snapshot"]["automation_rule_id"] == "automation.hubspot.contact_sync"
