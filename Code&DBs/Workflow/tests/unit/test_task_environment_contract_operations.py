from __future__ import annotations

from types import SimpleNamespace

from runtime.operations.commands import task_environment_contracts as commands
from runtime.operations.queries import task_environment_contracts as queries


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _contract() -> dict[str, object]:
    return {
        "contract_id": "task_contract.account_sync.1",
        "task_ref": "task.account_sync",
        "hierarchy_node_id": "task.account_sync",
        "owner_ref": "owner.ops",
        "steward_ref": "steward.ops",
        "revision_id": "rev.contract.1",
        "revision_no": 1,
        "status": "active",
        "effective_from": "2026-04-30T12:00:00Z",
        "sop_refs": [{"sop_ref": "sop.account_sync"}],
        "allowed_tools": [{"tool_ref": "tool.repo.read"}],
        "model_policy": {"model_policy_ref": "model_policy.object_truth"},
        "verifier_refs": [{"verifier_ref": "verifier.contract.behavior"}],
        "object_truth_contract_refs": ["object_truth.contract.account_sync"],
    }


def _evaluation_result() -> dict[str, object]:
    return {
        "ok": True,
        "status": "valid",
        "invalid_states": [],
        "warnings": [],
    }


def test_task_environment_contract_record_computes_hashes_persists_and_emits_event(
    monkeypatch,
) -> None:
    persist_calls: list[dict[str, object]] = []

    def _persist(conn, *, contract, evaluation_result, hierarchy_nodes, observed_by_ref=None, source_ref=None):
        persist_calls.append(
            {
                "contract": contract,
                "evaluation_result": evaluation_result,
                "hierarchy_nodes": hierarchy_nodes,
                "observed_by_ref": observed_by_ref,
                "source_ref": source_ref,
            }
        )
        return {
            "contract": {"contract_id": contract["contract_id"]},
            "revision": {"revision_id": contract["revision_id"]},
        }

    monkeypatch.setattr(commands, "persist_task_environment_contract", _persist)

    result = commands.handle_task_environment_contract_record(
        commands.RecordTaskEnvironmentContractCommand(
            contract=_contract(),
            evaluation_result=_evaluation_result(),
            hierarchy_nodes=[{"node_id": "task.account_sync", "revision_id": "rev.task.1"}],
            observed_by_ref="operator:nate",
            source_ref="phase_04_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "task_environment_contract_record"
    assert result["contract"]["contract_hash"]
    assert result["contract"]["dependency_hash"]
    assert result["event_payload"]["contract_id"] == "task_contract.account_sync.1"
    assert result["event_payload"]["hierarchy_node_count"] == 1
    assert persist_calls[0]["observed_by_ref"] == "operator:nate"
    assert persist_calls[0]["contract"]["contract_hash"] == result["contract"]["contract_hash"]


def test_task_environment_contract_read_lists_and_describes(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_task_environment_contracts",
        lambda conn, task_ref=None, status=None, limit=50: [
            {"contract_id": "task_contract.account_sync.1", "task_ref": task_ref, "status": status}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_task_environment_contract",
        lambda conn, contract_id, include_history=True: {
            "contract_id": contract_id,
            "revisions": [{}] if include_history else [],
        },
    )

    listed = queries.handle_task_environment_contract_read(
        queries.QueryTaskEnvironmentContractRead(
            action="list",
            task_ref="task.account_sync",
            status="active",
        ),
        _subsystems(),
    )
    described = queries.handle_task_environment_contract_read(
        queries.QueryTaskEnvironmentContractRead(
            action="describe",
            contract_id="task_contract.account_sync.1",
        ),
        _subsystems(),
    )

    assert listed["count"] == 1
    assert listed["items"][0]["task_ref"] == "task.account_sync"
    assert described["ok"] is True
    assert described["contract"]["contract_id"] == "task_contract.account_sync.1"
