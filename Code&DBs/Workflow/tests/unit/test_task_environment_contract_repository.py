from __future__ import annotations

from storage.postgres import task_environment_contract_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO task_environment_contract_heads" in sql:
            return {
                "contract_id": args[0],
                "task_ref": args[1],
                "hierarchy_node_id": args[2],
                "status": args[3],
                "current_revision_id": args[4],
                "current_contract_hash": args[5],
                "dependency_hash": args[6],
                "evaluation_status": args[9],
                "invalid_state_count": args[10],
                "warning_count": args[11],
                "contract_json": args[12],
                "evaluation_result_json": args[13],
            }
        if "INSERT INTO task_environment_contract_revisions" in sql:
            return {
                "contract_id": args[0],
                "revision_id": args[1],
                "revision_no": args[2],
                "parent_revision_id": args[3],
                "contract_hash": args[4],
                "dependency_hash": args[5],
                "status": args[6],
                "contract_json": args[9],
                "evaluation_result_json": args[10],
            }
        return None

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


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
        "contract_hash": "contract.digest",
        "dependency_hash": "dependency.digest",
    }


def _evaluation_result() -> dict[str, object]:
    return {
        "ok": True,
        "status": "valid_with_warnings",
        "invalid_states": [],
        "warnings": [
            {
                "reason_code": "task_contract.warning.demo",
                "severity": "warning",
                "field_ref": "contract.demo",
            }
        ],
    }


def _hierarchy_node() -> dict[str, object]:
    return {
        "node_id": "task.account_sync",
        "revision_id": "rev.task.1",
        "parent_node_id": "workflow.account_sync",
        "node_type": "task",
        "node_name": "Account Sync",
        "status": "active",
        "owner_ref": "owner.ops",
        "steward_ref": "steward.ops",
    }


def test_persist_task_environment_contract_writes_revision_scoped_records() -> None:
    conn = _RecordingConn()

    result = repo.persist_task_environment_contract(
        conn,
        contract=_contract(),
        evaluation_result=_evaluation_result(),
        hierarchy_nodes=[_hierarchy_node()],
        observed_by_ref="operator:nate",
        source_ref="phase_04_test",
    )

    assert "INSERT INTO task_environment_contract_heads" in conn.fetchrow_calls[0][0]
    assert "INSERT INTO task_environment_contract_revisions" in conn.fetchrow_calls[1][0]
    assert len(conn.execute_calls) == 2
    assert (
        "DELETE FROM task_environment_hierarchy_nodes WHERE contract_id = $1 AND revision_id = $2"
        in conn.execute_calls[0][0]
    )
    assert conn.execute_calls[0][1] == ("task_contract.account_sync.1", "rev.contract.1")
    assert any("task_environment_hierarchy_nodes" in call[0] for call in conn.batch_calls)
    assert any("task_environment_contract_invalid_states" in call[0] for call in conn.batch_calls)
    assert result["contract"]["contract_id"] == "task_contract.account_sync.1"
    assert result["revision"]["revision_id"] == "rev.contract.1"
    assert result["hierarchy_node_count"] == 1
    assert result["warning_count"] == 1
