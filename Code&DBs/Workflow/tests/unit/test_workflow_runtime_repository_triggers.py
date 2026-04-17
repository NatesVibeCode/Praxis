from __future__ import annotations

import json

from storage.postgres import workflow_runtime_repository as repo


class _TriggerConn:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith(
            "SELECT id, workflow_id, event_type, filter, cron_expression, enabled, source_trigger_id FROM workflow_triggers WHERE workflow_id = $1"
        ):
            workflow_id = str(args[0])
            return [dict(row) for row in self.rows if str(row.get("workflow_id")) == workflow_id]
        if normalized.startswith("DELETE FROM workflow_triggers WHERE id = $1"):
            trigger_id = str(args[0])
            self.rows = [row for row in self.rows if str(row.get("id")) != trigger_id]
            return []
        return []

    def fetchval(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT 1 FROM workflow_triggers WHERE id = $1"):
            trigger_id = str(args[0])
            return 1 if any(str(row.get("id")) == trigger_id for row in self.rows) else None
        return None

    def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE workflow_triggers SET"):
            trigger_id = str(args[0])
            row = next((item for item in self.rows if str(item.get("id")) == trigger_id), None)
            if row is None:
                return None
            updated = dict(row)
            param_index = 1
            if "workflow_id =" in query:
                updated["workflow_id"] = args[param_index]
                param_index += 1
            if "source_trigger_id =" in query:
                updated["source_trigger_id"] = args[param_index]
                param_index += 1
            if "event_type =" in query:
                updated["event_type"] = args[param_index]
                param_index += 1
            if "filter =" in query:
                updated["filter"] = json.loads(args[param_index])
                param_index += 1
            if "cron_expression =" in query:
                updated["cron_expression"] = args[param_index]
                param_index += 1
            if "enabled =" in query:
                updated["enabled"] = args[param_index]
            self.rows = [updated if str(item.get("id")) == trigger_id else item for item in self.rows]
            return updated
        if normalized.startswith("INSERT INTO workflow_triggers"):
            source_trigger_id = args[2]
            row = {
                "id": args[0],
                "workflow_id": args[1],
                "source_trigger_id": source_trigger_id,
                "event_type": args[3],
                "filter": json.loads(args[4]),
                "enabled": args[5],
                "cron_expression": args[6],
            }
            self.rows.append(row)
            return row
        return None


def test_reconcile_workflow_triggers_preserves_manual_triggers_and_owns_source_ids() -> None:
    conn = _TriggerConn(
        [
            {
                "id": "trg_manual",
                "workflow_id": "wf_123",
                "source_trigger_id": None,
                "event_type": "manual",
                "filter": {},
                "enabled": True,
                "cron_expression": None,
            },
            {
                "id": "trg_compiled",
                "workflow_id": "wf_123",
                "source_trigger_id": "trigger-001",
                "event_type": "schedule",
                "filter": {"minute": "0"},
                "enabled": True,
                "cron_expression": "0 * * * *",
            },
        ]
    )

    persisted = repo.reconcile_workflow_triggers(
        conn,
        workflow_id="wf_123",
        compiled_spec={
            "triggers": [
                {
                    "source_trigger_id": "trigger-001",
                    "event_type": "schedule",
                    "filter": {"minute": "0"},
                    "cron_expression": "0 * * * *",
                }
            ]
        },
    )

    assert len(persisted) == 1
    assert persisted[0]["id"] == "trg_compiled"
    assert persisted[0]["source_trigger_id"] == "trigger-001"
    assert any(row["id"] == "trg_manual" for row in conn.rows)
    assert any(row["id"] == "trg_compiled" and row["source_trigger_id"] == "trigger-001" for row in conn.rows)
    assert not any(
        query.strip() == "DELETE FROM workflow_triggers WHERE workflow_id = $1"
        for query, _ in conn.calls
    )
