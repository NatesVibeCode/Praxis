from __future__ import annotations

from runtime.operations.commands.promote_experiment_winner_command import (
    PromoteExperimentWinnerCommand,
    handle_promote_experiment_winner,
)


class _FakeTransaction:
    def __enter__(self) -> "_FakeConn":
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn


class _FakeConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.update_calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    def execute(self, sql: str, *params):
        self.statements.append((sql, tuple(params)))
        if sql.lstrip().upper().startswith("SELECT"):
            return [
                {
                    "task_type": "plan_synthesis",
                    "sub_task_type": "*",
                    "provider_slug": "openrouter",
                    "model_slug": "google/gemini-3-flash-preview",
                    "transport_type": "CLI",
                    "temperature": 0.0,
                    "max_tokens": 4096,
                }
            ]
        self.update_calls.append((sql, tuple(params)))
        return []


class _FakeSubsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self._conn


def test_promote_experiment_winner_updates_task_type_routing(monkeypatch) -> None:
    from runtime.operations.commands import promote_experiment_winner_command as module

    monkeypatch.setattr(
        module,
        "load_receipt_payload",
        lambda receipt_id: {
            "outputs": {
                "report": {
                    "summary_table": [
                        {
                            "config_index": 0,
                            "config": {"base_task_type": "plan_synthesis"},
                            "resolved_overrides": {
                                "temperature": 0.65,
                                "max_tokens": 8192,
                            },
                        }
                    ]
                }
            }
        },
    )

    conn = _FakeConn()
    result = handle_promote_experiment_winner(
        PromoteExperimentWinnerCommand(
            source_experiment_receipt_id="receipt:compose-experiment:1234",
            source_config_index=0,
        ),
        _FakeSubsystems(conn),
    )

    assert result["ok"] is True
    assert result["status"] == "promoted"
    assert result["target_task_type"] == "plan_synthesis"
    assert result["diff_keys"] == ["temperature", "max_tokens"]
    assert result["event_payload"]["target_provider_slug"] == "openrouter"
    assert result["event_payload"]["target_model_slug"] == "google/gemini-3-flash-preview"
    assert conn.update_calls
    update_sql, update_params = conn.update_calls[0]
    assert "UPDATE task_type_routing" in update_sql
    assert update_params[:2] == (0.65, 8192)
    assert update_params[2] == "plan_synthesis"

