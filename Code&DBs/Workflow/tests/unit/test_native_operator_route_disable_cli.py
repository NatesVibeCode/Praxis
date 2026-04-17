from __future__ import annotations

import json
from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def test_native_operator_route_disable_records_timed_provider_window(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute_operation_from_env(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "task_route_eligibility": {
                "task_route_eligibility_id": "task-route-eligibility.anthropic.build.claude-sonnet-4-6.rejected.20260408T160000Z",
                "provider_slug": payload["provider_slug"],
                "task_type": payload["task_type"],
                "model_slug": payload["model_slug"],
                "eligibility_status": payload["eligibility_status"],
                "reason_code": payload["reason_code"],
                "rationale": payload["rationale"],
                "effective_from": "2026-04-08T16:00:00+00:00",
                "effective_to": payload["effective_to"].isoformat(),
                "decision_ref": "decision:task-route-eligibility:anthropic:build:claude-sonnet-4-6:rejected:20260408T160000Z",
                "created_at": "2026-04-08T16:00:00+00:00",
            },
            "superseded_task_route_eligibility_ids": [],
            "command_receipt": {
                "operation_name": operation_name,
                "operation_kind": "command",
            },
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operation_catalog_gateway,
        "execute_operation_from_env",
        _execute_operation_from_env,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "route-disable",
                "anthropic",
                "2026-04-10T09:00:00-07:00",
                "--task-type",
                "build",
                "--model",
                "claude-sonnet-4-6",
                "--reason",
                "provider_disabled",
                "--rationale",
                "Anthropic off until Friday morning",
            ],
            env={},
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["operation_name"] == "operator.task_route_eligibility"
    assert captured["payload"]["provider_slug"] == "anthropic"
    assert captured["payload"]["eligibility_status"] == "rejected"
    assert captured["payload"]["task_type"] == "build"
    assert captured["payload"]["model_slug"] == "claude-sonnet-4-6"
    assert captured["payload"]["reason_code"] == "provider_disabled"
    assert captured["payload"]["rationale"] == "Anthropic off until Friday morning"
    assert payload["task_route_eligibility"]["provider_slug"] == "anthropic"
    assert payload["task_route_eligibility"]["effective_to"] == "2026-04-10T09:00:00-07:00"
    assert payload["command_receipt"]["operation_name"] == "operator.task_route_eligibility"
