from __future__ import annotations

import json
from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def _env() -> dict[str, str]:
    return {"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"}


def test_native_operator_roadmap_write_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute_operation_from_env(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "action": payload["action"],
            "normalized_payload": {
                "title": payload["title"],
                "template": payload["template"],
                "parent_roadmap_item_id": payload["parent_roadmap_item_id"],
            },
            "auto_fixes": [],
            "warnings": [],
            "blocking_errors": [],
            "operation_receipt": {
                "operation_name": operation_name,
                "operation_kind": "command",
            },
            "preview": {
                "roadmap_items": [
                    {
                        "roadmap_item_id": "roadmap_item.authority.cleanup.operator_write_gate",
                    },
                ],
                "roadmap_item_dependencies": [],
            },
            "committed": True,
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
                "roadmap-write",
                "--title",
                "Unified operator write gate",
                "--brief",
                "Single validation gate for roadmap authoring",
                "--template",
                "hard_cutover_program",
                "--parent",
                "roadmap_item.authority.cleanup",
                "--priority",
                "p1",
                "--depends-on",
                "roadmap_item.authority.cleanup.validation_review",
                "--lifecycle",
                "claimed",
                "--phase-ready",
                "--commit",
            ],
            env=_env(),
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["operation_name"] == "operator.roadmap_write"
    assert captured["payload"]["action"] == "commit"
    assert captured["payload"]["title"] == "Unified operator write gate"
    assert captured["payload"]["intent_brief"] == "Single validation gate for roadmap authoring"
    assert captured["payload"]["template"] == "hard_cutover_program"
    assert captured["payload"]["parent_roadmap_item_id"] == "roadmap_item.authority.cleanup"
    assert captured["payload"]["priority"] == "p1"
    assert captured["payload"]["depends_on"] == (
        "roadmap_item.authority.cleanup.validation_review",
    )
    assert captured["payload"]["lifecycle"] == "claimed"
    assert captured["payload"]["phase_ready"] is True
    assert payload["committed"] is True
    assert payload["operation_receipt"]["operation_name"] == "operator.roadmap_write"
    assert payload["normalized_payload"]["template"] == "hard_cutover_program"
