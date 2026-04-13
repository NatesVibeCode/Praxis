from __future__ import annotations

import json
from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def test_native_operator_roadmap_write_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _roadmap_write(**kwargs):
        captured.update(kwargs)
        return {
            "action": kwargs["action"],
            "normalized_payload": {
                "title": kwargs["title"],
                "template": kwargs["template"],
                "parent_roadmap_item_id": kwargs["parent_roadmap_item_id"],
            },
            "auto_fixes": [],
            "warnings": [],
            "blocking_errors": [],
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
    monkeypatch.setattr(native_operator.operator_write, "roadmap_write", _roadmap_write)

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
                "--phase-ready",
                "--commit",
            ],
            env={},
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["action"] == "commit"
    assert captured["title"] == "Unified operator write gate"
    assert captured["intent_brief"] == "Single validation gate for roadmap authoring"
    assert captured["template"] == "hard_cutover_program"
    assert captured["parent_roadmap_item_id"] == "roadmap_item.authority.cleanup"
    assert captured["priority"] == "p1"
    assert captured["depends_on"] == (
        "roadmap_item.authority.cleanup.validation_review",
    )
    assert captured["phase_ready"] is True
    assert payload["committed"] is True
    assert payload["normalized_payload"]["template"] == "hard_cutover_program"
