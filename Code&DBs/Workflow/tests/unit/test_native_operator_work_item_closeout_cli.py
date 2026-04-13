from __future__ import annotations

import json
from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def test_native_operator_work_item_closeout_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _reconcile_work_item_closeout(**kwargs):
        captured.update(kwargs)
        return {
            "action": kwargs["action"],
            "proof_threshold": {
                "bug_requires_evidence_role": "validates_fix",
                "roadmap_requires_source_bug_fix_proof": True,
            },
            "evaluated": {
                "bug_ids": list(kwargs["bug_ids"]),
                "roadmap_item_ids": list(kwargs["roadmap_item_ids"]),
            },
            "candidates": {"bugs": [], "roadmap_items": []},
            "skipped": {"bugs": [], "roadmap_items": []},
            "committed": True,
            "applied": {"bugs": [], "roadmap_items": []},
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operator_write,
        "reconcile_work_item_closeout",
        _reconcile_work_item_closeout,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "work-item-closeout",
                "--bug-id",
                "bug.closeout.1",
                "--roadmap-item-id",
                "roadmap_item.closeout.1",
                "--commit",
            ],
            env={},
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["action"] == "commit"
    assert captured["bug_ids"] == ("bug.closeout.1",)
    assert captured["roadmap_item_ids"] == ("roadmap_item.closeout.1",)
    assert payload["committed"] is True
    assert payload["proof_threshold"]["bug_requires_evidence_role"] == "validates_fix"
