from __future__ import annotations

import json
from io import StringIO

from surfaces.cli.commands import handoff as handoff_commands
from surfaces.cli.main import main as workflow_cli_main


def test_handoff_help_is_available() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["handoff", "--help"], stdout=stdout)

    assert rc == 0
    rendered = stdout.getvalue()
    assert "workflow handoff latest   [--artifact-kind KIND]" in rendered
    assert "workflow handoff lineage  [--artifact-kind KIND] --revision-ref REF" in rendered
    assert "workflow handoff status   --subscription-id ID --run-id ID" in rendered
    assert "workflow handoff history  [--artifact-kind KIND]" in rendered


def test_handoff_latest_dispatches_query(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_handler(query, subsystems):
        captured["query"] = query
        captured["subsystems"] = subsystems
        return {
            "artifact": {
                "artifact_kind": query.artifact_kind,
                "revision_ref": "definition-2",
                "artifact_ref": "definition-1",
                "content_hash": "hash-2",
            },
            "history": [],
            "count": 1,
        }

    monkeypatch.setattr(handoff_commands, "handle_query_handoff_latest", _fake_handler)
    stdout = StringIO()

    rc = workflow_cli_main(
        ["handoff", "latest", "--artifact-kind", "definition", "--json"],
        stdout=stdout,
    )

    assert rc == 0
    payload = json.loads(stdout.getvalue())
    assert payload["artifact"]["revision_ref"] == "definition-2"
    assert captured["query"].artifact_kind == "definition"
    assert captured["query"].artifact_ref is None
