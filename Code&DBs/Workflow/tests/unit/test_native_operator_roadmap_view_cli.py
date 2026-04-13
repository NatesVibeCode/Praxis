from __future__ import annotations

from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def test_native_operator_roadmap_view_renders_markdown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _query_roadmap_tree(**kwargs):
        captured.update(kwargs)
        return {
            "root_roadmap_item_id": kwargs["root_roadmap_item_id"],
            "rendered_markdown": "# Unified operator write validation gate\n\n- Shared validation and normalization gate [6.7.2]",
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(native_operator.operator_read, "query_roadmap_tree", _query_roadmap_tree)

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "roadmap-view",
                "roadmap_item.authority.cleanup.unified.operator.write.validation.gate",
            ],
            env={},
            stdout=stdout,
        )
        == 0
    )

    assert captured["root_roadmap_item_id"] == (
        "roadmap_item.authority.cleanup.unified.operator.write.validation.gate"
    )
    assert "# Unified operator write validation gate" in stdout.getvalue()
