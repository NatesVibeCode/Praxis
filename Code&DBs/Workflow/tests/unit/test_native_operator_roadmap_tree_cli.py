from __future__ import annotations

from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def _env() -> dict[str, str]:
    return {"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"}


def test_native_operator_roadmap_tree_renders_markdown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute_operation_from_env(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "root_roadmap_item_id": payload["root_roadmap_item_id"],
            "rendered_markdown": "# Unified operator write validation gate\n\n- Shared validation and normalization gate [6.7.2]",
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
                "roadmap-tree",
                "roadmap_item.authority.cleanup.unified.operator.write.validation.gate",
            ],
            env=_env(),
            stdout=stdout,
        )
        == 0
    )

    assert captured["operation_name"] == "operator.roadmap_tree"
    assert captured["payload"]["root_roadmap_item_id"] == (
        "roadmap_item.authority.cleanup.unified.operator.write.validation.gate"
    )
    assert "# Unified operator write validation gate" in stdout.getvalue()
