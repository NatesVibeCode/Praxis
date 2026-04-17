from __future__ import annotations

from surfaces.api._payload_contract import optional_text
from surfaces.mcp.tools import operator


def test_optional_text_treats_blank_strings_as_omitted() -> None:
    assert optional_text(None, field_name="decision_ref") is None
    assert optional_text("", field_name="decision_ref") is None
    assert optional_text("   ", field_name="decision_ref") is None
    assert optional_text(" decision.alpha ", field_name="decision_ref") == "decision.alpha"


def test_operator_write_treats_omitted_sequences_as_empty_lists(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_write(
        {
            "action": "preview",
            "title": "Payload regression",
            "intent_brief": "Verify omitted sequences do not leak None",
        }
    )

    assert result == {"ok": True}
    assert captured["operation_name"] == "operator.roadmap_write"
    assert captured["payload"]["depends_on"] == []
    assert captured["payload"]["registry_paths"] == []


def test_operator_roadmap_view_accepts_empty_input_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Subs:
        def get_pg_conn(self):
            class _Conn:
                def execute(self, *_args, **_kwargs):
                    return [{"roadmap_item_id": "roadmap_item.root"}]

            return _Conn()

    monkeypatch.setattr(operator, "_subs", _Subs())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"rendered_markdown": "# root"}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_roadmap_view({})

    assert result == {"rendered_markdown": "# root"}
    assert captured["operation_name"] == "operator.roadmap_tree"
    assert captured["payload"]["root_roadmap_item_id"] == "roadmap_item.root"
