from __future__ import annotations

import pytest

from runtime.operations.commands import operator_control


class _FakeFrontdoor:
    def __init__(self, *, captured: dict[str, object]) -> None:
        self._captured = captured

    def roadmap_write(self, **kwargs):
        self._captured["kwargs"] = kwargs
        return {"ok": True}


def test_runtime_operator_write_update_maps_to_preview_by_default(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "surfaces.api.operator_write.OperatorControlFrontdoor",
        lambda: _FakeFrontdoor(captured=captured),
    )

    result = operator_control.handle_operator_roadmap_write(
        operator_control.RoadmapWriteCommand(
            action="update",
            roadmap_item_id="roadmap_item.authority.cleanup.operator_write",
            intent_brief="Refresh the summary.",
        ),
        subsystems=object(),
    )

    assert result == {"ok": True, "requested_action": "update", "dry_run": True}
    assert captured["kwargs"]["action"] == "preview"
    assert captured["kwargs"]["roadmap_item_id"] == (
        "roadmap_item.authority.cleanup.operator_write"
    )
    assert captured["kwargs"]["priority"] is None


def test_runtime_operator_write_update_preserves_explicit_priority(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "surfaces.api.operator_write.OperatorControlFrontdoor",
        lambda: _FakeFrontdoor(captured=captured),
    )

    result = operator_control.handle_operator_roadmap_write(
        operator_control.RoadmapWriteCommand(
            action="update",
            roadmap_item_id="roadmap_item.authority.cleanup.operator_write",
            priority="p2",
        ),
        subsystems=object(),
    )

    assert result == {"ok": True, "requested_action": "update", "dry_run": True}
    assert captured["kwargs"]["priority"] == "p2"


def test_runtime_operator_write_retire_commit_sets_retired_lifecycle(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "surfaces.api.operator_write.OperatorControlFrontdoor",
        lambda: _FakeFrontdoor(captured=captured),
    )

    result = operator_control.handle_operator_roadmap_write(
        operator_control.RoadmapWriteCommand(
            action="retire",
            roadmap_item_id="roadmap_item.authority.cleanup.operator_write",
            dry_run=False,
        ),
        subsystems=object(),
    )

    assert result == {"ok": True, "requested_action": "retire", "dry_run": False}
    assert captured["kwargs"]["action"] == "commit"
    assert captured["kwargs"]["lifecycle"] == "retired"


def test_runtime_operator_write_reparent_requires_parent() -> None:
    with pytest.raises(ValueError, match="parent_roadmap_item_id is required"):
        operator_control.handle_operator_roadmap_write(
            operator_control.RoadmapWriteCommand(
                action="re-parent",
                roadmap_item_id="roadmap_item.authority.cleanup.operator_write",
            ),
            subsystems=object(),
        )


def test_multi_phase_program_template_has_five_phase_children() -> None:
    """multi_phase_program template produces 5 phase placeholder children
    that an operator customizes via action='update'. This is the shape
    33.2.1 (canonical authoring contract / template pack) ships."""

    from surfaces.api.operator_write import (
        _ROADMAP_TEMPLATE_CHILDREN,
        _require_roadmap_template,
    )

    children = _ROADMAP_TEMPLATE_CHILDREN["multi_phase_program"]
    assert len(children) == 5
    assert [c.suffix for c in children] == [
        "phase_1_foundations",
        "phase_2_buildout",
        "phase_3_substrate",
        "phase_4_supervision",
        "phase_5_release",
    ]
    # Template enum accepts the new shape.
    assert _require_roadmap_template("multi_phase_program") == "multi_phase_program"
    # Each child carries an editable placeholder must_have list.
    for child in children:
        assert child.must_have, f"{child.suffix} missing must_have placeholders"
        assert "Replace this placeholder" in child.must_have[0]


def test_multi_phase_program_template_visible_in_mcp_catalog_schema() -> None:
    """MCP catalog schema accepts multi_phase_program in template enum so
    tool callers can request the new shape without raw SQL."""

    from surfaces.mcp.tools.operator import TOOLS  # noqa: PLC0415

    schema = TOOLS["praxis_operator_write"][1]["inputSchema"]
    template_enum = schema["properties"]["template"]["enum"]
    assert "multi_phase_program" in template_enum
    assert "single_capability" in template_enum
    assert "hard_cutover_program" in template_enum


def test_roadmap_write_priority_catalog_matches_update_semantics() -> None:
    """Catalog metadata must not erase the distinction between omitted and p2."""

    from surfaces.mcp.tools.operator import TOOLS  # noqa: PLC0415

    schema = TOOLS["praxis_operator_write"][1]["inputSchema"]
    priority_schema = schema["properties"]["priority"]
    assert "default" not in priority_schema
    assert priority_schema["enum"] == ["p0", "p1", "p2", "p3"]


def test_runtime_operator_write_dispatch_parity_across_action_aliases(monkeypatch) -> None:
    """Same logical input (target row + new lifecycle) reaches the frontdoor
    with the same kwargs whether the caller said action=update or action=retire.
    Action aliases are translation, not divergent code paths."""

    captured_update: dict[str, object] = {}
    captured_retire: dict[str, object] = {}

    def _frontdoor_for(captured: dict[str, object]):
        return _FakeFrontdoor(captured=captured)

    monkeypatch.setattr(
        "surfaces.api.operator_write.OperatorControlFrontdoor",
        lambda: _frontdoor_for(captured_update),
    )
    operator_control.handle_operator_roadmap_write(
        operator_control.RoadmapWriteCommand(
            action="update",
            roadmap_item_id="roadmap_item.authority.cleanup.operator_write",
            lifecycle="retired",
            dry_run=False,
        ),
        subsystems=object(),
    )

    monkeypatch.setattr(
        "surfaces.api.operator_write.OperatorControlFrontdoor",
        lambda: _frontdoor_for(captured_retire),
    )
    operator_control.handle_operator_roadmap_write(
        operator_control.RoadmapWriteCommand(
            action="retire",
            roadmap_item_id="roadmap_item.authority.cleanup.operator_write",
            dry_run=False,
        ),
        subsystems=object(),
    )

    # Both routes ship lifecycle='retired' to the same frontdoor with the
    # same target row. action='retire' is just shorthand for the lifecycle.
    assert captured_update["kwargs"]["lifecycle"] == "retired"
    assert captured_retire["kwargs"]["lifecycle"] == "retired"
    assert captured_update["kwargs"]["roadmap_item_id"] == captured_retire["kwargs"]["roadmap_item_id"]
    assert captured_update["kwargs"]["action"] == captured_retire["kwargs"]["action"] == "commit"


def test_runtime_operator_write_update_requires_roadmap_item_id() -> None:
    """update / retire / re-parent without a target row id is rejected at the
    command-handler layer, before any DB call. This is part of the validation
    gate / transaction safety contract: bad input never reaches commit."""

    for action in ("update", "retire", "re-parent", "reparent"):
        with pytest.raises(ValueError, match="roadmap_item_id is required"):
            operator_control.handle_operator_roadmap_write(
                operator_control.RoadmapWriteCommand(action=action),
                subsystems=object(),
            )
