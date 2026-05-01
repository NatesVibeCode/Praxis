"""Tests for Plan.source_refs polymorphic resolver.

Honors architecture-policy::platform-architecture::source-refs-plural-
canonical-shape: source_refs is the canonical internal shape; legacy from_*
fields are deprecated aliases that route through the same dispatcher;
unresolvable prefixes raise UnresolvedSourceRefError (to become typed gaps
in Phase 1.5 per architecture-policy::platform-architecture::fail-closed-
at-compile-no-silent-defaults).
"""
from __future__ import annotations

import pytest

from runtime.spec_materializer import (
    Plan,
    PlanPacket,
    UnresolvedSourceRefError,
    _classify_source_ref,
    _coerce_plan,
    _plan_packets_from_source_refs,
)


def test_classify_source_ref_known_prefixes():
    assert _classify_source_ref("BUG-ABC123") == "bug"
    assert _classify_source_ref("bug-abc123") == "bug"
    assert _classify_source_ref("roadmap_item.praxis.beta") == "roadmap_item"
    assert _classify_source_ref("idea.abc") == "idea"
    assert _classify_source_ref("operator_idea.abc") == "idea"
    assert _classify_source_ref("friction.abc") == "friction"
    assert _classify_source_ref("friction_event.abc") == "friction"


def test_classify_source_ref_unknown_prefixes_return_none():
    assert _classify_source_ref("decision.2026-04-24.X") is None
    assert _classify_source_ref("review.V") is None
    assert _classify_source_ref("discovery.Z") is None
    assert _classify_source_ref("unknown.xyz") is None
    assert _classify_source_ref("") is None


def test_plan_dataclass_accepts_source_refs():
    plan = Plan(
        name="test",
        packets=[],
        source_refs=["BUG-X", "roadmap_item.Y"],
    )
    assert plan.source_refs == ["BUG-X", "roadmap_item.Y"]
    assert plan.from_bugs is None  # legacy field independent


def test_unresolved_source_ref_error_carries_full_ref_list():
    refs = ["decision.2026-04-24.foo", "review.V"]
    err = UnresolvedSourceRefError(refs)
    assert err.unresolved_refs == refs
    message = str(err)
    assert "2 source_ref" in message
    assert "decision.2026-04-24.foo" in message
    assert "review.V" in message


def test_source_refs_empty_list_returns_empty():
    assert _plan_packets_from_source_refs([], conn=None, program_id="test") == []


def test_source_refs_with_unknown_prefix_raises_typed_error():
    with pytest.raises(UnresolvedSourceRefError) as exc_info:
        _plan_packets_from_source_refs(
            ["decision.2026-04-24.X"],
            conn=None,
            program_id="test",
        )
    assert exc_info.value.unresolved_refs == ["decision.2026-04-24.X"]


def test_source_refs_collects_all_unresolved_not_just_first():
    with pytest.raises(UnresolvedSourceRefError) as exc_info:
        _plan_packets_from_source_refs(
            ["BUG-X", "decision.Y", "review.Z"],
            conn=None,
            program_id="test",
        )
    unresolved = exc_info.value.unresolved_refs
    assert "decision.Y" in unresolved
    assert "review.Z" in unresolved
    # BUG-X resolves to a known prefix, so it should NOT be in unresolved.
    assert "BUG-X" not in unresolved


def test_source_refs_ignores_blank_and_non_string_entries():
    # Whitespace-only / non-string entries are skipped silently, not counted
    # as unresolved. (Unresolvable is specifically "prefix has no resolver.")
    with pytest.raises(UnresolvedSourceRefError):
        # Still raises because "unknown.xyz" is unresolvable.
        _plan_packets_from_source_refs(
            ["", "   ", None, "unknown.xyz"],  # type: ignore[list-item]
            conn=None,
            program_id="test",
        )


def test_source_refs_bug_prefix_routes_through_bugs_resolver(monkeypatch):
    from runtime import spec_materializer

    captured: dict = {}

    def fake_bugs(refs, *, conn, program_id):
        captured["refs"] = list(refs)
        captured["program_id"] = program_id
        return [PlanPacket(description="bug-work", write=["a.py"], bug_ref=refs[0])]

    monkeypatch.setattr(spec_materializer, "_plan_packets_from_bugs", fake_bugs)

    result = _plan_packets_from_source_refs(
        ["BUG-ABC", "BUG-DEF"],
        conn=object(),
        program_id="test.program",
    )
    assert captured["refs"] == ["BUG-ABC", "BUG-DEF"]
    assert captured["program_id"] == "test.program"
    assert len(result) == 1
    assert result[0].bug_ref == "BUG-ABC"


def test_source_refs_mixed_kinds_dispatch_per_kind(monkeypatch):
    from runtime import spec_materializer

    calls: list[tuple[str, list[str]]] = []

    def fake_bugs(refs, *, conn, program_id):
        calls.append(("bug", list(refs)))
        return [PlanPacket(description="b", write=["a"])]

    def fake_roadmap(refs, *, conn):
        calls.append(("roadmap_item", list(refs)))
        return [PlanPacket(description="r", write=["b"])]

    monkeypatch.setattr(spec_materializer, "_plan_packets_from_bugs", fake_bugs)
    monkeypatch.setattr(spec_materializer, "_plan_packets_from_roadmap_items", fake_roadmap)

    result = _plan_packets_from_source_refs(
        ["BUG-X", "roadmap_item.Y", "BUG-Z"],
        conn=object(),
        program_id="test",
    )
    assert ("bug", ["BUG-X", "BUG-Z"]) in calls
    assert ("roadmap_item", ["roadmap_item.Y"]) in calls
    assert len(result) == 2


def test_source_refs_empty_materialization_raises_specific_error(monkeypatch):
    """Per-kind error diagnostics preserved from the legacy path."""
    from runtime import spec_materializer

    monkeypatch.setattr(
        spec_materializer, "_plan_packets_from_bugs", lambda refs, *, conn, program_id: []
    )
    with pytest.raises(ValueError, match="source_refs supplied 2 bug ID.*no packets"):
        _plan_packets_from_source_refs(
            ["BUG-X", "BUG-Y"],
            conn=object(),
            program_id="test",
        )


def test_coerce_plan_rejects_both_packets_and_source_refs():
    with pytest.raises(
        ValueError,
        match="either explicit 'packets' OR source_refs",
    ):
        _coerce_plan(
            {
                "name": "test",
                "packets": [{"description": "x", "write": ["a.py"]}],
                "source_refs": ["BUG-X"],
            },
            conn=object(),
        )


def test_coerce_plan_rejects_both_packets_and_legacy_from_bugs():
    """Legacy from_* aliases still trigger the ambiguity check (they merge
    into source_refs internally)."""
    with pytest.raises(
        ValueError,
        match="either explicit 'packets' OR source_refs",
    ):
        _coerce_plan(
            {
                "name": "test",
                "packets": [{"description": "x", "write": ["a.py"]}],
                "from_bugs": ["BUG-X"],
            },
            conn=object(),
        )


def test_coerce_plan_requires_conn_for_source_refs():
    with pytest.raises(ValueError, match="require a live.*Postgres conn"):
        _coerce_plan(
            {"name": "test", "source_refs": ["BUG-X"]},
            conn=None,
        )


def test_coerce_plan_requires_conn_for_legacy_from_bugs():
    with pytest.raises(ValueError, match="require a live.*Postgres conn"):
        _coerce_plan(
            {"name": "test", "from_bugs": ["BUG-X"]},
            conn=None,
        )


def test_coerce_plan_validates_source_refs_shape():
    with pytest.raises(
        ValueError, match="plan.source_refs must be a list of ref ID strings"
    ):
        _coerce_plan(
            {"name": "test", "source_refs": [123, "BUG-X"]},
            conn=object(),
        )


def test_coerce_plan_validates_from_bugs_shape():
    with pytest.raises(
        ValueError, match="plan.from_bugs must be a list of ref ID strings"
    ):
        _coerce_plan(
            {"name": "test", "from_bugs": [{"not": "a string"}]},
            conn=object(),
        )


def test_coerce_plan_merges_from_bugs_into_source_refs(monkeypatch):
    """Legacy from_bugs input produces canonical source_refs on the Plan,
    and both source_refs and from_bugs are populated for backwards compat."""
    from runtime import spec_materializer

    monkeypatch.setattr(
        spec_materializer,
        "_plan_packets_from_bugs",
        lambda refs, *, conn, program_id: [
            PlanPacket(description="b", write=["a"], bug_ref=refs[0])
        ],
    )

    plan = _coerce_plan(
        {"name": "test", "from_bugs": ["BUG-X", "BUG-Y"]},
        conn=object(),
    )
    assert plan.source_refs == ["BUG-X", "BUG-Y"]
    assert plan.from_bugs == ["BUG-X", "BUG-Y"]
    assert plan.from_roadmap_items is None
    assert len(plan.packets) == 1


def test_coerce_plan_source_refs_mixed_with_legacy_from_roadmap(monkeypatch):
    """Caller can mix new source_refs with legacy from_roadmap_items — both
    merge into the canonical source_refs list."""
    from runtime import spec_materializer

    monkeypatch.setattr(
        spec_materializer,
        "_plan_packets_from_bugs",
        lambda refs, *, conn, program_id: [
            PlanPacket(description="b", write=["a"]) for _ in refs
        ],
    )
    monkeypatch.setattr(
        spec_materializer,
        "_plan_packets_from_roadmap_items",
        lambda refs, *, conn: [PlanPacket(description="r", write=["b"]) for _ in refs],
    )

    plan = _coerce_plan(
        {
            "name": "test",
            "source_refs": ["BUG-X"],
            "from_roadmap_items": ["roadmap_item.Y"],
        },
        conn=object(),
    )
    assert plan.source_refs == ["BUG-X", "roadmap_item.Y"]
    assert plan.from_roadmap_items == ["roadmap_item.Y"]
    assert len(plan.packets) == 2


def test_coerce_plan_passes_through_existing_plan_instance():
    """An already-constructed Plan is returned unchanged."""
    plan = Plan(
        name="prebuilt",
        packets=[PlanPacket(description="x", write=["a"])],
        source_refs=["BUG-Z"],
    )
    assert _coerce_plan(plan) is plan


def test_coerce_plan_explicit_packets_still_work_without_source_refs(monkeypatch):
    """Baseline: explicit packets path unchanged by the source_refs addition."""
    plan = _coerce_plan(
        {"name": "test", "packets": [{"description": "x", "write": ["a.py"]}]},
        conn=None,  # no conn needed when no source_refs
    )
    assert plan.source_refs is None
    assert plan.from_bugs is None
    assert len(plan.packets) == 1
    assert plan.packets[0].description == "x"
