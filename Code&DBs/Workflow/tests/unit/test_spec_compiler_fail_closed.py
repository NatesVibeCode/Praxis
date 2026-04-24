"""Tests for fail-closed-at-compile policy in spec_compiler.

Honors architecture-policy::platform-architecture::fail-closed-at-compile-
no-silent-defaults: unknown packet.stage must raise a typed UnresolvedStageError
at compile rather than silently produce auto/{stage} that later KeyErrors at
_generate_prompt. All unresolved stages collected atomically before raise,
matching the CompilePlanError shape.
"""
from __future__ import annotations

import pytest

from runtime.spec_compiler import (
    CompilePlanError,
    Plan,
    PlanPacket,
    UnresolvedStageError,
    UnresolvedWriteScopeError,
    _STAGE_TEMPLATES,
    compile_plan,
)


class _StubConn:
    """Minimal conn stub — spec_compiler.compile_plan doesn't touch the DB
    on the explicit-packets path (only when source_refs are supplied)."""

    def execute(self, sql: str, *args):
        return []


def test_unknown_stage_raises_unresolved_stage_error():
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="do it", write=["a.py"], stage="rumble"),
        ],
    )
    with pytest.raises(UnresolvedStageError) as exc:
        compile_plan(plan, conn=_StubConn())
    assert len(exc.value.unresolved_stages) == 1
    assert exc.value.unresolved_stages[0]["stage"] == "rumble"


def test_multiple_unknown_stages_collected_atomically():
    """All unresolved stages reported in one pass — no fix-one-retry."""
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="a", write=["a.py"], stage="rumble"),
            PlanPacket(description="b", write=["b.py"], stage="build"),
            PlanPacket(description="c", write=["c.py"], stage="quux"),
            PlanPacket(description="d", write=["d.py"], stage="zig"),
        ],
    )
    with pytest.raises(UnresolvedStageError) as exc:
        compile_plan(plan, conn=_StubConn())
    stages = [entry["stage"] for entry in exc.value.unresolved_stages]
    assert sorted(stages) == ["quux", "rumble", "zig"]


def test_unresolved_stage_error_message_lists_known_stages():
    err = UnresolvedStageError(
        [{"index": 0, "label": "x", "stage": "mystery"}]
    )
    msg = str(err)
    assert "mystery" in msg
    # All known stages should be in the message.
    for known in _STAGE_TEMPLATES:
        assert known in msg


def test_unresolved_stage_takes_precedence_over_other_compile_failures():
    """When both unresolved stages AND other compile failures exist, stage
    errors are raised first so the caller fixes them first."""
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="a", write=["a.py"], stage="mystery"),
        ],
    )
    # Stage is mystery (unresolved); if compile_spec were also called, it would
    # KeyError at _generate_prompt — but pre-validation catches it first.
    with pytest.raises(UnresolvedStageError):
        compile_plan(plan, conn=_StubConn())


def test_known_stages_compile_successfully():
    """Baseline: all 5 admitted stages pass through without UnresolvedStageError."""
    for stage in sorted(_STAGE_TEMPLATES.keys()):
        plan = Plan(
            name=f"test-{stage}",
            packets=[
                PlanPacket(description="x", write=["a.py"], stage=stage),
            ],
        )
        # Should not raise UnresolvedStageError — either succeeds or raises
        # CompilePlanError for some OTHER reason (DB-less stub), but not for stage.
        try:
            compile_plan(plan, conn=_StubConn())
        except UnresolvedStageError:
            pytest.fail(f"stage={stage!r} should be admitted, raised UnresolvedStageError")
        except Exception:
            # Other failures (e.g., CompilePlanError from missing authority) are OK —
            # we only care that stage admission passed.
            pass


def test_unresolved_stage_error_preserves_packet_label_and_index():
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="first", write=["a"], stage="build", label="first_pkt"),
            PlanPacket(description="second", write=["b"], stage="mystery", label="second_pkt"),
        ],
    )
    with pytest.raises(UnresolvedStageError) as exc:
        compile_plan(plan, conn=_StubConn())
    assert len(exc.value.unresolved_stages) == 1
    entry = exc.value.unresolved_stages[0]
    assert entry["index"] == 1
    assert entry["label"] == "second_pkt"
    assert entry["stage"] == "mystery"


# ---------------------------------------------------------------------------
# UnresolvedWriteScopeError (Phase 1.1.e) — write scope fail-closed
# ---------------------------------------------------------------------------


def test_plan_packet_write_is_optional_default_empty_list():
    """PlanPacket.write now defaults to [] — caller tax 3→1 (description only
    required). Empty write is allowed at dataclass level but rejected at
    compile by fail-closed policy."""
    packet = PlanPacket(description="describe only")
    assert packet.write == []


def test_empty_write_raises_unresolved_write_scope_error():
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="do a thing", write=[], stage="build"),
        ],
    )
    with pytest.raises(UnresolvedWriteScopeError) as exc:
        compile_plan(plan, conn=_StubConn())
    assert len(exc.value.unresolved_writes) == 1
    entry = exc.value.unresolved_writes[0]
    assert entry["description_preview"] == "do a thing"
    assert entry["index"] == 0


def test_empty_write_collected_atomically_across_packets():
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="a", write=[], stage="build"),
            PlanPacket(description="b", write=["ok.py"], stage="build"),
            PlanPacket(description="c", write=[], stage="build"),
        ],
    )
    with pytest.raises(UnresolvedWriteScopeError) as exc:
        compile_plan(plan, conn=_StubConn())
    indices = sorted(entry["index"] for entry in exc.value.unresolved_writes)
    assert indices == [0, 2]


def test_unresolved_write_scope_error_message_lists_repair_paths():
    err = UnresolvedWriteScopeError(
        [{"index": 0, "label": "x", "description_preview": "do it"}]
    )
    msg = str(err)
    assert "1 packet(s) have empty write scope" in msg
    # Repair-path guidance must be present.
    assert "add explicit write=" in msg
    assert "source_refs" in msg
    assert "scope_resolver" in msg


def test_write_scope_error_takes_priority_over_stage_error():
    """A packet with BOTH empty write AND unknown stage surfaces the write
    error first — caller fixes the most fundamental problem (no output
    target) before worrying about stage vocabulary."""
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="a", write=[], stage="mystery"),
            PlanPacket(description="b", write=["b.py"], stage="quux"),
        ],
    )
    with pytest.raises(UnresolvedWriteScopeError) as exc:
        compile_plan(plan, conn=_StubConn())
    # Only the empty-write packet is in unresolved_writes; the stage-error
    # packet is NOT included here because `continue` skipped the stage
    # check after the write check fired. Caller fixes write first, retries,
    # THEN sees the stage error on the retry.
    assert len(exc.value.unresolved_writes) == 1
    assert exc.value.unresolved_writes[0]["index"] == 0


def test_write_scope_description_preview_truncates_at_80_chars():
    long_description = "x" * 200
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description=long_description, write=[], stage="build"),
        ],
    )
    with pytest.raises(UnresolvedWriteScopeError) as exc:
        compile_plan(plan, conn=_StubConn())
    preview = exc.value.unresolved_writes[0]["description_preview"]
    assert len(preview) == 80
    assert preview == "x" * 80


def test_non_empty_write_passes_pre_validation():
    """Baseline: packets with write populated don't trigger UnresolvedWriteScopeError."""
    plan = Plan(
        name="test",
        packets=[
            PlanPacket(description="x", write=["a.py"], stage="build"),
        ],
    )
    # May raise for OTHER reasons (e.g., CompilePlanError via stub conn),
    # but not UnresolvedWriteScopeError.
    try:
        compile_plan(plan, conn=_StubConn())
    except UnresolvedWriteScopeError:
        pytest.fail("non-empty write should not raise UnresolvedWriteScopeError")
    except Exception:
        pass  # other failures are acceptable


# ---------------------------------------------------------------------------
# compile_plan emits typed_gap.created events at error raise (Phase 1.6 wiring)
# ---------------------------------------------------------------------------


class _EventRecordingConn:
    """Stub conn that captures every execute() call — used to verify
    compile_plan emits system_events before raising."""

    def __init__(self) -> None:
        self.events: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.events.append((sql, args))
        return []


def _system_event_inserts(conn: _EventRecordingConn) -> list[tuple[str, tuple]]:
    return [
        (sql, args) for (sql, args) in conn.events
        if "INSERT INTO system_events" in sql
    ]


def test_compile_plan_emits_typed_gap_events_for_write_scope_errors():
    conn = _EventRecordingConn()
    plan = Plan(
        name="emit_test",
        packets=[
            PlanPacket(description="a", write=[], stage="build"),
            PlanPacket(description="b", write=[], stage="build"),
        ],
    )
    with pytest.raises(UnresolvedWriteScopeError):
        compile_plan(plan, conn=conn)
    inserts = _system_event_inserts(conn)
    assert len(inserts) == 2  # one event per unresolved write entry
    for sql, args in inserts:
        assert args[0] == "typed_gap.created"
        assert args[2] == "typed_gap"  # source_type


def test_compile_plan_emits_typed_gap_events_for_stage_errors():
    conn = _EventRecordingConn()
    plan = Plan(
        name="emit_test",
        packets=[
            PlanPacket(description="a", write=["a.py"], stage="mystery"),
            PlanPacket(description="b", write=["b.py"], stage="rumble"),
        ],
    )
    with pytest.raises(UnresolvedStageError):
        compile_plan(plan, conn=conn)
    inserts = _system_event_inserts(conn)
    assert len(inserts) == 2
    for sql, args in inserts:
        assert args[0] == "typed_gap.created"


def test_compile_plan_emission_is_best_effort_even_if_events_fail(monkeypatch):
    """If the emit helper raises (e.g., degraded system_events), compile_plan
    still raises the original error — emission is never a blocker."""
    import runtime.typed_gap_events as tge

    def broken(*args, **kwargs):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr(tge, "emit_typed_gaps_for_compile_errors", broken)

    plan = Plan(
        name="emit_test",
        packets=[PlanPacket(description="x", write=[], stage="build")],
    )
    with pytest.raises(UnresolvedWriteScopeError):
        compile_plan(plan, conn=_EventRecordingConn())
