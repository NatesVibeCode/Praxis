"""Tests for enriched LaunchReceipt.packet_map fields.

Honors architecture-policy::platform-architecture::expected-envelope-vs-
actual-truth-separation: compile-time packet_map carries the pre-run
envelope (inferred_stage, resolved_agent, capabilities, write_envelope,
expected_gates, verification_gaps). Post-run truth lands in a separate
receipt surface when Phase 3 wires actual_write_set back from the runtime.
"""
from __future__ import annotations

import pytest

from runtime.spec_materializer import (
    PlanPacket,
    _bind_packet_data_pills,
    _build_packet_map_entry,
    _compute_verification_gaps,
    _file_has_admitted_verifier,
)


# ---------------------------------------------------------------------------
# _file_has_admitted_verifier — shared predicate
# ---------------------------------------------------------------------------


def test_admitted_verifier_python_files():
    assert _file_has_admitted_verifier("foo.py") is True
    assert _file_has_admitted_verifier("path/to/test_bar.py") is True


def test_admitted_verifier_non_python_files():
    assert _file_has_admitted_verifier("foo.js") is False
    assert _file_has_admitted_verifier("styles.css") is False
    assert _file_has_admitted_verifier("migration.sql") is False
    assert _file_has_admitted_verifier("README.md") is False
    assert _file_has_admitted_verifier("config.toml") is False


def test_admitted_verifier_empty_and_malformed():
    assert _file_has_admitted_verifier("") is False
    assert _file_has_admitted_verifier(None) is False  # type: ignore[arg-type]
    assert _file_has_admitted_verifier("no_extension") is False


# ---------------------------------------------------------------------------
# _compute_verification_gaps — gap surfacer
# ---------------------------------------------------------------------------


def test_verification_gaps_all_python_is_empty():
    gaps = _compute_verification_gaps(["a.py", "path/test_b.py"])
    assert gaps == []


def test_verification_gaps_names_non_python_files_structured():
    gaps = _compute_verification_gaps(["a.py", "b.js", "c.sql", "README.md"])
    assert len(gaps) == 3
    files = sorted(g["file"] for g in gaps)
    assert files == ["README.md", "b.js", "c.sql"]
    for gap in gaps:
        assert gap["missing_type"] == "verifier"
        assert gap["reason_code"] == "verifier.no_admitted_for_extension"


def test_verification_gaps_skip_empty_file_entries():
    """Blank entries are skipped — they're not 'gaps', they're invalid input."""
    gaps = _compute_verification_gaps(["a.py", "", "   "])
    assert gaps == []


# ---------------------------------------------------------------------------
# _build_packet_map_entry — enriched shape
# ---------------------------------------------------------------------------


def _make_job(**overrides) -> dict:
    base: dict = {
        "label": "job-label",
        "agent": "auto/build",
        "prompt": "...",
        "task_type": "build",
        "write_scope": ["foo.py"],
        "workdir": "/repo",
    }
    base.update(overrides)
    return base


def test_packet_map_entry_preserves_legacy_fields_with_packet():
    packet = PlanPacket(
        description="do it",
        write=["foo.py"],
        stage="build",
        bug_ref="BUG-123",
        bug_refs=["BUG-123", "BUG-456"],
    )
    job = _make_job()
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["label"] == "job-label"
    assert entry["bug_ref"] == "BUG-123"
    assert entry["bug_refs"] == ["BUG-123", "BUG-456"]
    assert entry["agent"] == "auto/build"
    assert entry["stage"] == "build"


def test_packet_map_entry_includes_all_derived_fields():
    packet = PlanPacket(description="x", write=["foo.py", "bar.md"], stage="build")
    job = _make_job(
        capabilities=["cap.1", "cap.2"],
        verify_refs=["verify_ref.abc", "verify_ref.def"],
    )
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["inferred_stage"] == "build"
    assert entry["resolved_agent"] == "auto/build"
    assert entry["capabilities"] == ["cap.1", "cap.2"]
    assert entry["write_envelope"] == ["foo.py", "bar.md"]
    assert entry["expected_gates"] == ["verify_ref.abc", "verify_ref.def"]
    # Markdown has no admitted verifier → gap.
    gaps = entry["verification_gaps"]
    assert len(gaps) == 1
    assert gaps[0]["file"] == "bar.md"


def test_packet_map_entry_empty_lists_when_compile_omitted_fields():
    """Jobs missing capabilities/verify_refs default to empty lists, not None."""
    packet = PlanPacket(description="x", write=["foo.py"], stage="build")
    job = _make_job()  # no capabilities, no verify_refs
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["capabilities"] == []
    assert entry["expected_gates"] == []
    assert entry["verification_gaps"] == []


def test_packet_map_entry_verification_gaps_for_mixed_write_scope():
    packet = PlanPacket(
        description="x",
        write=["main.py", "view.tsx", "schema.sql", "doc.md"],
        stage="build",
    )
    job = _make_job(write_scope=list(packet.write))
    entry = _build_packet_map_entry(packet=packet, job=job)
    gap_files = sorted(g["file"] for g in entry["verification_gaps"])
    # Python is admitted; everything else is a gap.
    assert gap_files == ["doc.md", "schema.sql", "view.tsx"]


def test_packet_map_entry_builds_from_job_only_for_launch_proposed_path():
    """When no PlanPacket is available (launch_proposed path), the helper
    builds from job-level fields only. bug_ref/bug_refs remain None unless
    the job dict mirrors them (launch_proposed doesn't today, which is OK)."""
    job = _make_job(
        task_type="review",
        capabilities=["cap.x"],
        verify_refs=["vr.1"],
    )
    entry = _build_packet_map_entry(job=job)
    assert entry["label"] == "job-label"
    assert entry["bug_ref"] is None
    assert entry["bug_refs"] is None
    assert entry["stage"] == "review"
    assert entry["inferred_stage"] == "review"
    assert entry["resolved_agent"] == "auto/build"
    assert entry["capabilities"] == ["cap.x"]
    assert entry["write_envelope"] == ["foo.py"]
    assert entry["expected_gates"] == ["vr.1"]
    assert entry["verification_gaps"] == []


def test_packet_map_entry_bug_refs_independent_from_bug_ref():
    """Packets with only bug_refs (no single bug_ref) still surface the list."""
    packet = PlanPacket(
        description="x",
        write=["foo.py"],
        stage="fix",
        bug_refs=["BUG-A", "BUG-B"],
    )
    job = _make_job()
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["bug_ref"] is None
    assert entry["bug_refs"] == ["BUG-A", "BUG-B"]


def test_packet_map_entry_write_envelope_is_copy_not_reference():
    """write_envelope is a fresh list — mutating it does not affect the source packet."""
    packet = PlanPacket(description="x", write=["foo.py"], stage="build")
    job = _make_job()
    entry = _build_packet_map_entry(packet=packet, job=job)
    entry["write_envelope"].append("mutated.py")
    assert packet.write == ["foo.py"]


def test_packet_map_entry_inferred_stage_falls_back_to_declared():
    """When job lacks task_type, inferred_stage falls back to packet.stage."""
    packet = PlanPacket(description="x", write=["foo.py"], stage="test")
    job = _make_job()
    job.pop("task_type", None)
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["inferred_stage"] == "test"


def test_packet_map_entry_resolved_agent_mirrors_compile_time_agent():
    """resolved_agent is the post-compile agent string. At compile it still
    may contain the ``auto/{stage}`` placeholder; dispatch-time resolution
    overwrites later. The field exists so reconciliation has a home."""
    packet = PlanPacket(
        description="x",
        write=["foo.py"],
        stage="build",
        agent="openai/gpt-5",
    )
    job = _make_job(agent="openai/gpt-5")
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["agent"] == "openai/gpt-5"
    assert entry["resolved_agent"] == "openai/gpt-5"


def test_packet_map_entry_verification_gaps_list_is_always_present():
    """verification_gaps key must exist even when empty, so consumers can
    iterate unconditionally."""
    packet = PlanPacket(description="x", write=["foo.py"], stage="build")
    job = _make_job()
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert "verification_gaps" in entry
    assert entry["verification_gaps"] == []


# ---------------------------------------------------------------------------
# _bind_packet_data_pills + data_pills in packet_map (Phase 1.1.f)
# ---------------------------------------------------------------------------


def test_bind_packet_data_pills_empty_description_returns_zero_pills():
    result = _bind_packet_data_pills("", conn=None)
    assert result == {"bound": [], "ambiguous": [], "unbound": [], "warnings": []}


def test_bind_packet_data_pills_whitespace_only_returns_zero_pills():
    result = _bind_packet_data_pills("   \n   ", conn=None)
    assert result == {"bound": [], "ambiguous": [], "unbound": [], "warnings": []}


def test_bind_packet_data_pills_catches_module_import_failure(monkeypatch):
    """When intent_binding can't be imported (degraded data-dict substrate),
    we surface a warning rather than raising — launch compiler must not
    block on degraded binding."""
    import runtime.spec_materializer as spec_mod
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "runtime.intent_binding":
            raise ImportError("intent_binding unavailable (simulated)")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    result = _bind_packet_data_pills("Update users.first_name.", conn=None)
    assert result["bound"] == []
    assert any(
        w.startswith("intent_binding unavailable") for w in result["warnings"]
    )


def test_bind_packet_data_pills_catches_per_call_failure(monkeypatch):
    """When bind_data_pills raises on a specific call (e.g., DB blip), we
    fold the error into warnings and return zero-binding instead of raising."""
    import runtime.intent_binding as ib_mod

    def broken_bind(intent, *, conn, object_kinds=None):
        raise RuntimeError("simulated DB blip")

    monkeypatch.setattr(ib_mod, "bind_data_pills", broken_bind)

    result = _bind_packet_data_pills("Update users.first_name.", conn=object())
    assert result["bound"] == []
    assert any(
        w.startswith("bind_data_pills failed") for w in result["warnings"]
    )


def test_bind_packet_data_pills_returns_normalized_shape(monkeypatch):
    """Helper always returns all four keys (bound/ambiguous/unbound/warnings),
    even if bind_data_pills returns missing keys."""
    import runtime.intent_binding as ib_mod

    class _FakeBoundIntent:
        def to_dict(self):
            return {"intent": "...", "bound": [{"object_kind": "users", "field_path": "first_name"}]}

    def fake_bind(intent, *, conn, object_kinds=None):
        return _FakeBoundIntent()

    monkeypatch.setattr(ib_mod, "bind_data_pills", fake_bind)

    result = _bind_packet_data_pills("Update users.first_name.", conn=object())
    assert "bound" in result
    assert "ambiguous" in result
    assert "unbound" in result
    assert "warnings" in result
    assert len(result["bound"]) == 1
    assert result["bound"][0]["object_kind"] == "users"


def test_packet_map_entry_includes_data_pills_key():
    """Every packet_map entry has a data_pills dict, even when empty."""
    packet = PlanPacket(description="x", write=["a.py"], stage="build")
    job = _make_job()
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert "data_pills" in entry
    assert isinstance(entry["data_pills"], dict)
    assert "bound" in entry["data_pills"]
    assert "ambiguous" in entry["data_pills"]
    assert "unbound" in entry["data_pills"]
    assert "warnings" in entry["data_pills"]


def test_packet_map_entry_surfaces_attached_data_pills():
    """When compile_plan attaches data_pills to the job dict, the packet_map
    entry surfaces them verbatim."""
    packet = PlanPacket(description="x", write=["a.py"], stage="build")
    bound_pill = {
        "object_kind": "users",
        "field_path": "first_name",
        "field_kind": "text",
    }
    job = _make_job(
        data_pills={
            "bound": [bound_pill],
            "ambiguous": [],
            "unbound": [],
            "warnings": [],
        }
    )
    entry = _build_packet_map_entry(packet=packet, job=job)
    assert entry["data_pills"]["bound"] == [bound_pill]


def test_packet_map_entry_data_pills_is_outer_dict_copy():
    """The entry's data_pills is a separate dict instance (shallow copy via
    dict()). Inner lists remain shared by reference — deep-copy would be
    overkill and the helper stays cheap."""
    packet = PlanPacket(description="x", write=["a.py"], stage="build")
    source_pills = {
        "bound": [],
        "ambiguous": [],
        "unbound": [],
        "warnings": [],
    }
    job = _make_job(data_pills=source_pills)
    entry = _build_packet_map_entry(packet=packet, job=job)
    # Outer dict is a distinct instance — adding a new key to the entry's
    # data_pills doesn't affect the source.
    assert entry["data_pills"] is not source_pills
    entry["data_pills"]["new_key"] = "value"
    assert "new_key" not in source_pills
