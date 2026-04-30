"""Regression tests for praxis_wave start auto-defining a wave from jobs=.

BUG-B9325BED: praxis_wave start previously called orch.start_wave(wave_id)
directly, which raised KeyError for any wave that had not been pre-defined
via add_wave. The documented jobs= parameter was ignored. This blocked
batch coordination from the MCP surface because nothing else in the tool
authority exposes add_wave.

Fix: start now parses jobs= into a job-definition list, calls add_wave
when the wave is absent, then starts it. Intra-wave deps use a '|'
separator so we don't collide with the ':pass' grammar already used by
record. Contract codified in
architecture-policy::wave-orchestration::start-accepts-jobs-string.
"""

from __future__ import annotations

import pytest

from surfaces.mcp.tools.wave import _parse_start_jobs, tool_praxis_wave


class _FakeOrch:
    """Minimal test stand-in exposing the orchestrator surface that
    tool_praxis_wave depends on: is_wave_defined, add_wave, start_wave,
    resolve_default_wave_id, observe (not exercised here)."""

    def __init__(self) -> None:
        self._waves: dict[str, object] = {}
        self.add_wave_calls: list[dict] = []
        self.start_wave_calls: list[str] = []

    def resolve_default_wave_id(self, *, action: str) -> str:
        raise KeyError(action)

    def is_wave_defined(self, wave_id: str) -> bool:
        return wave_id in self._waves

    def add_wave(self, wave_id: str, jobs: list[dict], depends_on_wave=None) -> None:
        self.add_wave_calls.append(
            {"wave_id": wave_id, "jobs": jobs, "depends_on_wave": depends_on_wave}
        )
        # Cheap stand-in: mark wave present so start_wave doesn't see a miss.
        self._waves[wave_id] = object()

    def start_wave(self, wave_id: str):
        if wave_id not in self._waves:
            raise KeyError(wave_id)
        self.start_wave_calls.append(wave_id)

        class _WS:
            def __init__(self, wave_id):
                self.wave_id = wave_id

                class _S:
                    value = "running"

                self.status = _S()

        return _WS(wave_id)


@pytest.fixture
def patched_subs(monkeypatch):
    """Swap in the fake orchestrator for _subs.get_wave_orchestrator()."""
    orch = _FakeOrch()
    from surfaces.mcp.tools import wave as wave_mod

    monkeypatch.setattr(wave_mod._subs, "get_wave_orchestrator", lambda: orch)
    return orch


# ---------------------------------------------------------------------- parser


def test_parse_flat_label_list():
    assert _parse_start_jobs("a,b,c") == [
        {"label": "a", "depends_on": []},
        {"label": "b", "depends_on": []},
        {"label": "c", "depends_on": []},
    ]


def test_parse_strips_whitespace_and_empty_entries():
    assert _parse_start_jobs(" a ,, b ,   ") == [
        {"label": "a", "depends_on": []},
        {"label": "b", "depends_on": []},
    ]


def test_parse_intra_wave_deps_with_pipe():
    assert _parse_start_jobs("a,b|a,c|a|b") == [
        {"label": "a", "depends_on": []},
        {"label": "b", "depends_on": ["a"]},
        {"label": "c", "depends_on": ["a", "b"]},
    ]


def test_parse_empty_string_returns_empty_list():
    assert _parse_start_jobs("") == []


# ------------------------------------------------------------- start auto-define


def test_start_auto_defines_wave_when_undefined_and_jobs_supplied(patched_subs):
    orch = patched_subs
    result = tool_praxis_wave(
        {"action": "start", "wave_id": "batch-01", "jobs": "a1,a2,a3"}
    )
    # add_wave was called with the parsed job list.
    assert len(orch.add_wave_calls) == 1
    call = orch.add_wave_calls[0]
    assert call["wave_id"] == "batch-01"
    assert [j["label"] for j in call["jobs"]] == ["a1", "a2", "a3"]
    # start_wave was then called — not the KeyError path.
    assert orch.start_wave_calls == ["batch-01"]
    assert result["started"] is True
    assert result["wave_id"] == "batch-01"
    assert "auto-defined" in result["note"]


def test_start_with_intra_wave_deps_preserves_them_on_add_wave(patched_subs):
    orch = patched_subs
    tool_praxis_wave({"action": "start", "wave_id": "w", "jobs": "a,b|a,c|b"})
    jobs = orch.add_wave_calls[0]["jobs"]
    by_label = {j["label"]: j for j in jobs}
    assert by_label["a"]["depends_on"] == []
    assert by_label["b"]["depends_on"] == ["a"]
    assert by_label["c"]["depends_on"] == ["b"]


def test_start_without_jobs_returns_structured_error(patched_subs):
    result = tool_praxis_wave({"action": "start", "wave_id": "not-defined"})
    assert result.get("reason_code") == "wave.start.undefined_and_no_jobs"
    assert "not defined" in result["error"]
    # No add_wave call was made.
    assert patched_subs.add_wave_calls == []
    assert patched_subs.start_wave_calls == []


def test_start_with_empty_jobs_returns_parse_empty_error(patched_subs):
    result = tool_praxis_wave(
        {"action": "start", "wave_id": "w", "jobs": " ,, , "}
    )
    assert result.get("reason_code") == "wave.start.jobs_parse_empty"
    assert patched_subs.add_wave_calls == []


def test_start_does_not_redefine_already_defined_wave(patched_subs):
    """If the wave is already in _waves, jobs= is ignored and start_wave is
    called directly — we must never silently overwrite an existing wave's
    job list via start."""
    orch = patched_subs
    orch._waves["existing"] = object()  # pretend it's already defined
    tool_praxis_wave(
        {"action": "start", "wave_id": "existing", "jobs": "should,be,ignored"}
    )
    assert orch.add_wave_calls == []
    assert orch.start_wave_calls == ["existing"]


def test_start_jobs_field_still_describes_both_grammars_in_schema():
    """Schema self-documentation: tooling that reads the input schema should
    see both the start and record job-string grammars. Guards against a
    future refactor hiding the start grammar and leaving operators stuck
    with the pre-fix behavior again."""
    from surfaces.mcp.tools.wave import TOOLS

    _, meta = TOOLS["praxis_wave"]
    jobs_desc = meta["inputSchema"]["properties"]["jobs"]["description"]
    assert "start" in jobs_desc.lower()
    assert "record" in jobs_desc.lower()
    assert "|" in jobs_desc  # dep separator documented
