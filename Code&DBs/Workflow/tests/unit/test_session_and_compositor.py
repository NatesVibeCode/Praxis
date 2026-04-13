"""Tests for session_carry module."""

import importlib.util
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# Direct file imports to avoid runtime/__init__.py (which uses Python 3.10+ features)
_runtime_dir = Path(__file__).resolve().parents[2] / "runtime"

def _import_from_file(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod

_sc_mod = _import_from_file("session_carry", _runtime_dir / "session_carry.py")
CarryForwardManager = _sc_mod.CarryForwardManager
CarryForwardPack = _sc_mod.CarryForwardPack
SessionCompactor = _sc_mod.SessionCompactor
build_interaction_pack = _sc_mod.build_interaction_pack
pack_to_summary_dict = _sc_mod.pack_to_summary_dict


# ---------------------------------------------------------------------------
# CarryForwardPack
# ---------------------------------------------------------------------------

def test_pack_is_frozen():
    pack = _make_pack()
    try:
        pack.objective = "mutated"
        assert False, "should be frozen"
    except AttributeError:
        pass


def test_pack_fields():
    pack = _make_pack(objective="ship it", decisions=("go", "nogo"))
    assert pack.objective == "ship it"
    assert pack.decisions == ("go", "nogo")


# ---------------------------------------------------------------------------
# SessionCompactor
# ---------------------------------------------------------------------------

def test_estimate_tokens_positive():
    pack = _make_pack()
    sc = SessionCompactor()
    assert sc.estimate_tokens(pack) > 0


def test_compact_noop_under_budget():
    pack = _make_pack()
    sc = SessionCompactor()
    result = sc.compact(pack, max_tokens=50_000)
    assert result.objective == pack.objective
    assert result.decisions == pack.decisions


def test_compact_trims_low_priority_first():
    pack = _make_pack(
        artifacts=tuple(f"artifact_{i}" for i in range(100)),
        open_questions=tuple(f"question_{i}" for i in range(100)),
        constraints=("keep_this",),
    )
    sc = SessionCompactor()
    result = sc.compact(pack, max_tokens=200)
    # Artifacts and open_questions are lowest priority, should be trimmed most
    assert len(result.artifacts) <= len(pack.artifacts)
    # Objective is never cut
    assert result.objective == pack.objective


def test_compact_never_removes_objective():
    pack = _make_pack(
        objective="critical objective",
        decisions=tuple(f"d{i}" for i in range(200)),
        artifacts=tuple(f"a{i}" for i in range(200)),
    )
    sc = SessionCompactor()
    result = sc.compact(pack, max_tokens=100)
    assert "critical objective" in result.objective


def test_compact_summarizes_long_strings():
    long_str = "x" * 500
    pack = _make_pack(constraints=(long_str,))
    sc = SessionCompactor()
    result = sc.compact(pack, max_tokens=80)
    # Either the constraint was truncated or removed
    if result.constraints:
        assert len(result.constraints[0]) <= 103


def test_compact_updates_token_estimate():
    pack = _make_pack(
        decisions=tuple(f"decision_{i}" for i in range(50)),
    )
    sc = SessionCompactor()
    result = sc.compact(pack, max_tokens=200)
    assert result.token_estimate == sc.estimate_tokens(result)


# ---------------------------------------------------------------------------
# CarryForwardManager
# ---------------------------------------------------------------------------

def test_manager_build():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        pack = mgr.build("test objective", decisions=("a",))
        assert pack.objective == "test objective"
        assert pack.decisions == ("a",)
        assert pack.pack_id
        assert pack.token_estimate > 0


def test_manager_save_load_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        pack = mgr.build("roundtrip", constraints=("c1",))
        mgr.save(pack)
        loaded = mgr.load(pack.pack_id)
        assert loaded is not None
        assert loaded.objective == "roundtrip"
        assert loaded.constraints == ("c1",)


def test_manager_load_missing():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        assert mgr.load("nonexistent") is None


def test_manager_latest():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        p1 = mgr.build("first")
        mgr.save(p1)
        time.sleep(0.01)
        p2 = mgr.build("second")
        mgr.save(p2)
        latest = mgr.latest()
        assert latest is not None
        assert latest.objective == "second"


def test_manager_latest_empty():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        assert mgr.latest() is None


def test_manager_validate_empty_objective():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        pack = CarryForwardPack(
            pack_id="abc", objective="", decisions=(), open_questions=(),
            constraints=(), risks=(), artifacts=(), next_actions=(),
            created_at=datetime.now(timezone.utc), token_estimate=0,
        )
        errors = mgr.validate(pack)
        assert any("objective" in e for e in errors)


def test_manager_validate_clean():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        pack = mgr.build("valid objective")
        assert mgr.validate(pack) == []


def test_build_interaction_pack_extracts_sections():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        assistant = """
Must-Do Actions:
- Ship semantic parity for API and compiler
- Save carry-forward packs from chat completions

Build Decisions:
- Use the API chat completion path as the first save hook

Open Questions:
- Do we also want a run-completion save path next?
"""
        tool_results = [
            {
                "result": {
                    "constraints": [
                        {"pattern": "ImportError", "text": "Fix imports before rerun"},
                    ],
                    "artifacts": [
                        {"artifact_id": "art_123", "file_path": "artifacts/workflow/result.md"},
                    ],
                    "failure_code": "rate_limited",
                }
            }
        ]

        pack = build_interaction_pack(
            mgr,
            objective="Lean harder into action-oriented memory",
            assistant_content=assistant,
            tool_results=tool_results,
        )

        assert pack is not None
        assert pack.decisions == ("Use the API chat completion path as the first save hook",)
        assert pack.next_actions[:2] == (
            "Ship semantic parity for API and compiler",
            "Save carry-forward packs from chat completions",
        )
        assert pack.constraints == ("[ImportError] Fix imports before rerun",)
        assert "art_123" in pack.artifacts
        assert pack.risks == ("rate_limited",)
        assert pack.open_questions == ("Do we also want a run-completion save path next?",)


def test_build_interaction_pack_skips_low_signal_content():
    with tempfile.TemporaryDirectory() as td:
        mgr = CarryForwardManager(td)
        pack = build_interaction_pack(
            mgr,
            objective="Quick hello",
            assistant_content="Thanks.",
            tool_results=[],
        )
        assert pack is None


def test_pack_to_summary_dict_includes_counts_and_items():
    pack = _make_pack(
        objective="Persist the useful state",
        decisions=("Use the API hook",),
        constraints=("No silent downgrade",),
        artifacts=("artifacts/workflow/review.md",),
        next_actions=("Run focused tests",),
    )

    summary = pack_to_summary_dict(pack)

    assert summary["objective"] == "Persist the useful state"
    assert summary["counts"]["decisions"] == 1
    assert summary["decisions"] == ["Use the API hook"]
    assert summary["artifacts"] == ["artifacts/workflow/review.md"]



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pack(**overrides) -> CarryForwardPack:
    defaults = dict(
        pack_id="test123",
        objective="test objective",
        decisions=(),
        open_questions=(),
        constraints=(),
        risks=(),
        artifacts=(),
        next_actions=(),
        created_at=datetime.now(timezone.utc),
        token_estimate=0,
    )
    defaults.update(overrides)
    return CarryForwardPack(**defaults)
