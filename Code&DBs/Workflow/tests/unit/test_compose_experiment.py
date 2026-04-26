"""Unit tests for runtime.compose_experiment.

The runner orchestrates parallel ``compose_plan_via_llm`` calls. Tests
mock that function with synchronous stubs so the matrix logic — config
validation, ranking, error handling — is exercised without live LLM
calls or DB connections.
"""
from __future__ import annotations

import time
import types

import pytest

from runtime import compose_experiment
from runtime.compose_experiment import (
    ComposeExperimentReport,
    ComposeExperimentRun,
    _normalize_config,
    _rank,
    run_compose_experiment,
)
from runtime.compose_plan_via_llm import ComposeViaLLMResult
from runtime.intent_binding import BoundIntent
from runtime.intent_dependency import SkeletalPlan
from runtime.intent_suggestion import SuggestedAtoms
from runtime.plan_fork_author import AuthoredPlan
from runtime.plan_section_validator import ValidationReport
from runtime.plan_synthesis import PacketSeed, PlanSynthesis


def _empty_atoms() -> SuggestedAtoms:
    return SuggestedAtoms(
        intent="probe",
        pills=BoundIntent(intent="probe"),
        suggested_pills=[],
        step_types=[],
        parameters=[],
    )


class _FakeSubsystems:
    """Stub subsystems for the runner. Returns a no-op pg conn so the
    runner's gateway-dispatch branch fails fast and falls back to the
    direct ``compose_plan_via_llm`` call (which the tests stub via
    monkeypatch)."""

    def get_pg_conn(self):
        return object()


def _empty_skeleton() -> SkeletalPlan:
    return SkeletalPlan(
        parameters=[], packets=[], notes=[],
        stage_contracts={}, gate_contracts={},
    )


def _empty_validation() -> ValidationReport:
    return ValidationReport(
        findings=[], every_required_filled=True,
        no_forbidden_placeholders=True, no_workspace_root=True,
        no_dropped_floors=True, every_required_gate_scaffolded=True,
    )


def _make_result(*, ok: bool, packets: int = 0, usage_calls: int = 1) -> ComposeViaLLMResult:
    seeds = [
        PacketSeed(label=f"seed_{i}", stage="build", description="x", depends_on=[])
        for i in range(packets)
    ]
    synthesis = PlanSynthesis(
        packet_seeds=seeds,
        raw_response="{}", provider_slug="stub", model_slug="stub-v1",
        usage={"prompt_tokens": 100, "completion_tokens": 50,
               "total_tokens": 150, "cached_tokens": 0},
    )
    return ComposeViaLLMResult(
        ok=ok, intent="probe",
        atoms=_empty_atoms(), skeleton=_empty_skeleton(),
        synthesis=synthesis,
        authored=AuthoredPlan(packets=[], errors=[]),
        validation=_empty_validation(),
        plan_packets=[{"label": f"p{i}"} for i in range(packets)],
    )


def test_normalize_config_flat_legacy_shape():
    """Legacy flat-dict config — no base_task_type, treated as raw overrides."""
    out = _normalize_config(
        {"model_slug": "x/y", "temperature": 0.7, "max_tokens": 4096},
        index=0,
    )
    assert out["base_task_type"] is None
    assert out["overrides"]["model_slug"] == "x/y"
    assert out["overrides"]["temperature"] == 0.7
    assert out["overrides"]["max_tokens"] == 4096


def test_normalize_config_base_plus_overrides_shape():
    """Preferred shape: inherit from a task_type, layer per-leg deltas."""
    out = _normalize_config(
        {"base_task_type": "plan_synthesis", "overrides": {"temperature": 0.7}},
        index=0,
    )
    assert out["base_task_type"] == "plan_synthesis"
    assert out["overrides"] == {"temperature": 0.7}


def test_normalize_config_base_only_no_overrides():
    """Base alone (no overrides key) is the 'baseline' leg."""
    out = _normalize_config({"base_task_type": "plan_fork_author"}, index=0)
    assert out["base_task_type"] == "plan_fork_author"
    assert out["overrides"] == {}


def test_normalize_config_coerces_numeric_strings():
    out = _normalize_config(
        {"temperature": "0.5", "max_tokens": "2048"}, index=0,
    )
    assert out["overrides"]["temperature"] == 0.5
    assert isinstance(out["overrides"]["temperature"], float)
    assert out["overrides"]["max_tokens"] == 2048
    assert isinstance(out["overrides"]["max_tokens"], int)


def test_normalize_config_rejects_non_dict():
    with pytest.raises(ValueError, match=r"config\[3\] must be a dict"):
        _normalize_config("not-a-dict", index=3)


def test_normalize_config_rejects_zero_or_negative_max_tokens():
    with pytest.raises(ValueError, match="max_tokens must be positive"):
        _normalize_config({"max_tokens": 0}, index=0)
    with pytest.raises(ValueError, match="max_tokens must be positive"):
        _normalize_config(
            {"base_task_type": "plan_synthesis", "overrides": {"max_tokens": -100}},
            index=1,
        )


def test_normalize_config_rejects_non_string_provider_slug():
    with pytest.raises(ValueError, match="provider_slug must be a string"):
        _normalize_config({"provider_slug": 42}, index=0)


def test_normalize_config_rejects_unknown_override_keys():
    """Override-side typos must surface loudly, not silently disappear."""
    with pytest.raises(ValueError, match="unknown keys"):
        _normalize_config(
            {"base_task_type": "plan_synthesis", "overrides": {"temprature": 0.7}},
            index=0,
        )


def test_rank_puts_successful_runs_first_by_walltime():
    runs = [
        # config 0 — ok=True compose, slow (5s)
        ComposeExperimentRun(
            config_index=0, config={"a": 1}, ok=True, wall_seconds=5.0,
            result=_make_result(ok=True, packets=2),
        ),
        # config 1 — ok=False compose (failed), fast (1s)
        ComposeExperimentRun(
            config_index=1, config={"a": 2}, ok=True, wall_seconds=1.0,
            result=_make_result(ok=False),
        ),
        # config 2 — ok=True compose, fast (2s)  ← winner
        ComposeExperimentRun(
            config_index=2, config={"a": 3}, ok=True, wall_seconds=2.0,
            result=_make_result(ok=True, packets=3),
        ),
        # config 3 — exception during run
        ComposeExperimentRun(
            config_index=3, config={"a": 4}, ok=False, wall_seconds=0.5,
            result=None, error="boom",
        ),
    ]
    ranked = _rank(runs)
    # Successes by wall-time: 2 (2s), 0 (5s); then failures by wall-time: 3 (0.5s), 1 (1s)
    assert ranked == [2, 0, 3, 1]


def test_run_compose_experiment_fans_out_in_parallel(monkeypatch):
    """Three configs each with a 0.3s sleep should complete in <0.6s wall
    if the runner is actually parallel; sequential would take ~0.9s."""

    call_log: list[dict] = []

    def stub_compose(intent, *, conn, plan_name=None, why=None,
                    concurrency=5, hydrate_env=None, llm_overrides=None):
        call_log.append({
            "intent": intent, "overrides": dict(llm_overrides or {}),
        })
        time.sleep(0.3)
        # Vary outcome per config so ranking is exercised
        ok = (llm_overrides or {}).get("model_slug") != "broken/v1"
        packets = 2 if ok else 0
        return _make_result(ok=ok, packets=packets)

    monkeypatch.setattr(compose_experiment, "compose_plan_via_llm", stub_compose)

    started = time.monotonic()
    report = run_compose_experiment(
        "probe-intent",
        configs=[
            {"model_slug": "good/a", "temperature": 0.2},
            {"model_slug": "broken/v1", "temperature": 0.0},
            {"model_slug": "good/b", "temperature": 0.7, "max_tokens": 4096},
        ],
        subsystems=_FakeSubsystems(),
        max_workers=4,
    )
    wall = time.monotonic() - started

    assert wall < 0.7, f"runner did not parallelize (wall={wall:.2f}s)"
    assert len(report.runs) == 3
    assert len(call_log) == 3
    # Ranking: two successful runs first (any order), failed run last.
    assert report.ranked_indices[-1] == 1  # broken/v1 is the failure
    assert set(report.ranked_indices[:2]) == {0, 2}
    # Winner is one of the two good configs (whichever finished first).
    winner = report.winner()
    assert winner is not None
    assert winner.config_index in {0, 2}


def test_run_compose_experiment_captures_exceptions_per_run(monkeypatch):
    """A handler that raises in one worker should NOT poison other runs."""

    def stub_compose(intent, *, conn, plan_name=None, why=None,
                    concurrency=5, hydrate_env=None, llm_overrides=None):
        slug = (llm_overrides or {}).get("model_slug")
        if slug == "raise/v1":
            raise RuntimeError("synthetic kaboom")
        return _make_result(ok=True, packets=1)

    monkeypatch.setattr(compose_experiment, "compose_plan_via_llm", stub_compose)

    report = run_compose_experiment(
        "probe-intent",
        configs=[
            {"model_slug": "raise/v1"},
            {"model_slug": "good/v1"},
        ],
        subsystems=_FakeSubsystems(),
        max_workers=2,
    )
    # Failed run is captured, ok=False, error string set.
    failed = next(r for r in report.runs if r.config_index == 0)
    assert failed.ok is False
    assert "synthetic kaboom" in (failed.error or "")
    # Other run still succeeded.
    succeeded = next(r for r in report.runs if r.config_index == 1)
    assert succeeded.ok is True
    assert succeeded.result is not None and succeeded.result.ok is True


def test_run_compose_experiment_rejects_empty_configs():
    with pytest.raises(ValueError, match="non-empty list"):
        run_compose_experiment(
            "intent", configs=[], subsystems=_FakeSubsystems(),
        )


def test_run_compose_experiment_rejects_oversize_matrix():
    with pytest.raises(ValueError, match="hard cap"):
        run_compose_experiment(
            "intent",
            configs=[{} for _ in range(200)],
            subsystems=_FakeSubsystems(),
        )


def test_report_to_dict_shape(monkeypatch):
    """The serialized report carries enough to render a comparison
    table without needing the in-memory dataclass."""

    def stub_compose(intent, *, conn, plan_name=None, why=None,
                    concurrency=5, hydrate_env=None, llm_overrides=None):
        return _make_result(ok=True, packets=2)

    monkeypatch.setattr(compose_experiment, "compose_plan_via_llm", stub_compose)

    report = run_compose_experiment(
        "intent",
        configs=[{"model_slug": "x/y", "temperature": 0.5}],
        subsystems=_FakeSubsystems(),
    )
    d = report.to_dict()
    assert d["intent"] == "intent"
    assert isinstance(d["runs"], list) and len(d["runs"]) == 1
    assert isinstance(d["summary_table"], list) and len(d["summary_table"]) == 1
    assert isinstance(d["ranked_summary"], list) and len(d["ranked_summary"]) == 1
    row = d["summary_table"][0]
    # Matrix rows carry the comprehensive trace shape — top-level
    # convenience keys + grouped detail blocks.
    top_level = ("config", "ok", "wall_seconds", "compose_ok", "reason_code",
                 "packet_count", "totals", "synthesis", "fork_author",
                 "per_packet", "per_packet_failures", "validation", "quality",
                 "cost_usd")
    for key in top_level:
        assert key in row, f"missing {key} in summary row"
    # Token rollup is in totals
    assert "completion_tokens" in row["totals"]
    assert "prompt_tokens" in row["totals"]
    assert "cached_tokens" in row["totals"]
    assert "calls" in row["totals"]
    # Validation has structured detail, not just a count
    assert "passed" in row["validation"]
    assert "findings_by_severity" in row["validation"]
    assert "findings" in row["validation"]
    # Quality has structural signals
    assert "distinct_stages_used" in row["quality"]
    assert "depends_on_chain_max_depth" in row["quality"]
