"""Tests for runtime.wave_orchestrator."""

import importlib.util
import os
import sys

import pytest

# Import the module directly to avoid runtime/__init__.py (needs Python 3.10+).
_mod_path = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, "runtime", "wave_orchestrator.py"
)
_spec = importlib.util.spec_from_file_location("wave_orchestrator", os.path.abspath(_mod_path))
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

WaveOrchestrator = _mod.WaveOrchestrator
WaveSnapshot = _mod.WaveSnapshot
GateVerdict = _mod.GateVerdict
JobState = _mod.JobState
WaveState = _mod.WaveState
WaveStatus = _mod.WaveStatus


class TestAddWaveAndObserve:
    def test_add_wave_appears_in_observe(self):
        orch = WaveOrchestrator("orch-1")
        orch.add_wave("w1", [{"label": "j1"}, {"label": "j2"}])
        state = orch.observe()
        assert isinstance(state, WaveSnapshot)
        assert len(state.waves) == 1
        assert state.waves[0].wave_id == "w1"
        assert len(state.waves[0].jobs) == 2

    def test_multiple_waves_ordered(self):
        orch = WaveOrchestrator("orch-2")
        orch.add_wave("w1", [{"label": "j1"}])
        orch.add_wave("w2", [{"label": "j2"}], depends_on_wave="w1")
        state = orch.observe()
        assert [w.wave_id for w in state.waves] == ["w1", "w2"]


class TestStartWave:
    def test_start_wave_no_dependencies(self):
        orch = WaveOrchestrator("orch-3")
        orch.add_wave("w1", [{"label": "j1"}])
        ws = orch.start_wave("w1")
        assert ws.status == WaveStatus.RUNNING
        assert ws.started_at is not None

    def test_start_wave_blocked_when_predecessor_incomplete(self):
        orch = WaveOrchestrator("orch-4")
        orch.add_wave("w1", [{"label": "j1"}])
        orch.add_wave("w2", [{"label": "j2"}], depends_on_wave="w1")
        orch.start_wave("w1")
        with pytest.raises(RuntimeError):
            orch.start_wave("w2")


class TestRecordJobResult:
    def test_record_job_result_updates_state(self):
        orch = WaveOrchestrator("orch-5")
        orch.add_wave("w1", [{"label": "j1"}, {"label": "j2"}])
        orch.start_wave("w1")
        orch.record_job_result("w1", "j1", succeeded=True)
        ws = orch.wave_state("w1")
        j1 = next(j for j in ws.jobs if j.job_label == "j1")
        assert j1.status == "succeeded"
        assert j1.completed_at is not None

    def test_wave_status_succeeded_when_all_pass(self):
        orch = WaveOrchestrator("orch-6")
        orch.add_wave("w1", [{"label": "j1"}, {"label": "j2"}])
        orch.start_wave("w1")
        orch.record_job_result("w1", "j1", succeeded=True)
        orch.record_job_result("w1", "j2", succeeded=True)
        assert orch.wave_state("w1").status == WaveStatus.SUCCEEDED

    def test_wave_status_failed_when_any_fail(self):
        orch = WaveOrchestrator("orch-7")
        orch.add_wave("w1", [{"label": "j1"}, {"label": "j2"}])
        orch.start_wave("w1")
        orch.record_job_result("w1", "j1", succeeded=True)
        orch.record_job_result("w1", "j2", succeeded=False)
        assert orch.wave_state("w1").status == WaveStatus.FAILED


class TestNextRunnableJobs:
    def test_respects_intra_wave_dependencies(self):
        orch = WaveOrchestrator("orch-8")
        orch.add_wave("w1", [
            {"label": "j1"},
            {"label": "j2", "depends_on": ["j1"]},
            {"label": "j3", "depends_on": ["j1"]},
        ])
        orch.start_wave("w1")
        assert orch.next_runnable_jobs("w1") == ["j1"]
        orch.record_job_result("w1", "j1", succeeded=True)
        assert sorted(orch.next_runnable_jobs("w1")) == ["j2", "j3"]

    def test_no_runnable_when_dep_failed(self):
        orch = WaveOrchestrator("orch-9")
        orch.add_wave("w1", [
            {"label": "j1"},
            {"label": "j2", "depends_on": ["j1"]},
        ])
        orch.start_wave("w1")
        orch.record_job_result("w1", "j1", succeeded=False)
        assert orch.next_runnable_jobs("w1") == []

    def test_resolve_default_wave_id_prefers_current_wave(self):
        orch = WaveOrchestrator("orch-9b")
        orch.add_wave("w1", [{"label": "j1"}])
        orch.start_wave("w1")

        assert orch.resolve_default_wave_id(action="next") == "w1"

    def test_resolve_default_wave_id_uses_only_wave_when_not_running(self):
        orch = WaveOrchestrator("orch-9c")
        orch.add_wave("w1", [{"label": "j1"}])

        assert orch.resolve_default_wave_id(action="start") == "w1"

    def test_resolve_default_wave_id_refuses_ambiguous_choice(self):
        orch = WaveOrchestrator("orch-9d")
        orch.add_wave("w1", [{"label": "j1"}])
        orch.add_wave("w2", [{"label": "j2"}])

        with pytest.raises(KeyError):
            orch.resolve_default_wave_id(action="next")


class TestGateVerdict:
    def test_gate_verdict_controls_wave_progression(self):
        orch = WaveOrchestrator("orch-10")
        orch.add_wave("w1", [{"label": "j1"}])
        orch.add_wave("w2", [{"label": "j2"}], depends_on_wave="w1")
        orch.start_wave("w1")
        orch.record_job_result("w1", "j1", succeeded=True)
        assert not orch.can_start_wave("w2")
        orch.record_gate_verdict("w1", passed=False, reason="quality too low")
        assert not orch.can_start_wave("w2")
        orch.record_gate_verdict("w1", passed=True, reason="all clear")
        assert orch.can_start_wave("w2")


class TestFullLifecycle:
    def test_multi_wave_lifecycle(self):
        orch = WaveOrchestrator("orch-11")
        orch.add_wave("w1", [{"label": "build"}, {"label": "test", "depends_on": ["build"]}])
        orch.add_wave("w2", [{"label": "deploy"}], depends_on_wave="w1")

        orch.start_wave("w1")
        assert orch.next_runnable_jobs("w1") == ["build"]
        orch.record_job_result("w1", "build", succeeded=True)
        assert orch.next_runnable_jobs("w1") == ["test"]
        orch.record_job_result("w1", "test", succeeded=True)
        assert orch.is_wave_complete("w1")

        orch.record_gate_verdict("w1", passed=True, reason="tests passed", evidence={"pass_rate": 1.0})
        assert orch.can_start_wave("w2")

        ws2 = orch.start_wave("w2")
        assert ws2.status == WaveStatus.RUNNING
        orch.record_job_result("w2", "deploy", succeeded=True)
        assert orch.is_wave_complete("w2")

        state = orch.observe()
        assert len(state.waves) == 2
        assert state.waves[0].status == WaveStatus.SUCCEEDED
        assert state.waves[1].status == WaveStatus.SUCCEEDED


class TestIsWaveComplete:
    def test_mixed_success_and_failure(self):
        orch = WaveOrchestrator("orch-12")
        orch.add_wave("w1", [{"label": "j1"}, {"label": "j2"}, {"label": "j3"}])
        orch.start_wave("w1")
        orch.record_job_result("w1", "j1", succeeded=True)
        orch.record_job_result("w1", "j2", succeeded=False)
        assert not orch.is_wave_complete("w1")
        orch.record_job_result("w1", "j3", succeeded=True)
        assert orch.is_wave_complete("w1")
        assert orch.wave_state("w1").status == WaveStatus.FAILED
