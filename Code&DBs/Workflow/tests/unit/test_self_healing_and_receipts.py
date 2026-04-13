"""Tests for self_healing."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

# Direct file imports to avoid runtime/__init__.py pulling in incompatible modules
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_RT = _ROOT / "runtime"

def _load(mod_name: str, file_name: str):
    spec = importlib.util.spec_from_file_location(mod_name, f"{_RT}/{file_name}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod

_sh = _load("self_healing", "self_healing.py")
RecoveryAction = _sh.RecoveryAction
DiagnosticResult = _sh.DiagnosticResult
DiagnosticModule = _sh.DiagnosticModule
EarlyTerminationDiagnostic = _sh.EarlyTerminationDiagnostic
ScopeRecoveryDiagnostic = _sh.ScopeRecoveryDiagnostic
DependencyDiagnostic = _sh.DependencyDiagnostic
HealingRecommendation = _sh.HealingRecommendation
SelfHealingOrchestrator = _sh.SelfHealingOrchestrator


# ===================================================================
# Self-Healing Orchestrator Tests
# ===================================================================


class TestRecoveryAction:
    def test_all_actions_exist(self):
        expected = {"RETRY_SAME", "RETRY_ESCALATED", "SKIP", "HALT", "FIX_AND_RETRY"}
        assert {a.name for a in RecoveryAction} == expected


class TestDiagnosticResult:
    def test_frozen(self):
        dr = DiagnosticResult("mod", RecoveryAction.SKIP, 0.8, "reason")
        with pytest.raises(AttributeError):
            dr.reason = "new"

    def test_confidence_clamped(self):
        dr = DiagnosticResult("mod", RecoveryAction.SKIP, 1.5, "over")
        assert dr.confidence <= 1.0
        dr2 = DiagnosticResult("mod", RecoveryAction.SKIP, -0.5, "under")
        assert dr2.confidence >= 0.0

    def test_context_patch_optional(self):
        dr = DiagnosticResult("mod", RecoveryAction.SKIP, 0.5, "r")
        assert dr.context_patch is None


class TestEarlyTerminationDiagnostic:
    def test_retry_below_threshold(self):
        diag = EarlyTerminationDiagnostic()
        r1 = diag.diagnose("job-1", "E001", "boom")
        assert r1.recommendation == RecoveryAction.RETRY_SAME

    def test_skip_at_threshold(self):
        diag = EarlyTerminationDiagnostic()
        for _ in range(2):
            diag.diagnose("job-1", "E001", "boom")
        r3 = diag.diagnose("job-1", "E001", "boom")
        assert r3.recommendation == RecoveryAction.SKIP
        assert r3.confidence >= 0.7

    def test_different_jobs_independent(self):
        diag = EarlyTerminationDiagnostic()
        for _ in range(3):
            diag.diagnose("job-A", "E001", "x")
        r = diag.diagnose("job-B", "E001", "x")
        assert r.recommendation == RecoveryAction.RETRY_SAME


class TestScopeRecoveryDiagnostic:
    def test_detects_scope(self):
        diag = ScopeRecoveryDiagnostic()
        r = diag.diagnose("j", "E", "FileNotFoundError: No such file or directory: /tmp/scope/missing")
        assert r.recommendation == RecoveryAction.FIX_AND_RETRY
        assert r.context_patch is not None

    def test_no_scope_low_confidence(self):
        diag = ScopeRecoveryDiagnostic()
        r = diag.diagnose("j", "E", "timeout after 30s")
        assert r.confidence < 0.2


class TestDependencyDiagnostic:
    def test_hard_dependency_halts(self):
        diag = DependencyDiagnostic()
        r = diag.diagnose("j", "E", "ModuleNotFoundError: No module named 'foobar'")
        assert r.recommendation == RecoveryAction.HALT
        assert r.confidence >= 0.8

    def test_transient_import_retries(self):
        diag = DependencyDiagnostic()
        r = diag.diagnose("j", "E", "ImportError while loading module")
        assert r.recommendation == RecoveryAction.RETRY_SAME
        assert r.confidence > 0.4

    def test_no_import_low_confidence(self):
        diag = DependencyDiagnostic()
        r = diag.diagnose("j", "E", "segfault at 0x0")
        assert r.confidence <= 0.2


class TestSelfHealingOrchestrator:
    def test_default_diagnostics(self):
        orch = SelfHealingOrchestrator()
        rec = orch.diagnose("j", "E", "something happened")
        assert isinstance(rec, HealingRecommendation)
        assert rec.diagnostics_run == 3

    def test_picks_highest_confidence(self):
        orch = SelfHealingOrchestrator()
        rec = orch.diagnose("j", "E", "No module named 'xyz' import failure")
        # DependencyDiagnostic should win with HALT at ~0.85
        assert rec.action == RecoveryAction.HALT
        assert rec.confidence >= 0.8

    def test_empty_diagnostics(self):
        orch = SelfHealingOrchestrator(diagnostics=[])
        rec = orch.diagnose("j", "E", "x")
        assert rec.action == RecoveryAction.RETRY_SAME
        assert rec.diagnostics_run == 0

    def test_context_patches_collected(self):
        orch = SelfHealingOrchestrator()
        rec = orch.diagnose("j", "E", "scope path not found")
        assert len(rec.context_patches) >= 1
