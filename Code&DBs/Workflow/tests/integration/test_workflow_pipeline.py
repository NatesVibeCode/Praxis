"""Integration tests for the workflow pipeline facade.

Exercises the full pre-workflow -> execute -> post-workflow lifecycle
using the real safety modules (GovernanceFilter, LoopDetector, etc.)
with no mocked internals — only the actual execution call is stubbed.
"""

from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# Direct file imports to avoid runtime/__init__.py
# ---------------------------------------------------------------------------

_RUNTIME_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "runtime",
)


def _load(name: str, filename: str):
    key = f"runtime.{name}"
    if key in sys.modules:
        return sys.modules[key]
    spec = importlib.util.spec_from_file_location(
        key, os.path.join(_RUNTIME_DIR, filename)
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[key] = mod
    spec.loader.exec_module(mod)
    return mod


_pipeline_mod = _load("workflow_pipeline", "workflow_pipeline.py")
_governance_mod = _load("governance", "governance.py")
_conflict_mod = _load("conflict_resolver", "conflict_resolver.py")
_loop_mod = _load("loop_detector", "loop_detector.py")
_auto_retry_mod = _load("auto_retry", "auto_retry.py")
_retry_ctx_mod = _load("retry_context", "retry_context.py")
_posture_mod = _load("posture", "posture.py")

WorkflowPipeline = _pipeline_mod.WorkflowPipeline
PipelineGate = _pipeline_mod.PipelineGate
PostWorkflowAction = _pipeline_mod.PostWorkflowAction

GovernanceFilter = _governance_mod.GovernanceFilter
ConflictResolver = _conflict_mod.ConflictResolver
LoopDetector = _loop_mod.LoopDetector
FailureRecord = _loop_mod.FailureRecord
AutoRetryManager = _auto_retry_mod.AutoRetryManager
RetryPolicy = _auto_retry_mod.RetryPolicy
RetryContextBuilder = _retry_ctx_mod.RetryContextBuilder
PostureEnforcer = _posture_mod.PostureEnforcer
Posture = _posture_mod.Posture


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pipeline_operate() -> WorkflowPipeline:
    """Pipeline in OPERATE posture (workflow execution allowed)."""
    return WorkflowPipeline(
        governance=GovernanceFilter(),
        conflict_resolver=ConflictResolver(),
        loop_detector=LoopDetector(max_consecutive_failures=3),
        auto_retry=AutoRetryManager(
            policy=RetryPolicy(max_retries=3, escalate_after=2, backoff_seconds=(1, 5, 15)),
        ),
        retry_context_builder=RetryContextBuilder(),
        posture_enforcer=PostureEnforcer(Posture.OPERATE),
    )


@pytest.fixture()
def pipeline_observe() -> WorkflowPipeline:
    """Pipeline in OBSERVE posture (workflow execution blocked)."""
    return WorkflowPipeline(
        governance=GovernanceFilter(),
        conflict_resolver=ConflictResolver(),
        loop_detector=LoopDetector(max_consecutive_failures=3),
        auto_retry=AutoRetryManager(),
        retry_context_builder=RetryContextBuilder(),
        posture_enforcer=PostureEnforcer(Posture.OBSERVE),
    )


# ---------------------------------------------------------------------------
# Pre-workflow tests
# ---------------------------------------------------------------------------


class TestPreDispatchClean:
    """A clean spec with no secrets and no prior failures passes all gates."""

    def test_clean_workflow_passes(self, pipeline_operate: WorkflowPipeline):
        gate = pipeline_operate.pre_dispatch({
            "prompt": "Fix the flaky test in test_utils.py",
            "job_label": "j-clean-001",
        })
        assert gate.passed is True
        assert gate.blocked_by == ()
        assert gate.governance_findings == ()
        assert gate.loop_verdict is not None
        assert gate.loop_verdict["action"] == "proceed"


class TestGovernanceBlocks:
    """Governance scanner blocks prompts containing API keys."""

    def test_api_key_blocked(self, pipeline_operate: WorkflowPipeline):
        gate = pipeline_operate.pre_dispatch({
            "prompt": "Use this key: sk-abc123XYZabc123XYZabc123XYZ to authenticate",
            "job_label": "j-secret-001",
        })
        assert gate.passed is False
        assert any("governance" in b for b in gate.blocked_by)
        assert len(gate.governance_findings) > 0


class TestLoopDetectorBlocks:
    """Loop detector blocks after consecutive failures."""

    def test_consecutive_failures_block(self, pipeline_operate: WorkflowPipeline):
        # Simulate 3 consecutive failures for the same job
        for i in range(3):
            pipeline_operate._loop_detector.record_failure(
                FailureRecord(
                    job_label="j-loop-001",
                    failure_code="timeout",
                    timestamp=_utc_now(),
                    token_cost=1.0,
                    attempt_number=i + 1,
                )
            )

        gate = pipeline_operate.pre_dispatch({
            "prompt": "Try again",
            "job_label": "j-loop-001",
        })
        assert gate.passed is False
        assert any("loop_detector" in b for b in gate.blocked_by)


class TestPostureBlocks:
    """OBSERVE posture blocks workflow execution (a MUTATE operation)."""

    def test_observe_blocks_workflow(self, pipeline_observe: WorkflowPipeline):
        gate = pipeline_observe.pre_dispatch({
            "prompt": "Do something",
            "job_label": "j-observe-001",
        })
        assert gate.passed is False
        assert any("posture" in b for b in gate.blocked_by)


# ---------------------------------------------------------------------------
# Post-workflow tests
# ---------------------------------------------------------------------------


class TestPostDispatchSuccess:
    """Successful workflow execution records in loop detector and returns complete."""

    def test_success_returns_complete(self, pipeline_operate: WorkflowPipeline):
        action = pipeline_operate.post_workflow("j-ok-001", succeeded=True)
        assert action.action == "complete"
        assert action.retry_context is None
        assert action.wait_seconds == 0

    def test_success_resets_loop_counter(self, pipeline_operate: WorkflowPipeline):
        # Record some failures first
        pipeline_operate._loop_detector.record_failure(
            FailureRecord(
                job_label="j-reset-001",
                failure_code="timeout",
                timestamp=_utc_now(),
                token_cost=0.5,
                attempt_number=1,
            )
        )
        # Success should reset consecutive counter
        pipeline_operate.post_workflow("j-reset-001", succeeded=True)
        verdict = pipeline_operate._loop_detector.check("j-reset-001")
        assert verdict.consecutive_failures == 0


class TestPostDispatchRetry:
    """Failed transient workflow execution triggers retry with context."""

    def test_transient_failure_retries(self, pipeline_operate: WorkflowPipeline):
        action = pipeline_operate.post_workflow(
            "j-retry-001",
            succeeded=False,
            failure_code="timeout",
            stderr="Error: request timed out after 120s",
            cost=0.5,
        )
        assert action.action == "retry"
        assert action.retry_context is not None
        assert "PREVIOUS ATTEMPT FAILED" in action.retry_context
        assert action.wait_seconds > 0


class TestPostDispatchHalt:
    """Non-retryable failure returns halt."""

    def test_non_retryable_halts(self, pipeline_operate: WorkflowPipeline):
        action = pipeline_operate.post_workflow(
            "j-halt-001",
            succeeded=False,
            failure_code="scope_violation",
            stderr="Error: scope violation detected in sandbox",
            cost=0.1,
        )
        assert action.action == "halt"
        assert action.retry_context is None
        assert any("non-retryable" in r for r in action.reasons)


# ---------------------------------------------------------------------------
# Conflict analysis tests
# ---------------------------------------------------------------------------


class TestConflictAnalysis:
    """Conflict resolver identifies parallel writers."""

    def test_parallel_writers_detected(self, pipeline_operate: WorkflowPipeline):
        jobs = [
            {
                "job_label": "j-write-A",
                "write_paths": ["src/utils.py", "src/config.py"],
                "read_paths": [],
            },
            {
                "job_label": "j-write-B",
                "write_paths": ["src/utils.py"],
                "read_paths": [],
            },
            {
                "job_label": "j-write-C",
                "write_paths": ["src/other.py"],
                "read_paths": [],
            },
        ]
        analysis = pipeline_operate.check_conflicts(jobs)

        # A and B conflict on src/utils.py
        assert len(analysis.conflicts) >= 1
        conflict_pairs = {(c.job_a, c.job_b) for c in analysis.conflicts}
        assert any(
            {"j-write-A", "j-write-B"} == {a, b} for a, b in conflict_pairs
        )

        # C has no conflicts
        assert "j-write-C" in analysis.parallel_safe_jobs

        # Serialization group should contain A and B
        assert len(analysis.serialization_groups) >= 1
        group_labels = set()
        for g in analysis.serialization_groups:
            group_labels.update(g.job_labels)
        assert "j-write-A" in group_labels
        assert "j-write-B" in group_labels


# ---------------------------------------------------------------------------
# Full lifecycle test
# ---------------------------------------------------------------------------


class TestFullLifecycle:
    """End-to-end: pre_dispatch -> mock execute -> post_workflow."""

    def test_clean_lifecycle(self, pipeline_operate: WorkflowPipeline):
        spec = {
            "prompt": "Refactor the logger module",
            "job_label": "j-lifecycle-001",
        }

        # 1. Pre-workflow gate
        gate = pipeline_operate.pre_dispatch(spec)
        assert gate.passed is True

        # 2. Simulate execution (mock)
        executed = True  # pretend the workflow ran successfully

        # 3. Post-dispatch
        action = pipeline_operate.post_workflow(
            "j-lifecycle-001", succeeded=executed,
        )
        assert action.action == "complete"

    def test_failure_retry_lifecycle(self, pipeline_operate: WorkflowPipeline):
        spec = {
            "prompt": "Run the test suite",
            "job_label": "j-lifecycle-002",
        }

        # 1. Pre-dispatch
        gate = pipeline_operate.pre_dispatch(spec)
        assert gate.passed is True

        # 2. Simulate failure
        action = pipeline_operate.post_workflow(
            "j-lifecycle-002",
            succeeded=False,
            failure_code="timeout",
            stderr="Timed out waiting for response",
            cost=0.3,
        )
        assert action.action == "retry"
        assert action.retry_context is not None
        assert action.wait_seconds > 0

    def test_failure_then_block_lifecycle(self, pipeline_operate: WorkflowPipeline):
        """After enough consecutive failures, pre_dispatch blocks the next attempt."""
        job_label = "j-lifecycle-003"
        spec = {"prompt": "Run builds", "job_label": job_label}

        # First attempt passes pre-dispatch
        gate = pipeline_operate.pre_dispatch(spec)
        assert gate.passed is True

        # Simulate 3 failures via post_workflow (each records in loop detector)
        for _ in range(3):
            pipeline_operate.post_workflow(
                job_label,
                succeeded=False,
                failure_code="timeout",
                stderr="Timed out",
                cost=1.0,
            )

        # Now pre_dispatch should block due to loop detection
        gate2 = pipeline_operate.pre_dispatch(spec)
        assert gate2.passed is False
        assert any("loop_detector" in b for b in gate2.blocked_by)
