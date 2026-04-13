"""Workflow pipeline facade — composes safety modules into a pre/post execution flow.

Wraps the existing dispatch module WITHOUT modifying it. Orchestrates:
  pre-dispatch  -> posture + governance + loop detection gates
  conflicts     -> write-scope conflict analysis
  post-dispatch -> failure classification, retry decisions, context enrichment

Usage:
    >>> pipeline = WorkflowPipeline(
    ...     governance=GovernanceFilter(),
    ...     conflict_resolver=ConflictResolver(),
    ...     loop_detector=LoopDetector(),
    ...     auto_retry=AutoRetryManager(),
    ...     retry_context_builder=RetryContextBuilder(),
    ...     posture_enforcer=PostureEnforcer(Posture.OPERATE),
    ... )
    >>> gate = pipeline.pre_dispatch({"prompt": "fix the tests", "job_label": "j-001"})
    >>> if gate.passed:
    ...     # run actual workflow step ...
    ...     action = pipeline.post_workflow("j-001", succeeded=True)
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# Direct sibling imports (avoids runtime/__init__.py issues)
# ---------------------------------------------------------------------------

_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_sibling(module_name: str, filename: str):
    """Import a sibling module by file path, caching in sys.modules."""
    key = f"runtime.{module_name}"
    if key in sys.modules:
        return sys.modules[key]
    spec = importlib.util.spec_from_file_location(key, os.path.join(_DIR, filename))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[key] = mod
    spec.loader.exec_module(mod)
    return mod


_governance_mod = _load_sibling("governance", "governance.py")
_conflict_mod = _load_sibling("conflict_resolver", "conflict_resolver.py")
_loop_mod = _load_sibling("loop_detector", "loop_detector.py")
_auto_retry_mod = _load_sibling("auto_retry", "auto_retry.py")
_retry_ctx_mod = _load_sibling("retry_context", "retry_context.py")
_posture_mod = _load_sibling("posture", "posture.py")

GovernanceFilter = _governance_mod.GovernanceFilter
GovernanceScanResult = _governance_mod.GovernanceScanResult

ConflictResolver = _conflict_mod.ConflictResolver
ConflictAnalysis = _conflict_mod.ConflictAnalysis
JobWriteScope = _conflict_mod.JobWriteScope

LoopDetector = _loop_mod.LoopDetector
LoopVerdict = _loop_mod.LoopVerdict
FailureRecord = _loop_mod.FailureRecord

AutoRetryManager = _auto_retry_mod.AutoRetryManager
RetryClassification = _auto_retry_mod.RetryClassification

RetryContextBuilder = _retry_ctx_mod.RetryContextBuilder

PostureEnforcer = _posture_mod.PostureEnforcer
Posture = _posture_mod.Posture
ToolCall = _posture_mod.ToolCall
CallClassification = _posture_mod.CallClassification


def _utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Pipeline data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PipelineGate:
    """Result of pre-workflow safety checks."""

    passed: bool
    blocked_by: tuple[str, ...]
    governance_findings: tuple
    loop_verdict: dict | None


@dataclass(frozen=True)
class PostWorkflowAction:
    """Instruction returned after a workflow step completes or fails."""

    action: str  # 'complete' | 'retry' | 'halt' | 'escalate'
    retry_context: str | None
    wait_seconds: int
    reasons: tuple[str, ...]


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

class WorkflowPipeline:
    """Facade that composes safety modules around the existing workflow path.

    Parameters
    ----------
    governance : GovernanceFilter
    conflict_resolver : ConflictResolver
    loop_detector : LoopDetector
    auto_retry : AutoRetryManager
    retry_context_builder : RetryContextBuilder
    posture_enforcer : PostureEnforcer
    workflow_fn : optional callable wrapping the real workflow run
    """

    def __init__(
        self,
        governance: GovernanceFilter,
        conflict_resolver: ConflictResolver,
        loop_detector: LoopDetector,
        auto_retry: AutoRetryManager,
        retry_context_builder: RetryContextBuilder,
        posture_enforcer: PostureEnforcer,
        workflow_fn: Callable | None = None,
    ) -> None:
        self._governance = governance
        self._conflict_resolver = conflict_resolver
        self._loop_detector = loop_detector
        self._auto_retry = auto_retry
        self._retry_ctx = retry_context_builder
        self._posture = posture_enforcer
        self._workflow_fn = workflow_fn

    # ------------------------------------------------------------------
    # Pre-workflow gate
    # ------------------------------------------------------------------

    def pre_dispatch(self, spec: dict) -> PipelineGate:
        """Run ordered safety checks before executing a job.

        Order:
          1. Posture check (is execution allowed?)
          2. Governance scan (secrets in prompt?)
          3. Loop detection (stuck in retry loop?)

        Returns a PipelineGate indicating pass/fail with reasons.
        """
        blocked_by: list[str] = []
        governance_findings: tuple = ()
        loop_verdict_dict: dict | None = None

        # --- 0. Provider preflight --------------------------------------
        agent_slug = spec.get("agent_slug", "")
        if agent_slug and not agent_slug.startswith(("human", "integration/")):
            try:
                from runtime.agent_spawner import AgentSpawner
                readiness = AgentSpawner().preflight(agent_slug)
                if not readiness.ready:
                    blocked_by.append(
                        f"provider_preflight:{agent_slug} is not ready — {readiness.reason}"
                    )
            except Exception:
                pass  # fail-open: preflight errors don't block execution

        # --- 1. Posture check -------------------------------------------
        # Workflow execution is a MUTATE operation; check posture allows it.
        workflow_call = ToolCall(
            tool_name="workflow_job",
            arguments=spec,
            timestamp=_utc_now(),
        )
        posture_verdict = self._posture.check(workflow_call)
        if not posture_verdict.allowed:
            blocked_by.append(f"posture:{posture_verdict.reason}")

        # --- 2. Governance scan -----------------------------------------
        prompt_text = spec.get("prompt", "")
        gov_result: GovernanceScanResult = self._governance.scan_prompt(prompt_text)
        if not gov_result.passed:
            job_label_for_gov = spec.get("job_label", spec.get("label", "unknown"))
            blocked_by.append(
                f"governance:{gov_result.blocked_reason}. "
                f"Job: '{job_label_for_gov}'. "
                f"Remediation: Remove or redact the secret from job '{job_label_for_gov}' prompt before resubmitting."
            )
            governance_findings = gov_result.findings

        # --- 3. Loop detection ------------------------------------------
        job_label = spec.get("job_label", spec.get("label", "unknown"))
        loop_verdict: LoopVerdict = self._loop_detector.check(job_label)
        if loop_verdict.action == "stop":
            blocked_by.append(f"loop_detector:{'; '.join(loop_verdict.reasons)}")
        loop_verdict_dict = {
            "action": loop_verdict.action,
            "reasons": list(loop_verdict.reasons),
            "consecutive_failures": loop_verdict.consecutive_failures,
            "total_failures_in_window": loop_verdict.total_failures_in_window,
            "total_token_burn": loop_verdict.total_token_burn,
        }

        passed = len(blocked_by) == 0
        return PipelineGate(
            passed=passed,
            blocked_by=tuple(blocked_by),
            governance_findings=governance_findings,
            loop_verdict=loop_verdict_dict,
        )

    # ------------------------------------------------------------------
    # Conflict analysis
    # ------------------------------------------------------------------

    def check_conflicts(self, jobs: list[dict]) -> ConflictAnalysis:
        """Analyze a batch of jobs for write conflicts.

        Each job dict should have:
          - job_label: str
          - write_paths: list[str]
          - read_paths: list[str]  (optional, defaults to [])

        Returns a ConflictAnalysis with conflicts, serialization groups,
        and parallel-safe job labels.
        """
        scopes = [
            JobWriteScope(
                job_label=j["job_label"],
                write_paths=tuple(j.get("write_paths", [])),
                read_paths=tuple(j.get("read_paths", [])),
            )
            for j in jobs
        ]
        return self._conflict_resolver.analyze(scopes)

    # ------------------------------------------------------------------
    # Post-workflow
    # ------------------------------------------------------------------

    def post_workflow(
        self,
        job_label: str,
        succeeded: bool,
        failure_code: str = "",
        stderr: str = "",
        cost: float = 0.0,
    ) -> PostWorkflowAction:
        """Process the outcome of a workflow step and decide next steps.

        If succeeded: record success in loop detector, return 'complete'.
        If failed:
          - classify failure via AutoRetryManager
          - check loop detector
          - if retryable and not looping: return 'retry' with enriched context
          - if non-retryable or looping: return 'halt'
        """

        # --- Success path -----------------------------------------------
        if succeeded:
            self._loop_detector.record_success(job_label, _utc_now())
            return PostWorkflowAction(
                action="complete",
                retry_context=None,
                wait_seconds=0,
                reasons=("workflow succeeded",),
            )

        # --- Failure path -----------------------------------------------
        classification = self._auto_retry.classify(
            failure_code, stderr,
        )

        # Record failure in loop detector
        self._loop_detector.record_failure(
            FailureRecord(
                job_label=job_label,
                failure_code=failure_code,
                timestamp=_utc_now(),
                token_cost=cost,
                attempt_number=self._auto_retry.attempt_count(job_label) + 1,
            )
        )

        # Check loop detector verdict
        loop_verdict = self._loop_detector.check(job_label)

        # Non-retryable or loop detected -> halt
        if not classification.retryable:
            reasons = [f"non-retryable: {classification.reason}"]
            return PostWorkflowAction(
                action="halt",
                retry_context=None,
                wait_seconds=0,
                reasons=tuple(reasons),
            )

        if loop_verdict.action == "stop":
            reasons = list(loop_verdict.reasons)
            reasons.insert(0, "loop detector stopped retries")
            return PostWorkflowAction(
                action="halt",
                retry_context=None,
                wait_seconds=0,
                reasons=tuple(reasons),
            )

        # Retryable -> ask AutoRetryManager for decision
        retry_decision = self._auto_retry.should_retry(job_label, classification)
        self._auto_retry.record_attempt(job_label, classification)

        if not retry_decision.retry:
            return PostWorkflowAction(
                action="halt",
                retry_context=None,
                wait_seconds=0,
                reasons=(f"retries exhausted after {retry_decision.attempt_number} attempts",),
            )

        # Build retry context
        retry_context = self._retry_ctx.build(
            job_label=job_label,
            failure_code=failure_code,
            stderr=stderr,
            classification=classification,
        )

        return PostWorkflowAction(
            action="retry",
            retry_context=retry_context,
            wait_seconds=retry_decision.wait_seconds,
            reasons=(
                f"retry #{retry_decision.attempt_number}: {classification.reason}",
            ),
        )
