"""Self-healing orchestrator for dispatch job recovery."""

import enum
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

__all__ = [
    "RecoveryAction",
    "DiagnosticResult",
    "DiagnosticModule",
    "EarlyTerminationDiagnostic",
    "ScopeRecoveryDiagnostic",
    "DependencyDiagnostic",
    "OrchestrationFailureDiagnostic",
    "HealingRecommendation",
    "SelfHealingOrchestrator",
    "derive_terminal_reason_code",
    "normalize_failure_code",
]


_NORMALIZED_FAILURE_CODE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"failure_code\b.*non-empty string", re.IGNORECASE),
        "orchestration.failure_code_missing",
    ),
    (
        re.compile(r"reason_code\b.*non-empty string", re.IGNORECASE),
        "orchestration.reason_code_missing",
    ),
    (
        re.compile(r"submission receipt sync failed", re.IGNORECASE),
        "workflow_submission.service_error",
    ),
    (
        re.compile(r"workflow_submission\.service_error", re.IGNORECASE),
        "workflow_submission.service_error",
    ),
)

_GENERIC_WRAPPER_FAILURE_CODES = frozenset({
    "execution_exception",
    "worker_exception",
    "worker_future_exception",
    "dispatch.execution_crash",
    "dispatch.thread_error",
    "unknown",
})


def normalize_failure_code(
    failure_code: str | None,
    stderr: str | None = None,
) -> str:
    """Return a usable failure code even when the upstream envelope is broken."""

    normalized = str(failure_code or "").strip()
    stderr_text = str(stderr or "")
    for pattern, inferred_code in _NORMALIZED_FAILURE_CODE_PATTERNS:
        if pattern.search(stderr_text) and (
            not normalized or normalized in _GENERIC_WRAPPER_FAILURE_CODES
        ):
            return inferred_code
    if normalized:
        return normalized
    return "unknown"


# Attribute probe order for ``derive_terminal_reason_code``. Ordered so a
# worker-emitted typed error's ``.reason_code`` wins over legacy ``.failure_code``
# (from older adapters) and the generic Python/library ``.code``.
_TERMINAL_REASON_CODE_ATTRS: tuple[str, ...] = (
    "reason_code",
    "failure_code",
    "error_code",
    "code",
)


def derive_terminal_reason_code(exc: BaseException, *, fallback: str) -> str:
    """Canonical authority: map an exception to a stable terminal reason code.

    This is the SINGLE place where worker-layer code is converted from a live
    exception into a stable string reason_code for persistence. Previously
    ``runtime/workflow/worker.py`` and ``runtime/workflow/_worker_loop.py``
    each defined their own ``_worker_error_code`` helper — two independent
    authorities for the same decision, each free to drift. Closes the worker
    half of BUG-CBC73AB3 (failure-code authority split).

    Order of authority:

    1. If the exception carries one of the canonical typed-error attributes
       (``reason_code``, ``failure_code``, ``error_code``, ``code``) with a
       non-empty string value, that is the stable code.
    2. Otherwise the caller-supplied ``fallback`` is used.

    In both branches the result is run through :func:`normalize_failure_code`
    with ``str(exc)`` as the stderr signal so that *wrapper* codes like
    ``worker_exception`` are upgraded to a more specific inferred code when
    the exception text clearly names the failure — e.g. the well-known
    "failure_code … non-empty string" orchestration envelope error.

    ``fallback`` must be a non-empty string; the caller's contract is that it
    names the worker path (``worker_exception``, ``worker_future_exception``,
    ``workflow_graph_execution_failed``) so operators can trace the origin.
    """

    fallback = str(fallback or "").strip()
    if not fallback:
        raise ValueError("derive_terminal_reason_code: fallback must be non-empty")
    for attr in _TERMINAL_REASON_CODE_ATTRS:
        value = getattr(exc, attr, None)
        if isinstance(value, str) and value.strip():
            return normalize_failure_code(value.strip(), str(exc))
    return normalize_failure_code(fallback, str(exc))


# ---------------------------------------------------------------------------
# Recovery actions
# ---------------------------------------------------------------------------

class RecoveryAction(enum.Enum):
    RETRY_SAME = "retry_same"
    RETRY_ESCALATED = "retry_escalated"
    SKIP = "skip"
    HALT = "halt"
    FIX_AND_RETRY = "fix_and_retry"


# ---------------------------------------------------------------------------
# Diagnostic result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DiagnosticResult:
    module_name: str
    recommendation: RecoveryAction
    confidence: float  # 0.0 - 1.0
    reason: str
    context_patch: Optional[str] = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            object.__setattr__(self, "confidence", max(0.0, min(1.0, self.confidence)))


# ---------------------------------------------------------------------------
# Diagnostic module ABC
# ---------------------------------------------------------------------------

class DiagnosticModule(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> DiagnosticResult: ...


# ---------------------------------------------------------------------------
# Built-in diagnostics
# ---------------------------------------------------------------------------

class EarlyTerminationDiagnostic(DiagnosticModule):
    """If the same failure repeats 3+ times, recommend SKIP."""

    def __init__(self) -> None:
        self._history: Dict[str, List[str]] = defaultdict(list)

    @property
    def name(self) -> str:
        return "early_termination"

    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> DiagnosticResult:
        key = f"{job_label}::{failure_code}"
        self._history[key].append(stderr)
        count = len(self._history[key])

        if count >= 3:
            return DiagnosticResult(
                module_name=self.name,
                recommendation=RecoveryAction.SKIP,
                confidence=min(0.7 + 0.05 * (count - 3), 0.95),
                reason=f"Same failure repeated {count} times for {job_label} ({failure_code}); skipping to avoid loop",
            )
        return DiagnosticResult(
            module_name=self.name,
            recommendation=RecoveryAction.RETRY_SAME,
            confidence=0.3,
            reason=f"Failure count {count} < 3; retry is still viable",
        )


class ScopeRecoveryDiagnostic(DiagnosticModule):
    """If stderr mentions scope/path issues, recommend FIX_AND_RETRY with narrowed scope."""

    _SCOPE_PATTERNS = re.compile(r"\b(scope|path|directory|folder|file not found|no such file)\b", re.IGNORECASE)

    @property
    def name(self) -> str:
        return "scope_recovery"

    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> DiagnosticResult:
        if self._SCOPE_PATTERNS.search(stderr):
            return DiagnosticResult(
                module_name=self.name,
                recommendation=RecoveryAction.FIX_AND_RETRY,
                confidence=0.75,
                reason="Scope/path issue detected in stderr; narrowing scope for retry",
                context_patch=f"Narrow scope for {job_label}: verify paths exist before operating",
            )
        return DiagnosticResult(
            module_name=self.name,
            recommendation=RecoveryAction.RETRY_SAME,
            confidence=0.1,
            reason="No scope/path indicators found",
        )


class DependencyDiagnostic(DiagnosticModule):
    """If stderr mentions import/module issues, recommend RETRY_SAME or HALT."""

    _TRANSIENT_PATTERNS = re.compile(r"\b(importerror|modulenotfounderror)\b", re.IGNORECASE)
    _HARD_PATTERNS = re.compile(r"\b(no module named|cannot import name|missing dep)\b", re.IGNORECASE)

    @property
    def name(self) -> str:
        return "dependency"

    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> DiagnosticResult:
        has_import = bool(re.search(r"\b(import|module)\b", stderr, re.IGNORECASE))
        if not has_import:
            return DiagnosticResult(
                module_name=self.name,
                recommendation=RecoveryAction.RETRY_SAME,
                confidence=0.1,
                reason="No import/module indicators found",
            )

        if self._HARD_PATTERNS.search(stderr):
            return DiagnosticResult(
                module_name=self.name,
                recommendation=RecoveryAction.HALT,
                confidence=0.85,
                reason="Hard dependency failure detected; halting until resolved",
            )

        return DiagnosticResult(
            module_name=self.name,
            recommendation=RecoveryAction.RETRY_SAME,
            confidence=0.55,
            reason="Transient import issue; retry may resolve",
        )


class OrchestrationFailureDiagnostic(DiagnosticModule):
    """Detect runner/meta failures where useful work landed before orchestration died."""

    _META_FAILURE_CODES = frozenset({
        "orchestration.failure_code_missing",
        "orchestration.reason_code_missing",
        "workflow_submission.service_error",
        "worker_exception",
        "worker_future_exception",
        "dispatch.execution_crash",
        "dispatch.thread_error",
    })
    _META_PATTERNS = re.compile(
        r"("
        r"failure_code\b.*non-empty string|"
        r"reason_code\b.*non-empty string|"
        r"submission receipt sync failed|"
        r"workflow_submission\.service_error|"
        r"worker_future_exception|"
        r"worker_exception"
        r")",
        re.IGNORECASE,
    )

    @property
    def name(self) -> str:
        return "orchestration_failure"

    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> DiagnosticResult:
        resolved_code = normalize_failure_code(failure_code, stderr)
        if resolved_code in {
            "orchestration.failure_code_missing",
            "orchestration.reason_code_missing",
        }:
            return DiagnosticResult(
                module_name=self.name,
                recommendation=RecoveryAction.FIX_AND_RETRY,
                confidence=0.96,
                reason=(
                    "Runner failed while building the terminal failure envelope; "
                    "preserve completed artifacts, synthesize a stable failure code, "
                    "and retry the affected jobs."
                ),
                context_patch=(
                    f"Treat {job_label} as an orchestration-envelope failure: keep any "
                    "artifacts already written, assign a synthetic failure code, and "
                    "retry only the failed frontier jobs before unblocking descendants."
                ),
            )

        if resolved_code in self._META_FAILURE_CODES or self._META_PATTERNS.search(stderr):
            return DiagnosticResult(
                module_name=self.name,
                recommendation=RecoveryAction.FIX_AND_RETRY,
                confidence=0.82,
                reason=(
                    "Workflow orchestration failed after job execution crossed the finish line; "
                    "repair the control-plane seam and retry from the failed boundary instead "
                    "of discarding downstream progress."
                ),
                context_patch=(
                    f"Preserve job outputs for {job_label}, repair the orchestration boundary, "
                    "then retry the failed frontier jobs so cancelled descendants can resume."
                ),
            )

        return DiagnosticResult(
            module_name=self.name,
            recommendation=RecoveryAction.RETRY_SAME,
            confidence=0.05,
            reason="No orchestration failure indicators found",
        )


# ---------------------------------------------------------------------------
# Healing recommendation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HealingRecommendation:
    action: RecoveryAction
    reason: str
    confidence: float
    context_patches: Tuple[str, ...]
    diagnostics_run: int


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class SelfHealingOrchestrator:
    """Runs diagnostics and picks the highest-confidence recommendation."""

    def __init__(self, diagnostics: Optional[List[DiagnosticModule]] = None) -> None:
        self._diagnostics: List[DiagnosticModule] = diagnostics if diagnostics is not None else [
            EarlyTerminationDiagnostic(),
            OrchestrationFailureDiagnostic(),
            ScopeRecoveryDiagnostic(),
            DependencyDiagnostic(),
        ]

    @staticmethod
    def resolve_failure_code(failure_code: str | None, stderr: str | None = None) -> str:
        return normalize_failure_code(failure_code, stderr)

    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> HealingRecommendation:
        normalized_failure_code = self.resolve_failure_code(failure_code, stderr)
        results: List[DiagnosticResult] = []
        for diag in self._diagnostics:
            result = diag.diagnose(job_label, normalized_failure_code, stderr)
            results.append(result)

        if not results:
            return HealingRecommendation(
                action=RecoveryAction.RETRY_SAME,
                reason="No diagnostics available",
                confidence=0.0,
                context_patches=(),
                diagnostics_run=0,
            )

        best = max(results, key=lambda r: r.confidence)
        patches = tuple(r.context_patch for r in results if r.context_patch is not None)

        return HealingRecommendation(
            action=best.recommendation,
            reason=best.reason,
            confidence=best.confidence,
            context_patches=patches,
            diagnostics_run=len(results),
        )
