"""Self-healing orchestrator for dispatch job recovery."""

import enum
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple


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
            ScopeRecoveryDiagnostic(),
            DependencyDiagnostic(),
        ]

    def diagnose(self, job_label: str, failure_code: str, stderr: str) -> HealingRecommendation:
        results: List[DiagnosticResult] = []
        for diag in self._diagnostics:
            result = diag.diagnose(job_label, failure_code, stderr)
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
