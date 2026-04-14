"""Capability inference feedback loop.

Tracks whether the capability inferred for a dispatch actually matched the
output quality observed.  Over time this lets ``compute_model_fitness``
weight results by quality rather than treating all successes equally.

Usage
-----
The module exposes a singleton tracker::

    from runtime.capability_feedback import get_capability_tracker

    tracker = get_capability_tracker()
    tracker.record_outcome(result, capabilities=["code_review"])

    # Inspect
    print(tracker.capability_accuracy("code_review"))
    print(tracker.model_capability_matrix())
    print(tracker.suggest_capability_reclassification())

Persistence
-----------
Outcomes are stored in the ``capability_outcomes`` table in Postgres.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .workflow import WorkflowResult
    from storage.postgres.connection import SyncPostgresConnection
    from storage.postgres.verification_repository import PostgresVerificationRepository

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CapabilityOutcome dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CapabilityOutcome:
    """A single recorded dispatch outcome with capability-level quality signals."""

    run_id: str
    provider_slug: str
    model_slug: str
    inferred_capabilities: list[str]
    succeeded: bool
    output_quality_signals: dict[str, float]  # capability → quality score [0.0, 1.0]
    recorded_at: str  # ISO-8601 string

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "inferred_capabilities": list(self.inferred_capabilities),
            "succeeded": self.succeeded,
            "output_quality_signals": dict(self.output_quality_signals),
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "CapabilityOutcome":
        return cls(
            run_id=d["run_id"],
            provider_slug=d["provider_slug"],
            model_slug=d["model_slug"],
            inferred_capabilities=list(d.get("inferred_capabilities", [])),
            succeeded=bool(d.get("succeeded", False)),
            output_quality_signals=dict(d.get("output_quality_signals", {})),
            recorded_at=d.get("recorded_at", ""),
        )

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "CapabilityOutcome":
        """Build from a Postgres row dict."""
        caps = row.get("inferred_capabilities") or []
        signals = row.get("output_quality_signals") or {}
        if isinstance(signals, str):
            signals = json.loads(signals)
        recorded = row.get("recorded_at")
        if hasattr(recorded, "isoformat"):
            recorded = recorded.isoformat()
        return cls(
            run_id=row["run_id"],
            provider_slug=row["provider_slug"],
            model_slug=row["model_slug"],
            inferred_capabilities=list(caps),
            succeeded=bool(row.get("succeeded", False)),
            output_quality_signals=dict(signals),
            recorded_at=str(recorded or ""),
        )


# ---------------------------------------------------------------------------
# Quality heuristics
# ---------------------------------------------------------------------------

_CODE_BLOCK_RE = re.compile(r"```[\w]*\n[\s\S]*?```", re.MULTILINE)
_DIFF_RE = re.compile(r"^[-+]{3}\s|^@@\s", re.MULTILINE)
_FUNC_DEF_RE = re.compile(r"\b(def |function |async def |class )\w+", re.MULTILINE)
_REVIEW_KEYWORD_RE = re.compile(
    r"\b(issue|suggestion|finding|problem|bug|error|warning|recommend|concern)\b",
    re.IGNORECASE,
)
_NUMBERED_LIST_RE = re.compile(r"^\s*\d+[.)]\s+\S", re.MULTILINE)
_ARCH_KEYWORD_RE = re.compile(
    r"\b(component|service|module|layer|interface|boundary|trade.?off|decision|pattern|"
    r"diagram|architecture|design)\b",
    re.IGNORECASE,
)
_ANALYSIS_SCORE_RE = re.compile(
    r"\b(score|rank|rating|result|output|value)\s*[:=]\s*[\d.]+",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_DATE_RE = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december"
    r"|\d{4}[-/]\d{2}[-/]\d{2})\b",
    re.IGNORECASE,
)
_DEBUG_KEYWORD_RE = re.compile(
    r"\b(cause|root.?cause|fix|because|reason|trace|stack.?trace|exception|error)\b",
    re.IGNORECASE,
)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _score_mechanical_edit(text: str) -> float:
    score = 0.0
    if _CODE_BLOCK_RE.search(text):
        score += 0.4
    if _DIFF_RE.search(text):
        score += 0.4
    if re.search(r"\b\w+\.\w{1,5}\b", text):
        score += 0.2
    return _clamp(score)


def _score_code_generation(text: str) -> float:
    score = 0.0
    if _CODE_BLOCK_RE.findall(text):
        score += 0.5
    if _FUNC_DEF_RE.search(text):
        score += 0.3
    if re.search(r"\b(import|return|const |let |var |def |function )\b", text):
        score += 0.2
    return _clamp(score)


def _score_code_review(text: str) -> float:
    score = 0.0
    findings = len(_REVIEW_KEYWORD_RE.findall(text))
    if findings >= 3:
        score += 0.4
    elif findings >= 1:
        score += 0.2
    if _NUMBERED_LIST_RE.search(text):
        score += 0.3
    if re.search(r"line\s+\d+|:\s*\d+\s*$", text, re.MULTILINE):
        score += 0.3
    return _clamp(score)


def _score_architecture(text: str) -> float:
    score = 0.0
    hits = len(_ARCH_KEYWORD_RE.findall(text))
    if hits >= 5:
        score += 0.5
    elif hits >= 2:
        score += 0.25
    if re.search(r"\b(pros?|cons?|advantage|disadvantage|trade.?off)\b", text, re.IGNORECASE):
        score += 0.3
    if re.search(r"[-+|]{3,}|graph\s+\w+|sequenceDiagram", text):
        score += 0.2
    return _clamp(score)


def _score_analysis(text: str) -> float:
    score = 0.0
    from .output_parser import parse_json_from_completion

    parsed = parse_json_from_completion(text)
    if parsed is not None:
        score += 0.5
    if _ANALYSIS_SCORE_RE.search(text):
        score += 0.3
    if _NUMBERED_LIST_RE.search(text):
        score += 0.2
    return _clamp(score)


def _score_creative(text: str) -> float:
    score = 0.0
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    if len(paragraphs) >= 3:
        score += 0.4
    elif len(paragraphs) >= 1:
        score += 0.2
    words = len(text.split())
    if words >= 200:
        score += 0.4
    elif words >= 80:
        score += 0.2
    if _CODE_BLOCK_RE.search(text):
        score -= 0.2
    return _clamp(score)


def _score_research(text: str) -> float:
    score = 0.0
    if _URL_RE.search(text):
        score += 0.3
    if _DATE_RE.search(text):
        score += 0.2
    if re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", text):
        score += 0.2
    words = len(text.split())
    if words >= 150:
        score += 0.3
    return _clamp(score)


def _score_debug(text: str) -> float:
    score = 0.0
    hits = len(_DEBUG_KEYWORD_RE.findall(text))
    if hits >= 4:
        score += 0.5
    elif hits >= 2:
        score += 0.25
    if re.search(r"(stack trace|traceback|at line \d+)", text, re.IGNORECASE):
        score += 0.3
    if _CODE_BLOCK_RE.search(text):
        score += 0.2
    return _clamp(score)


_SCORERS: dict[str, Any] = {
    "mechanical_edit": _score_mechanical_edit,
    "code_generation": _score_code_generation,
    "code_review": _score_code_review,
    "architecture": _score_architecture,
    "analysis": _score_analysis,
    "creative": _score_creative,
    "research": _score_research,
    "debug": _score_debug,
}


def assess_output_quality(
    completion: str,
    *,
    capabilities: list[str],
) -> dict[str, float]:
    """Assess per-capability quality of an LLM completion.

    Returns a dict mapping each requested capability to a quality score
    in [0.0, 1.0].  Unknown capabilities default to 0.5 (neutral).
    """
    if not isinstance(completion, str) or not completion.strip():
        return {cap: 0.0 for cap in capabilities}

    result: dict[str, float] = {}
    for cap in capabilities:
        scorer = _SCORERS.get(cap)
        if scorer is None:
            result[cap] = 0.5
        else:
            result[cap] = round(scorer(completion), 4)
    return result


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def _get_conn() -> SyncPostgresConnection:
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    return SyncPostgresConnection(get_workflow_pool())


def _get_repository(conn: "SyncPostgresConnection | None" = None) -> "PostgresVerificationRepository":
    from storage.postgres.verification_repository import PostgresVerificationRepository

    return PostgresVerificationRepository(conn or _get_conn())


# ---------------------------------------------------------------------------
# CapabilityTracker — Postgres-backed
# ---------------------------------------------------------------------------

class CapabilityTracker:
    """Records outcomes and derives accuracy/quality analytics.

    Persistence: ``capability_outcomes`` table in Postgres.
    """

    _QUALITY_THRESHOLD = 0.5

    def __init__(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_outcome(
        self,
        result: "WorkflowResult",
        *,
        capabilities: list[str],
    ) -> CapabilityOutcome:
        """Assess output quality and store the outcome."""
        completion = result.completion or ""
        succeeded = result.status == "succeeded"

        from .capability_router import TaskCapability
        all_caps = list(TaskCapability.all())
        quality_signals = assess_output_quality(completion, capabilities=all_caps)

        recorded_at = datetime.now(timezone.utc).isoformat()

        outcome = CapabilityOutcome(
            run_id=result.run_id,
            provider_slug=result.provider_slug,
            model_slug=result.model_slug or "unknown",
            inferred_capabilities=list(capabilities),
            succeeded=succeeded,
            output_quality_signals=quality_signals,
            recorded_at=recorded_at,
        )

        _get_repository().record_capability_outcome(
            run_id=outcome.run_id,
            provider_slug=outcome.provider_slug,
            model_slug=outcome.model_slug,
            inferred_capabilities=outcome.inferred_capabilities,
            succeeded=outcome.succeeded,
            output_quality_signals=outcome.output_quality_signals,
            recorded_at=recorded_at,
        )

        return outcome

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    def _load_all(self) -> list[CapabilityOutcome]:
        """Load all outcomes from Postgres."""
        rows = _get_repository().list_capability_outcomes()
        return [CapabilityOutcome.from_row(dict(r)) for r in rows]

    def capability_accuracy(self, capability: str) -> dict[str, Any]:
        """Return inference accuracy metrics for one capability."""
        outcomes = self._load_all()

        relevant = [
            o for o in outcomes
            if capability in o.inferred_capabilities
        ]
        total = len(relevant)
        if total == 0:
            return {
                "capability": capability,
                "total_workflows": 0,
                "quality_matched": 0,
                "accuracy_rate": 0.0,
                "avg_quality": 0.0,
                "threshold": self._QUALITY_THRESHOLD,
            }

        quality_scores = [
            o.output_quality_signals.get(capability, 0.0) for o in relevant
        ]
        matched = sum(1 for q in quality_scores if q > self._QUALITY_THRESHOLD)
        avg_quality = sum(quality_scores) / total

        return {
            "capability": capability,
            "total_workflows": total,
            "quality_matched": matched,
            "accuracy_rate": round(matched / total, 4),
            "avg_quality": round(avg_quality, 4),
            "threshold": self._QUALITY_THRESHOLD,
        }

    def model_capability_matrix(self) -> dict[str, Any]:
        """For each (model, capability), return attempts / successes / avg_quality."""
        outcomes = self._load_all()

        matrix: dict[str, dict[str, dict[str, Any]]] = {}

        for outcome in outcomes:
            model_key = f"{outcome.provider_slug}/{outcome.model_slug}"
            model_data = matrix.setdefault(model_key, {})

            for cap in outcome.inferred_capabilities:
                cap_data = model_data.setdefault(cap, {
                    "attempts": 0, "successes": 0,
                    "quality_matched": 0, "_quality_sum": 0.0,
                })
                quality = outcome.output_quality_signals.get(cap, 0.0)
                cap_data["attempts"] += 1
                if outcome.succeeded:
                    cap_data["successes"] += 1
                if quality > self._QUALITY_THRESHOLD:
                    cap_data["quality_matched"] += 1
                cap_data["_quality_sum"] += quality

        result: dict[str, Any] = {}
        for model_key, model_data in matrix.items():
            result[model_key] = {}
            for cap, cap_data in model_data.items():
                n = cap_data["attempts"]
                result[model_key][cap] = {
                    "attempts": n,
                    "successes": cap_data["successes"],
                    "quality_matched": cap_data["quality_matched"],
                    "avg_quality": round(cap_data["_quality_sum"] / n, 4) if n else 0.0,
                }
        return result

    def suggest_capability_reclassification(self) -> list[dict[str, Any]]:
        """Find dispatches where inference was probably wrong."""
        outcomes = self._load_all()

        from .capability_router import TaskCapability
        all_caps = set(TaskCapability.all())

        suggestions: list[dict[str, Any]] = []
        for outcome in outcomes:
            inferred_set = set(outcome.inferred_capabilities)
            inferred_low = {
                cap: q
                for cap, q in outcome.output_quality_signals.items()
                if cap in inferred_set and q <= self._QUALITY_THRESHOLD
            }
            if not inferred_low:
                continue

            high_alternatives = {
                cap: q
                for cap, q in outcome.output_quality_signals.items()
                if cap not in inferred_set and q > self._QUALITY_THRESHOLD
            }
            if not high_alternatives:
                continue

            suggestions.append({
                "run_id": outcome.run_id,
                "provider_slug": outcome.provider_slug,
                "model_slug": outcome.model_slug,
                "inferred_capabilities": list(outcome.inferred_capabilities),
                "inferred_quality": dict(inferred_low),
                "suggested_capabilities": list(high_alternatives.keys()),
                "suggested_quality": high_alternatives,
                "recorded_at": outcome.recorded_at,
            })
        return suggestions

    def all_outcomes(self) -> list[CapabilityOutcome]:
        """Return all recorded outcomes."""
        return self._load_all()


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_TRACKER: CapabilityTracker | None = None


def get_capability_tracker() -> CapabilityTracker:
    """Return the module-level CapabilityTracker singleton."""
    global _TRACKER
    if _TRACKER is None:
        _TRACKER = CapabilityTracker()
    return _TRACKER
