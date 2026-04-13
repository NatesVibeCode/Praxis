"""Review authorship tracking and review-dispatch linkage.

When model A builds code and model B reviews it, this module links review
findings back to the original author and accumulates per-author bug density
metrics so the platform can decide how much review coverage a given model needs.

Persistence: ``review_records`` table in Postgres.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .workflow import WorkflowResult
    from storage.postgres.connection import SyncPostgresConnection

_log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReviewRecord:
    """One recorded review event."""

    review_run_id: str
    reviewer_model: str  # "provider/model" of the reviewing model
    author_model: str  # "provider/model" of the model whose work was reviewed
    task_type: str  # inferred from label or review content
    modules_reviewed: list[str]  # files/modules covered
    findings: list[dict]  # raw parsed findings from completion
    bug_count: int  # total bugs found (any severity)
    severity_counts: dict  # {"high": N, "medium": N, "low": N, "none": N}
    reviewed_at: str  # ISO-8601 string
    reviewed_workflow_id: str | None = None
    dimension_scores: list[dict] | None = None
    avg_dimension_scores: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReviewRecord:
        return cls(
            review_run_id=data["review_run_id"],
            reviewer_model=data["reviewer_model"],
            author_model=data["author_model"],
            task_type=data["task_type"],
            modules_reviewed=data.get("modules_reviewed") or [],
            findings=data.get("findings") or [],
            bug_count=int(data.get("bug_count", 0)),
            severity_counts=data.get("severity_counts") or {},
            reviewed_at=data["reviewed_at"],
            reviewed_workflow_id=data.get("reviewed_workflow_id"),
            dimension_scores=data.get("dimension_scores"),
            avg_dimension_scores=data.get("avg_dimension_scores"),
        )


# ---------------------------------------------------------------------------
# Finding / severity parsing
# ---------------------------------------------------------------------------

_SEVERITY_KEYS = ("high", "medium", "low", "none")

_FINDING_PATTERNS = [
    re.compile(r"```(?:json)?\s*(\[.*?\])\s*```", re.DOTALL | re.IGNORECASE),
    re.compile(r"(\[\s*\{.*?\}\s*\])", re.DOTALL),
]

_SEVERITY_WORD_RE = re.compile(
    r"\b(critical|high|medium|low|minor|none|info)\b", re.IGNORECASE,
)

_SEVERITY_NORMALISE = {
    "critical": "high", "high": "high", "medium": "medium",
    "low": "low", "minor": "low", "none": "none", "info": "none",
}


def _parse_findings(completion: str | None) -> list[dict]:
    """Extract structured finding objects from LLM completion text."""
    if not completion:
        return []

    for pattern in _FINDING_PATTERNS:
        m = pattern.search(completion)
        if m:
            try:
                parsed = json.loads(m.group(1))
                if isinstance(parsed, list):
                    out = [item for item in parsed if isinstance(item, dict)]
                    if out:
                        return out
            except (json.JSONDecodeError, ValueError):
                pass

    findings: list[dict] = []
    bullet_re = re.compile(r"(?:^|\n)\s*(?:\d+\.|[-*])\s+(.+?)(?=\n\s*(?:\d+\.|[-*]|\Z))", re.DOTALL)
    for m in bullet_re.finditer(completion):
        text = m.group(1).strip()
        if len(text) < 10:
            continue
        severity_m = _SEVERITY_WORD_RE.search(text)
        severity = _SEVERITY_NORMALISE.get(
            severity_m.group(1).lower() if severity_m else "none", "none"
        )
        findings.append({"severity": severity, "description": text})

    return findings


_DIMENSION_KEYS = ("outcome", "correctness", "safety", "resilience", "integration", "scope", "diligence")


def _parse_dimension_scores(completion: str | None) -> tuple[list[dict], dict | None]:
    """Extract per-module dimension scores from structured review output."""
    if not completion:
        return [], None

    sources = []
    for pattern in _FINDING_PATTERNS:
        m = pattern.search(completion)
        if m:
            try:
                sources.append(json.loads(m.group(1)))
            except (json.JSONDecodeError, ValueError):
                pass

    try:
        sources.append(json.loads(completion))
    except (json.JSONDecodeError, ValueError, TypeError):
        pass

    for parsed in sources:
        if isinstance(parsed, dict) and "modules" in parsed:
            modules = parsed["modules"]
            if isinstance(modules, list):
                scores = []
                for mod in modules:
                    if isinstance(mod, dict) and "scores" in mod:
                        scores.append({
                            "module": mod.get("module", "unknown"),
                            "scores": {
                                k: min(3, max(0, int(mod["scores"].get(k, 2))))
                                for k in _DIMENSION_KEYS
                            },
                            "composite": float(mod.get("composite", 0)),
                            "findings": mod.get("findings", []),
                        })
                if scores:
                    avgs = {}
                    for dim in _DIMENSION_KEYS:
                        vals = [s["scores"][dim] for s in scores]
                        avgs[dim] = round(sum(vals) / len(vals), 2) if vals else 0.0
                    return scores, avgs

    return [], None


def _count_severities(findings: list[dict]) -> dict:
    counts: dict[str, int] = {k: 0 for k in _SEVERITY_KEYS}
    for f in findings:
        raw = str(f.get("severity", "none")).lower()
        normalised = _SEVERITY_NORMALISE.get(raw, "none")
        counts[normalised] = counts.get(normalised, 0) + 1
    return counts


def _count_bugs(severity_counts: dict) -> int:
    return sum(v for k, v in severity_counts.items() if k != "none")


def _infer_task_type(result: WorkflowResult) -> str:
    label = (result.label or "").lower()
    if ":" in label:
        return label.split(":", 1)[1].strip() or "general"
    for kw in ("code", "build", "implement", "generate"):
        if kw in label:
            return "code_generation"
    for kw in ("arch", "design", "plan"):
        if kw in label:
            return "architecture"
    for kw in ("test", "spec", "qa"):
        if kw in label:
            return "testing"
    return "general"


def _is_review_dispatch(result: WorkflowResult) -> bool:
    if result.reviews_workflow_id is not None:
        return True
    label = (result.label or "").lower()
    return label.startswith("review:") or label == "review"


# ---------------------------------------------------------------------------
# Receipt lookup — resolve author_model from original dispatch's receipt
# ---------------------------------------------------------------------------

def _lookup_author_from_receipt(reviewed_workflow_id: str) -> str | None:
    from . import receipt_store

    rec = receipt_store.find_receipt_by_run_id(reviewed_workflow_id)
    if rec is None:
        return None
    data = rec.to_dict()
    if data.get("author_model"):
        return data["author_model"]
    provider = data.get("provider_slug", "unknown")
    model = data.get("model_slug")
    return f"{provider}/{model}" if model else provider


def _lookup_reviewed_receipt_payload(reviewed_workflow_id: str) -> dict[str, Any] | None:
    from . import receipt_store

    rec = receipt_store.find_receipt_by_run_id(reviewed_workflow_id)
    if rec is None:
        return None
    return rec.to_dict()


# ---------------------------------------------------------------------------
# ReviewTracker — Postgres-backed
# ---------------------------------------------------------------------------

def _get_conn() -> SyncPostgresConnection:
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    return SyncPostgresConnection(get_workflow_pool())


class ReviewTracker:
    """Track review events, link to authors, compute bug density.

    Persistence: Postgres ``review_records`` table.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_review(self, review_result: WorkflowResult) -> ReviewRecord | None:
        """Record a review workflow result. Returns None for non-review dispatches."""
        if not _is_review_dispatch(review_result):
            return None

        findings = _parse_findings(review_result.completion)
        severity_counts = _count_severities(findings)
        bug_count = _count_bugs(severity_counts)
        task_type = _infer_task_type(review_result)

        dimension_scores, avg_dimension_scores = _parse_dimension_scores(
            review_result.completion
        )

        author_model: str | None = None
        reviewed_payload: dict[str, Any] | None = None
        if review_result.reviews_workflow_id:
            reviewed_payload = _lookup_reviewed_receipt_payload(review_result.reviews_workflow_id)
            author_model = _lookup_author_from_receipt(review_result.reviews_workflow_id)
        if not author_model:
            author_model = review_result.author_model or review_result.provider_slug

        reviewer_model = review_result.author_model or review_result.provider_slug
        modules_reviewed: list[str] = list(review_result.review_target_modules or [])
        reviewed_at = _utc_now().isoformat()
        if reviewed_payload and reviewed_payload.get("task_type"):
            task_type = str(reviewed_payload.get("task_type") or task_type)

        record = ReviewRecord(
            review_run_id=review_result.run_id,
            reviewed_workflow_id=review_result.reviews_workflow_id,
            author_model=author_model,
            reviewer_model=reviewer_model,
            task_type=task_type,
            modules_reviewed=modules_reviewed,
            findings=findings,
            bug_count=bug_count,
            severity_counts=severity_counts,
            reviewed_at=reviewed_at,
            dimension_scores=dimension_scores or None,
            avg_dimension_scores=avg_dimension_scores,
        )

        conn = _get_conn()
        conn.execute(
            """INSERT INTO review_records
               (review_run_id, reviewed_workflow_id, reviewer_model, author_model,
                task_type, modules_reviewed, findings, bug_count, severity_counts,
                dimension_scores, avg_dimension_scores, reviewed_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12)""",
            record.review_run_id, record.reviewed_workflow_id,
            record.reviewer_model, record.author_model,
            record.task_type, record.modules_reviewed,
            json.dumps(record.findings), record.bug_count,
            json.dumps(record.severity_counts),
            json.dumps(record.dimension_scores) if record.dimension_scores else None,
            json.dumps(record.avg_dimension_scores) if record.avg_dimension_scores else None,
            reviewed_at,
        )

        if (
            author_model
            and "/" in author_model
            and task_type
            and task_type != "general"
        ):
            from .task_type_router import TaskTypeRouter

            provider_slug, model_slug = author_model.split("/", 1)
            conn = _get_conn()
            TaskTypeRouter(conn).record_review_feedback(
                task_type,
                provider_slug,
                model_slug,
                bug_count=bug_count,
                severity_counts=severity_counts,
            )

        return record

    def author_bug_density(
        self,
        author_model: str,
        *,
        task_type: str | None = None,
    ) -> dict[str, Any]:
        """Return bug density stats for one author."""
        conn = _get_conn()
        if task_type:
            rows = conn.execute(
                """SELECT bug_count, severity_counts, task_type
                   FROM review_records
                   WHERE author_model = $1 AND task_type = $2""",
                author_model, task_type,
            )
        else:
            rows = conn.execute(
                """SELECT bug_count, severity_counts, task_type
                   FROM review_records WHERE author_model = $1""",
                author_model,
            )

        total_reviews = len(rows)
        total_bugs = sum(r["bug_count"] for r in rows)
        bug_density = total_bugs / total_reviews if total_reviews > 0 else 0.0

        by_severity: dict[str, int] = {k: 0 for k in _SEVERITY_KEYS}
        by_task_type: dict[str, dict] = {}
        for r in rows:
            sc = r["severity_counts"]
            if isinstance(sc, str):
                sc = json.loads(sc)
            for sev, cnt in (sc or {}).items():
                by_severity[sev] = by_severity.get(sev, 0) + cnt
            tt = by_task_type.setdefault(r["task_type"], {"reviews": 0, "bugs": 0})
            tt["reviews"] += 1
            tt["bugs"] += r["bug_count"]

        return {
            "total_reviews": total_reviews,
            "total_bugs": total_bugs,
            "bug_density": round(bug_density, 4),
            "by_severity": by_severity,
            "by_task_type": by_task_type,
        }

    def author_summary(self) -> list[dict[str, Any]]:
        """Return per-author summary rows."""
        conn = _get_conn()
        rows = conn.execute(
            "SELECT DISTINCT author_model FROM review_records ORDER BY author_model"
        )
        all_authors = [r["author_model"] for r in rows]

        result = []
        for author in all_authors:
            stats = self.author_bug_density(author)
            requirement = self.review_requirement(author, task_type=None)
            result.append({
                "author_model": author,
                "total_reviews": stats["total_reviews"],
                "total_bugs": stats["total_bugs"],
                "bug_density": stats["bug_density"],
                "by_severity": stats["by_severity"],
                "review_requirement": requirement,
            })
        return result

    def review_requirement(
        self,
        author_model: str,
        task_type: str | None = None,
    ) -> str:
        """Return the review requirement level for an author.

        Levels:
          "none"        — density < 0.1 and >= 20 samples
          "spot_check"  — density < 0.3 and >= 10 samples
          "full_review" — density < 0.5
          "block"       — density >= 0.5 or < 5 samples
        """
        stats = self.author_bug_density(author_model, task_type=task_type)
        n = stats["total_reviews"]
        density = stats["bug_density"]

        if n < 5 or density >= 0.5:
            return "block"
        if density < 0.1 and n >= 20:
            return "none"
        if density < 0.3 and n >= 10:
            return "spot_check"
        return "full_review"

    def author_review_history(
        self,
        author_model: str,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return the raw review records for one author (newest first)."""
        conn = _get_conn()
        rows = conn.execute(
            """SELECT review_run_id, reviewed_workflow_id, reviewer_model,
                      author_model, task_type, modules_reviewed, findings,
                      bug_count, severity_counts, dimension_scores,
                      avg_dimension_scores, reviewed_at
               FROM review_records
               WHERE author_model = $1
               ORDER BY reviewed_at DESC
               LIMIT $2""",
            author_model, limit,
        )

        result = []
        for r in rows:
            d = dict(r)
            # Ensure JSONB fields are dicts not strings
            for key in ("findings", "severity_counts", "dimension_scores", "avg_dimension_scores"):
                if isinstance(d.get(key), str):
                    try:
                        d[key] = json.loads(d[key])
                    except (json.JSONDecodeError, TypeError) as exc:
                        raise ValueError(
                            f"Malformed review_records.{key} JSON for review_run_id={d.get('review_run_id')}"
                        ) from exc
            result.append(d)
        return result


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_tracker: ReviewTracker | None = None
_tracker_lock = threading.Lock()


def get_review_tracker() -> ReviewTracker:
    """Return the process-level ReviewTracker singleton."""
    global _tracker
    if _tracker is None:
        with _tracker_lock:
            if _tracker is None:
                _tracker = ReviewTracker()
    return _tracker
