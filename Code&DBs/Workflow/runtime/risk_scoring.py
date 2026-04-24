"""Per-file risk scoring for workflow work routing.

Analyzes workflow receipt history to identify risky files (high failure rate,
frequently touched, slow to work on). This helps route risky files to stronger
models and identify areas that need refactoring.

Risk scores are derived from Postgres receipt authority. Manual export remains
available for diagnostics only when a caller provides an explicit output path;
there is no default artifact JSON authority.

Risk dimensions (6 weighted factors, 0-100 scale):
  - (1 - success_rate) × 35  — failure history dominates
  - min(touch_count / 10, 1.0) × 15  — churn rate
  - min(avg_duration_ms / 300000, 1.0) × 15  — complexity proxy
  - min(unique_failure_codes / 5, 1.0) × 15  — failure diversity
  - staleness × 10  — days since last touch (0-10 scale capped)
  - min(file_size_kb / 100, 1.0) × 10  — size proxy

The formula rewards success, low churn, fast execution, and recent activity.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple, Dict, List

from .receipt_store import list_receipt_payloads

_log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# FileRiskScore
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FileRiskScore:
    """Risk assessment for a single file.

    Attributes:
        file_path: Absolute or relative path to the file
        risk_score: Computed risk (0-100, higher = riskier)
        success_rate: Fraction of successful touches (0.0-1.0)
        touch_count: Number of times this file was touched in receipts
        avg_duration_ms: Average duration (ms) of jobs touching this file
        failure_codes: Tuple of unique failure codes observed
        last_touched: ISO datetime of most recent touch (or None if never touched)
        file_size_kb: File size in KB (0 if not found)
    """

    file_path: str
    risk_score: float  # 0-100
    success_rate: float  # 0.0-1.0
    touch_count: int
    avg_duration_ms: int
    failure_codes: Tuple[str, ...]
    last_touched: Optional[datetime]
    file_size_kb: int

    def to_json(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "file_path": self.file_path,
            "risk_score": round(self.risk_score, 2),
            "success_rate": round(self.success_rate, 4),
            "touch_count": self.touch_count,
            "avg_duration_ms": self.avg_duration_ms,
            "failure_codes": list(self.failure_codes),
            "last_touched": self.last_touched.isoformat() if self.last_touched else None,
            "file_size_kb": self.file_size_kb,
        }


# ---------------------------------------------------------------------------
# RiskScorer
# ---------------------------------------------------------------------------


class RiskScorer:
    """Computes per-file risk scores from workflow receipt history."""

    def __init__(self):
        """Initialize scorer with empty data."""
        self._scores_by_path: Dict[str, FileRiskScore] = {}
        self._file_stats: Dict[str, Dict[str, Any]] = {}

    def compute_from_receipts(
        self,
        receipts_dir: Optional[str] = None,
    ) -> List[FileRiskScore]:
        """Scan workflow receipts and compute risk scores for all touched files.

        Returns:
            List of FileRiskScore objects, sorted by risk descending
        """
        del receipts_dir

        # Accumulate statistics per file
        # file_path → {success_count, failure_count, durations[], failure_codes[], last_touched}
        file_stats: Dict[str, Dict[str, Any]] = {}

        receipts = list_receipt_payloads(limit=100_000)

        for receipt in receipts:

            status = receipt.get("status", "")
            succeeded = status == "succeeded"
            latency_ms = receipt.get("latency_ms", 0)
            finished_at = receipt.get("finished_at")
            failure_code = receipt.get("failure_code")

            # Parse finished_at timestamp
            try:
                if isinstance(finished_at, str):
                    last_touched_dt = datetime.fromisoformat(finished_at)
                else:
                    last_touched_dt = None
            except (ValueError, TypeError):
                last_touched_dt = None

            # Extract file paths from scope_read and scope_write
            scope_read = receipt.get("scope_read") or []
            scope_write = receipt.get("scope_write") or []
            all_files = list(set(scope_read + scope_write))

            for file_path in all_files:
                if not file_path or file_path.startswith("**"):
                    # Skip glob patterns and empty paths
                    continue

                stats = file_stats.setdefault(file_path, {
                    "success_count": 0,
                    "failure_count": 0,
                    "durations": [],
                    "failure_codes": set(),
                    "last_touched": None,
                })

                if succeeded:
                    stats["success_count"] += 1
                else:
                    stats["failure_count"] += 1
                    if failure_code:
                        stats["failure_codes"].add(failure_code)

                if latency_ms > 0:
                    stats["durations"].append(latency_ms)

                if last_touched_dt:
                    if stats["last_touched"] is None:
                        stats["last_touched"] = last_touched_dt
                    else:
                        stats["last_touched"] = max(stats["last_touched"], last_touched_dt)

        # Convert accumulated stats to FileRiskScore objects
        scores: List[FileRiskScore] = []

        for file_path, stats in file_stats.items():
            success_count = stats["success_count"]
            failure_count = stats["failure_count"]
            touch_count = success_count + failure_count
            durations = stats["durations"]
            failure_codes = tuple(sorted(stats["failure_codes"]))
            last_touched = stats["last_touched"]

            # Compute risk dimensions
            success_rate = success_count / touch_count if touch_count > 0 else 0.0
            avg_duration_ms = int(sum(durations) / len(durations)) if durations else 0

            # Get file size
            file_size_kb = self._get_file_size_kb(file_path)

            # Compute staleness (days since last touch, normalized)
            staleness_score = 0.0
            if last_touched:
                age_days = (_utc_now() - last_touched).days
                staleness_score = min(age_days / 30.0, 1.0)  # 30 days = max score

            # Risk formula (6 dimensions, weighted)
            risk = (
                (1.0 - success_rate) * 35.0
                + min(touch_count / 10.0, 1.0) * 15.0
                + min(avg_duration_ms / 300000.0, 1.0) * 15.0
                + min(len(failure_codes) / 5.0, 1.0) * 15.0
                + staleness_score * 10.0
                + min(file_size_kb / 100.0, 1.0) * 10.0
            )
            risk = max(0.0, min(100.0, risk))  # Clamp to 0-100

            score = FileRiskScore(
                file_path=file_path,
                risk_score=risk,
                success_rate=success_rate,
                touch_count=touch_count,
                avg_duration_ms=avg_duration_ms,
                failure_codes=failure_codes,
                last_touched=last_touched,
                file_size_kb=file_size_kb,
            )
            scores.append(score)

        # Sort by risk descending
        scores.sort(key=lambda s: -s.risk_score)
        self._scores_by_path = {s.file_path: s for s in scores}
        self._file_stats = file_stats

        return scores

    def risk_for_files(self, paths: List[str]) -> List[FileRiskScore]:
        """Get risk scores for specific files.

        Returns a list of FileRiskScore objects for requested paths,
        only including those that were found in receipt history.

        Args:
            paths: List of file paths to query

        Returns:
            List of FileRiskScore objects (may be shorter than input)
        """
        result = []
        for path in paths:
            if path in self._scores_by_path:
                result.append(self._scores_by_path[path])
        return result

    def top_risky(self, limit: int = 20) -> List[FileRiskScore]:
        """Return the top risky files.

        Args:
            limit: Maximum number of results to return

        Returns:
            List of FileRiskScore objects sorted by risk descending
        """
        scores = list(self._scores_by_path.values())
        scores.sort(key=lambda s: -s.risk_score)
        return scores[:limit]

    def persist(self, path: Optional[str] = None) -> str:
        """Export risk scores to an explicit JSON path for diagnostics.

        Args:
            path: Required output path. Risk scoring has no default local
                  artifact authority.

        Returns:
            Path where scores were written
        """
        if path is None:
            raise ValueError("risk score export requires an explicit path")

        output_dir = Path(path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        scores = list(self._scores_by_path.values())
        payload = {
            "kind": "risk_scores",
            "computed_at": _utc_now().isoformat(),
            "scores": [s.to_json() for s in scores],
            "summary": {
                "total_files": len(scores),
                "high_risk_count": len([s for s in scores if s.risk_score >= 70]),
                "medium_risk_count": len([s for s in scores if 40 <= s.risk_score < 70]),
                "low_risk_count": len([s for s in scores if s.risk_score < 40]),
            },
        }

        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.write("\n")

        _log.info(f"persisted risk scores to {path}")
        return path

    @staticmethod
    def load(path: Optional[str] = None) -> Dict[str, Any]:
        """Load an explicitly exported risk-score diagnostic artifact.

        Args:
            path: Required input path

        Returns:
            Loaded risk scores dict (with 'scores' key)
        """
        if path is None:
            raise ValueError("risk score load requires an explicit path")

        if not os.path.exists(path):
            return {"kind": "risk_scores", "scores": []}

        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def _get_file_size_kb(file_path: str) -> int:
        """Get file size in KB, safely.

        Args:
            file_path: Path to file

        Returns:
            File size in KB, or 0 if not found/unreadable
        """
        try:
            path = Path(file_path)
            if path.exists() and path.is_file():
                size_bytes = path.stat().st_size
                return max(1, size_bytes // 1024)
        except (OSError, ValueError):
            pass
        return 0


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------


def format_risk_table(scores: List[FileRiskScore]) -> str:
    """Format risk scores as a human-readable table.

    Args:
        scores: List of FileRiskScore objects

    Returns:
        Fixed-width table string
    """
    if not scores:
        return "No risk scores available (no receipts found)."

    lines: list[str] = []
    header = (
        f"{'file_path':<60} {'risk':>6} {'success%':>8} "
        f"{'touches':>7} {'avg_ms':>8} {'codes':>5} {'size_kb':>7}"
    )
    sep = "-" * len(header)

    lines.append(sep)
    lines.append(header)
    lines.append(sep)

    for score in scores:
        # Truncate long paths
        path_display = score.file_path
        if len(path_display) > 60:
            path_display = "..." + path_display[-57:]

        failure_code_str = ",".join(score.failure_codes[:2])  # Show first 2
        if len(score.failure_codes) > 2:
            failure_code_str += f"+{len(score.failure_codes) - 2}"

        lines.append(
            f"{path_display:<60} {score.risk_score:>6.1f} "
            f"{score.success_rate * 100:>7.1f}% {score.touch_count:>7} "
            f"{score.avg_duration_ms:>8} {failure_code_str:>5} {score.file_size_kb:>7}"
        )

    lines.append(sep)
    return "\n".join(lines)
