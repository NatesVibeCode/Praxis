"""ELO-based trust scoring for LLM providers.

Tracks reliability of (provider, model) pairs using ELO rating system from
competitive games. Baseline 1000, K-factor 32. Persists scores to disk.

Module-level singleton via get_trust_scorer().
"""

import json
import os
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING, Optional, Dict, Tuple

from runtime.workspace_paths import repo_root as workspace_repo_root

if TYPE_CHECKING:
    from .workflow import WorkflowResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _repo_root() -> Path:
    return workspace_repo_root()


def _artifacts_dir() -> Path:
    artifacts = _repo_root() / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    return artifacts


@dataclass(frozen=True)
class TrustScore:
    """Trust score for one (provider, model) pair using ELO rating."""

    provider_slug: str
    model_slug: Optional[str]
    elo_score: float
    total_runs: int
    wins: int
    losses: int
    win_rate: float
    last_updated: datetime


class TrustScorer:
    """Thread-safe ELO-based trust scorer for providers.

    Uses standard ELO formula:
      expected = 1 / (1 + 10^((1000 - current) / 400))
      new = current + K * (observed - expected)

    Baseline ELO: 1000
    K-factor: 32
    """

    def __init__(self, persistence_path: Optional[str] = None) -> None:
        """Initialize scorer.

        Args:
            persistence_path: Path to trust_scores.json. If None, uses
              artifacts/trust_scores.json in repo root.
        """
        if persistence_path:
            self._persistence_path = persistence_path
        else:
            self._persistence_path = str(_artifacts_dir() / "trust_scores.json")

        self._scores: Dict[Tuple[str, Optional[str]], TrustScore] = {}
        self._lock = threading.Lock()
        self._load_from_disk()

    def _elo_expected_win_rate(self, current_elo: float) -> float:
        """Compute expected win rate against baseline 1000.

        Args:
            current_elo: Current ELO score

        Returns:
            Expected win probability (0.0 to 1.0)
        """
        return 1.0 / (1.0 + (10.0 ** ((1000.0 - current_elo) / 400.0)))

    def _update_elo(self, current_elo: float, succeeded: bool) -> float:
        """Apply K-factor update to ELO score.

        Args:
            current_elo: Current ELO score
            succeeded: True if dispatch succeeded, False if failed

        Returns:
            New ELO score
        """
        K = 32  # K-factor
        expected = self._elo_expected_win_rate(current_elo)
        observed = 1.0 if succeeded else 0.0
        delta = K * (observed - expected)
        return current_elo + delta

    def update(self, provider_slug: str, model_slug: Optional[str], succeeded: bool) -> TrustScore:
        """Update trust score after a dispatch completes.

        Args:
            provider_slug: Provider identifier (e.g., "anthropic", "openai")
            model_slug: Model identifier (e.g., "claude-3-5-sonnet", "gpt-4")
            succeeded: True if dispatch succeeded, False if failed

        Returns:
            Updated TrustScore
        """
        key = (provider_slug, model_slug)

        with self._lock:
            # Get or initialize score
            if key in self._scores:
                old = self._scores[key]
                total_runs = old.total_runs + 1
                wins = old.wins + (1 if succeeded else 0)
                losses = old.losses + (0 if succeeded else 1)
                elo_score = self._update_elo(old.elo_score, succeeded)
            else:
                total_runs = 1
                wins = 1 if succeeded else 0
                losses = 0 if succeeded else 1
                elo_score = self._update_elo(1000.0, succeeded)

            win_rate = wins / total_runs if total_runs > 0 else 0.0

            new_score = TrustScore(
                provider_slug=provider_slug,
                model_slug=model_slug,
                elo_score=elo_score,
                total_runs=total_runs,
                wins=wins,
                losses=losses,
                win_rate=win_rate,
                last_updated=_utc_now(),
            )

            self._scores[key] = new_score
            self._persist_to_disk()

        return new_score

    def score(self, provider_slug: str, model_slug: Optional[str]) -> Optional[TrustScore]:
        """Get current trust score for a (provider, model) pair.

        Args:
            provider_slug: Provider identifier
            model_slug: Model identifier

        Returns:
            TrustScore if found, else None
        """
        key = (provider_slug, model_slug)
        with self._lock:
            return self._scores.get(key)

    def all_scores(self):
        """Get all trust scores sorted by ELO descending.

        Returns:
            List of TrustScore objects sorted by ELO (highest first)
        """
        with self._lock:
            scores = list(self._scores.values())
        # Sort by ELO descending, then by total_runs descending
        scores.sort(key=lambda s: (-s.elo_score, -s.total_runs))
        return scores

    def compute_from_receipts(self, receipts_dir: str | None = None) -> None:
        """Rebuild all trust scores from historical workflow receipts.

        Reads from Postgres, extracts (provider, model, status) tuples,
        and replays them through the ELO update logic. This gives a complete
        historical view of trust over time.

        Args:
            receipts_dir: Ignored (legacy parameter). Reads from Postgres.
        """
        from . import receipt_store

        # Load all receipts from Postgres
        records = receipt_store.list_receipts(limit=10000)
        receipts = [rec.to_dict() for rec in records]

        # Sort by timestamp to replay in order
        def _get_timestamp(r):
            return r.get("finished_at", "")

        receipts.sort(key=_get_timestamp)

        # Hold lock for entire rebuild operation
        with self._lock:
            self._scores.clear()

            # Replay each receipt through the update logic
            for r in receipts:
                provider = r.get("provider_slug", "unknown")
                model = r.get("model_slug")
                status = r.get("status", "unknown")
                succeeded = status == "succeeded"

                # Update ELO based on this historical event
                # Apply update directly within locked section
                key = (provider, model)
                if key in self._scores:
                    old = self._scores[key]
                    total_runs = old.total_runs + 1
                    wins = old.wins + (1 if succeeded else 0)
                    losses = old.losses + (0 if succeeded else 1)
                    elo_score = self._update_elo(old.elo_score, succeeded)
                else:
                    total_runs = 1
                    wins = 1 if succeeded else 0
                    losses = 0 if succeeded else 1
                    elo_score = self._update_elo(1000.0, succeeded)

                win_rate = wins / total_runs if total_runs > 0 else 0.0

                new_score = TrustScore(
                    provider_slug=provider,
                    model_slug=model,
                    elo_score=elo_score,
                    total_runs=total_runs,
                    wins=wins,
                    losses=losses,
                    win_rate=win_rate,
                    last_updated=_utc_now(),
                )

                self._scores[key] = new_score

            self._persist_to_disk()

    def _persist_to_disk(self) -> None:
        """Write current scores to disk as JSON."""
        try:
            scores_data = [asdict(s) for s in self._scores.values()]
            # Convert datetime to ISO format string for JSON serialization
            for item in scores_data:
                if isinstance(item["last_updated"], datetime):
                    item["last_updated"] = item["last_updated"].isoformat()

            # Ensure parent directory exists for custom persistence paths
            Path(self._persistence_path).parent.mkdir(parents=True, exist_ok=True)

            with open(self._persistence_path, "w", encoding="utf-8") as fh:
                json.dump(scores_data, fh, indent=2)
                fh.write("\n")
        except Exception as exc:
            # Never fail a dispatch due to persistence issues
            import sys
            print(
                f"[trust_scoring] persist failed: {exc}",
                file=sys.stderr,
            )

    def _load_from_disk(self) -> None:
        """Load scores from disk if they exist."""
        if not os.path.exists(self._persistence_path):
            return

        try:
            with open(self._persistence_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)

            with self._lock:
                for item in data:
                    # Parse datetime from ISO format
                    last_updated_str = item.get("last_updated")
                    if isinstance(last_updated_str, str):
                        last_updated = datetime.fromisoformat(last_updated_str)
                    else:
                        last_updated = _utc_now()

                    score = TrustScore(
                        provider_slug=item["provider_slug"],
                        model_slug=item.get("model_slug"),
                        elo_score=float(item["elo_score"]),
                        total_runs=int(item["total_runs"]),
                        wins=int(item["wins"]),
                        losses=int(item["losses"]),
                        win_rate=float(item["win_rate"]),
                        last_updated=last_updated,
                    )
                    key = (score.provider_slug, score.model_slug)
                    self._scores[key] = score
        except Exception as exc:
            import sys
            print(
                f"[trust_scoring] load from disk failed: {exc}",
                file=sys.stderr,
            )


_TRUST_SCORER = TrustScorer()


def get_trust_scorer() -> TrustScorer:
    """Return the module-level singleton TrustScorer."""
    return _TRUST_SCORER


def format_trust_scores(scores):
    """Pretty-print trust scores as a fixed-width table.

    Args:
        scores: List of TrustScore objects

    Returns:
        Formatted table string
    """
    if not scores:
        return "No trust scores found."

    header = (
        f"{'provider/model':<35} {'ELO':>8} {'runs':>6} {'W-L':>10} "
        f"{'win%':>6} {'updated':>19}"
    )
    sep = "-" * len(header)
    lines = [header, sep]

    for s in scores:
        label = f"{s.provider_slug}/{s.model_slug or 'default'}"
        w_l = f"{s.wins}-{s.losses}"
        win_pct = f"{s.win_rate * 100:.1f}%"
        updated = s.last_updated.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(
            f"{label:<35} {s.elo_score:>8.1f} {s.total_runs:>6} {w_l:>10} "
            f"{win_pct:>6} {updated:>19}"
        )

    return "\n".join(lines)
