"""Focus-Aware Scoring with Temporal Decay."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Sequence


@dataclass(frozen=True)
class DecayProfile:
    entity_type: str
    half_life_days: float = 30.0
    never_decay: bool = False


# Default profiles per the spec
_DEFAULT_PROFILES: list[DecayProfile] = [
    DecayProfile(entity_type="constraint", never_decay=True),
    DecayProfile(entity_type="decision", never_decay=True),
    DecayProfile(entity_type="task", half_life_days=10.0),
    DecayProfile(entity_type="fact", half_life_days=30.0),
    DecayProfile(entity_type="lesson", half_life_days=60.0),
    DecayProfile(entity_type="topic", half_life_days=45.0),
    DecayProfile(entity_type="person", half_life_days=90.0),
    DecayProfile(entity_type="document", half_life_days=30.0),
    DecayProfile(entity_type="pattern", never_decay=True),
]


@dataclass(frozen=True)
class FocusBoost:
    entity_id: str
    boost_factor: float
    reason: str


class FocusScorer:
    """Score entities with temporal decay and active-focus boosting."""

    def __init__(
        self,
        profiles: list[DecayProfile] | None = None,
        active_focus: list[FocusBoost] | None = None,
    ) -> None:
        raw = profiles if profiles is not None else list(_DEFAULT_PROFILES)
        self._profiles: dict[str, DecayProfile] = {p.entity_type: p for p in raw}
        self._focus: dict[str, FocusBoost] = {}
        if active_focus:
            self._focus = {b.entity_id: b for b in active_focus}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score(
        self,
        entity_id: str,
        entity_type: str,
        base_score: float,
        updated_at: datetime,
        now: datetime | None = None,
    ) -> float:
        if now is None:
            now = datetime.now(timezone.utc)

        profile = self._profiles.get(entity_type)

        # Temporal decay
        if profile and profile.never_decay:
            decayed = base_score
        else:
            age_days = max(0.0, (now - updated_at).total_seconds() / 86400.0)
            half_life = profile.half_life_days if profile else 30.0
            decay = math.exp(-math.log(2) * age_days / half_life)
            decayed = base_score * decay

        # Focus boost
        boost = self._focus.get(entity_id)
        if boost:
            decayed *= 1.0 + boost.boost_factor

        return decayed

    def batch_score(self, entities: list[dict]) -> list[tuple[str, float]]:
        """Score many entities and return sorted descending by score.

        Each dict must contain: entity_id, entity_type, base_score, updated_at.
        Optionally: now.
        """
        results: list[tuple[str, float]] = []
        for ent in entities:
            s = self.score(
                entity_id=ent["entity_id"],
                entity_type=ent["entity_type"],
                base_score=ent["base_score"],
                updated_at=ent["updated_at"],
                now=ent.get("now"),
            )
            results.append((ent["entity_id"], s))
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def set_focus(self, boosts: list[FocusBoost]) -> None:
        """Replace the active focus list."""
        self._focus = {b.entity_id: b for b in boosts}
