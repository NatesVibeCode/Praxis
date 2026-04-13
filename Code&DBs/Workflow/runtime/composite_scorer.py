"""Composite scorer: weighted multi-factor scoring with pluggable scale functions."""

import math
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple


class ScaleFn(Enum):
    """Scale functions that map raw values into [0, 1]."""

    LINEAR = "linear"
    SIGMOID = "sigmoid"
    BUCKET = "bucket"
    LOGARITHMIC = "logarithmic"


def _apply_scale(fn: ScaleFn, raw: float) -> float:
    """Apply a scale function to a raw value, returning a float in [0, 1]."""
    if fn is ScaleFn.LINEAR:
        return max(0.0, min(1.0, float(raw)))

    if fn is ScaleFn.SIGMOID:
        # Logistic curve centred at 0.5 with moderate steepness.
        # Maps raw 0->~0.018, 0.5->0.5, 1->~0.982
        k = 8.0
        return 1.0 / (1.0 + math.exp(-k * (raw - 0.5)))

    if fn is ScaleFn.BUCKET:
        # Three-step threshold: <0.33 -> 0.0, <0.66 -> 0.5, >=0.66 -> 1.0
        if raw < 0.33:
            return 0.0
        if raw < 0.66:
            return 0.5
        return 1.0

    if fn is ScaleFn.LOGARITHMIC:
        # log(1 + 9x) / log(10)  -- maps 0->0, 1->1 with log compression
        clamped = max(0.0, min(1.0, float(raw)))
        return math.log10(1.0 + 9.0 * clamped)

    raise ValueError(f"Unknown scale function: {fn}")


@dataclass(frozen=True)
class ScoringFactor:
    """A single evaluated scoring factor with its raw and scaled values."""

    name: str
    weight: float
    scale_fn: ScaleFn
    required: bool
    raw_value: Optional[float]
    scaled_value: Optional[float]


@dataclass(frozen=True)
class ScoreResult:
    """Final composite score with full transparency into per-factor breakdown."""

    total_score: float
    factors: Tuple[ScoringFactor, ...]
    completeness_ratio: float
    missing_factors: Tuple[str, ...]


class CompositeScorer:
    """Weighted multi-factor scorer with pluggable scale functions.

    Fails closed: if any *required* factor is missing from the score() call
    the scorer raises ValueError rather than producing a partial score.
    """

    _WEIGHT_EPSILON = 0.01

    def __init__(
        self,
        factor_defs: List[Tuple[str, float, ScaleFn, bool]],
    ) -> None:
        if not factor_defs:
            raise ValueError("At least one factor definition is required")

        self._defs: Dict[str, Tuple[float, ScaleFn, bool]] = {}
        total_weight = 0.0

        for name, weight, scale_fn, required in factor_defs:
            if name in self._defs:
                raise ValueError(f"Duplicate factor name: {name}")
            self._defs[name] = (weight, scale_fn, required)
            total_weight += weight

        if abs(total_weight - 1.0) > self._WEIGHT_EPSILON:
            raise ValueError(
                f"Weights must sum to 1.0 (got {total_weight:.4f})"
            )

    def score(self, **kwargs: float) -> ScoreResult:
        """Score a set of named factor values.

        Args:
            **kwargs: factor_name=raw_value pairs. Values should be in [0, 1].

        Returns:
            ScoreResult with the weighted composite score and full breakdown.

        Raises:
            ValueError: if a required factor is missing.
        """
        factors: List[ScoringFactor] = []
        missing: List[str] = []
        weighted_sum = 0.0
        provided_count = 0

        for name, (weight, scale_fn, required) in self._defs.items():
            if name in kwargs:
                raw = float(kwargs[name])
                scaled = _apply_scale(scale_fn, raw)
                weighted_sum += weight * scaled
                provided_count += 1
                factors.append(
                    ScoringFactor(
                        name=name,
                        weight=weight,
                        scale_fn=scale_fn,
                        required=required,
                        raw_value=raw,
                        scaled_value=scaled,
                    )
                )
            else:
                if required:
                    raise ValueError(
                        f"Required factor '{name}' is missing"
                    )
                missing.append(name)
                factors.append(
                    ScoringFactor(
                        name=name,
                        weight=weight,
                        scale_fn=scale_fn,
                        required=required,
                        raw_value=None,
                        scaled_value=None,
                    )
                )

        total_count = len(self._defs)
        completeness = provided_count / total_count if total_count else 0.0

        return ScoreResult(
            total_score=weighted_sum,
            factors=tuple(factors),
            completeness_ratio=completeness,
            missing_factors=tuple(missing),
        )
