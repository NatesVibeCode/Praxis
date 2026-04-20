"""Projections for canonical failure semantics.

This module keeps read surfaces on one rule: project failure semantics from the
canonical failure category, and do not silently repair missing data from legacy
compatibility fields.
"""

from __future__ import annotations

from typing import Any

__all__ = ["project_failure_classification"]


def project_failure_classification(
    *,
    failure_category: str,
    is_transient: bool = False,
    stdout_preview: str = "",
) -> dict[str, Any] | None:
    """Project one job failure classification from canonical failure semantics.

    Returns ``None`` when the classifier cannot produce a meaningful
    classification. Read surfaces treat ``None`` as "classification
    unavailable" and render accordingly — they do not substitute a default.

    BUG-186B78D0: previously this function treated the sentinel string
    ``"unknown"`` as a legitimate category and returned the classifier's
    ``severity="low"`` fallback card for it, while any other unrecognized
    code (where the classifier also returned ``category="unknown"``) fell
    through to ``None``. Operators looking at dashboards saw ``severity=low``
    for explicitly-unknown failures and (reasonably) dismissed them as
    routine noise, even though the actual severity is unclassified and
    could be arbitrarily high. Fix: an unknown classification fails closed
    regardless of whether the input was literally ``"unknown"`` or some
    other unrecognized code.
    """

    normalized_category = str(failure_category or "").strip()
    if not normalized_category:
        return None

    try:
        from runtime.failure_classifier import classify_failure

        classification = classify_failure(
            normalized_category,
            outputs={"stderr": str(stdout_preview or "")},
        ).to_dict()
    except Exception:
        return None

    # Fail-closed when the classifier couldn't actually classify. The old
    # asymmetric guard exempted the literal string "unknown" and returned a
    # fabricated low-severity card; now both paths collapse to the same
    # "classification unavailable" signal.
    if classification.get("category") == "unknown":
        return None

    classification["category"] = normalized_category
    return classification
