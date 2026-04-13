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
    """Project one job failure classification from canonical failure semantics."""

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
        classification = None

    if classification is None:
        return {
            "category": normalized_category,
            "is_retryable": not bool(is_transient),
            "is_transient": bool(is_transient),
            "recommended_action": "",
            "severity": "low",
        }

    if classification.get("category") == "unknown" and normalized_category != "unknown":
        return {
            "category": normalized_category,
            "is_retryable": not bool(is_transient),
            "is_transient": bool(is_transient),
            "recommended_action": "",
            "severity": "low",
        }

    classification["category"] = normalized_category
    return classification
