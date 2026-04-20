"""Regression pin for BUG-186B78D0.

Before the fix, ``runtime.failure_projection.project_failure_classification``
had an asymmetric guard:

    if classification.get("category") == "unknown" and normalized_category != "unknown":
        return None

A failure_category like ``"some.unrecognized.code"`` caused the classifier to
return ``category="unknown"``, the guard matched (``normalized_category`` was
something else), and the projection correctly returned ``None`` — read
surfaces rendered "classification unavailable". But when the worker stored
the literal sentinel string ``"unknown"`` as the failure_category, the guard
did NOT match: the classifier still returned ``category="unknown"`` with
``severity="low"``, line 40 overwrote the category back to ``"unknown"``, and
the projection returned a fabricated low-severity classification card.

Dashboards and the MCP health surface read this card and showed
``severity=low`` for an unclassified failure — a known operator trap: "low
severity" looks routine and gets deprioritized, even though the actual
failure semantics are unknown and could be arbitrarily high.

The fix collapses both paths: ANY ``category="unknown"`` from the classifier
triggers ``None``, regardless of the input category. Read surfaces treat
``None`` as "classification unavailable" (they already do — they check
``if classification:`` or ``(projection or {})``).

Pins:

1. Unrecognized code — unchanged behavior, returns ``None``.
2. Literal ``"unknown"`` input — NEW: returns ``None`` (used to return a
   fake low-severity card).
3. Empty input — unchanged, returns ``None``.
4. Recognized code — passes through with the real classification.
5. Classifier exception — unchanged, returns ``None``.
"""
from __future__ import annotations

import pytest

from runtime.failure_projection import project_failure_classification


def test_unrecognized_failure_category_fails_closed() -> None:
    """Baseline: unrecognized code was already None; pin it so the fix
    doesn't accidentally regress this path."""
    assert (
        project_failure_classification(
            failure_category="some.code.the.classifier.does.not.know",
            stdout_preview="",
        )
        is None
    )


def test_literal_unknown_failure_category_fails_closed() -> None:
    """The core BUG-186B78D0 pin: the literal string ``"unknown"`` must not
    fabricate a ``severity="low"`` classification.

    Before the fix this returned a dict like
    ``{"category": "unknown", "severity": "low", "is_retryable": False, ...}``
    — a fake that read surfaces rendered as a routine low-severity card.
    After the fix both the "unknown" literal and any other unrecognized code
    collapse to ``None``.
    """
    got = project_failure_classification(
        failure_category="unknown",
        stdout_preview="something went wrong but we do not know what",
    )
    assert got is None, (
        "projection must return None for the literal 'unknown' category "
        "instead of fabricating a severity=low classification"
    )


def test_empty_failure_category_fails_closed() -> None:
    """Unchanged: empty/whitespace inputs return None (pre-fix path)."""
    assert project_failure_classification(failure_category="", stdout_preview="") is None
    assert project_failure_classification(failure_category="   ", stdout_preview="") is None


def test_recognized_failure_category_returns_real_classification() -> None:
    """Sanity check: a classifier-recognized code still passes through.

    ``dispatch.timeout`` is a canonical timeout code in the classifier's
    registry — it must resolve to the TIMEOUT category with its real
    severity, not get dropped by an overcorrection.
    """
    got = project_failure_classification(
        failure_category="dispatch.timeout",
        stdout_preview="operation timed out after 30s",
    )
    assert got is not None
    assert got["category"] == "dispatch.timeout"
    # The real classifier returns severity != "low" for timeout.
    assert got.get("severity") != "low"


def test_classifier_import_error_fails_closed(monkeypatch) -> None:
    """If the classifier can't be imported at all, projection still
    returns None — no silent default card."""
    import runtime.failure_projection as proj_mod

    # Force the classifier import to raise. The projection currently does
    # ``from runtime.failure_classifier import classify_failure`` inside the
    # try block, so monkeypatching sys.modules before the call makes the
    # import raise.
    import sys

    monkeypatch.setitem(sys.modules, "runtime.failure_classifier", None)
    # Reimport proj_mod symbol via its public entry so we exercise the
    # try/except inside the function body.
    got = proj_mod.project_failure_classification(
        failure_category="dispatch.timeout",
        stdout_preview="",
    )
    assert got is None
