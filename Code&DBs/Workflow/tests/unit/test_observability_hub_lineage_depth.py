"""Regression pin for BUG-C19968ED.

Before the fix, ``runtime.observability_hub.ObservabilityHub`` collapsed the
authoritative lineage depth into a binary parent-present signal at two sites:

* ``ingest_receipt`` — when the receipt carried ``parent_run_id`` but no
  ``lineage_depth`` (top-level or nested under ``lineage``), the hub wrote
  ``lineage_depth = 1`` into the operator panel. A true grandchild (depth 2)
  or deeper descendant that somehow lost the stamp showed up as "depth 1"
  in the panel, indistinguishable from a real direct child.
* ``refresh_operator_panel`` — the fallback path had the same ``= 1``
  synthesis when deriving from the most recent receipt.

The authoritative depth is computed in
``runtime.workflow._routing._build_request_envelope`` and stamped onto the
request envelope as both ``lineage_depth`` and ``lineage.lineage_depth``.
Receipts that carry either of those represent the truth; a receipt that
lacks both is a producer-side omission and must not be silently papered
over with a fabricated 1.

Pins:

1. Receipt with top-level ``lineage_depth=3`` → panel reports 3.
2. Receipt with nested ``lineage.lineage_depth=2`` → panel reports 2.
3. Receipt with no depth and no parent → panel stays at 0.
4. Receipt with ``parent_run_id`` but NO stamped depth → panel does NOT
   fabricate 1; it keeps the previous depth (the core BUG-C19968ED pin).
5. ``refresh_operator_panel`` fallback respects the stamped depth and
   returns 0 (not 1) when the latest receipt has a parent but no depth.
"""
from __future__ import annotations

from typing import Any

import pytest


class _NoopQuality:
    """Stand-in for QualityViewMaterializer. The hub calls
    ``_get_quality().ingest_receipt(receipt)`` inside ``ingest_receipt`` —
    these tests exercise the lineage-depth path, not quality rollups, so a
    no-op is enough."""

    def ingest_receipt(self, _receipt: Any) -> None:
        return None


def _make_hub():
    """Construct an ObservabilityHub without a real DB connection.

    The hub is lazy about its subsystems — operator panel is only touched
    via ``_get_panel`` and doesn't need the conn. We stub the quality
    subsystem so ``ingest_receipt`` doesn't try to materialize against a
    real DB.
    """
    from runtime.observability_hub import ObservabilityHub

    hub = ObservabilityHub(None)  # type: ignore[arg-type]
    hub._quality = _NoopQuality()  # type: ignore[attr-defined]
    return hub


def test_receipt_top_level_lineage_depth_is_threaded_through() -> None:
    hub = _make_hub()
    hub.ingest_receipt(
        {
            "run_id": "run:a",
            "parent_run_id": "run:root",
            "lineage_depth": 3,
            "status": "succeeded",
        }
    )
    snap = hub.operator_snapshot()
    assert snap.recent_lineage_depth == 3


def test_receipt_nested_lineage_depth_is_threaded_through() -> None:
    """The helper already checks nested ``lineage.lineage_depth``; pin it
    so a refactor doesn't quietly remove that extraction path."""
    hub = _make_hub()
    hub.ingest_receipt(
        {
            "run_id": "run:b",
            "parent_run_id": "run:root",
            "lineage": {"lineage_depth": 2, "parent_run_id": "run:root"},
            "status": "succeeded",
        }
    )
    snap = hub.operator_snapshot()
    assert snap.recent_lineage_depth == 2


def test_root_receipt_reports_zero_depth() -> None:
    """A receipt with no parent and no stamped depth leaves the panel at
    its initial 0."""
    hub = _make_hub()
    hub.ingest_receipt({"run_id": "run:root", "status": "succeeded"})
    snap = hub.operator_snapshot()
    assert snap.recent_lineage_depth == 0


def test_receipt_with_parent_but_no_depth_does_not_fabricate_one() -> None:
    """The core BUG-C19968ED pin.

    Before the fix, any receipt with ``parent_run_id`` but no stamped
    ``lineage_depth`` was recorded as depth 1, erasing the real tree depth.
    After the fix, the panel preserves the previously-registered depth
    (here, 4 from the earlier grandchild-of-grandchild receipt) rather
    than clobbering it with a fabricated 1.
    """
    hub = _make_hub()
    hub.ingest_receipt(
        {"run_id": "run:deep", "lineage_depth": 4, "status": "succeeded"}
    )
    # Now an orphan receipt arrives with a parent but no stamped depth.
    hub.ingest_receipt(
        {
            "run_id": "run:orphan",
            "parent_run_id": "run:someone",
            "status": "succeeded",
        }
    )
    snap = hub.operator_snapshot()
    # The earlier depth-4 signal is preserved; the fix refuses to overwrite
    # it with a fabricated 1.
    assert snap.recent_lineage_depth == 4, (
        "orphan receipt with parent_run_id but no stamped lineage_depth "
        "must not overwrite the panel with a fabricated 1"
    )


def test_refresh_operator_panel_fallback_does_not_fabricate_one() -> None:
    """Even when the caller omits ``recent_lineage_depth`` and the most
    recent receipt has a parent but no stamped depth, the fallback must
    report 0 (unknown) rather than 1 (fabricated)."""
    hub = _make_hub()
    hub.ingest_receipt(
        {
            "run_id": "run:orphan",
            "parent_run_id": "run:someone",
            "status": "succeeded",
        }
    )
    snap = hub.refresh_operator_panel(
        circuit_breakers={},
        loop_warnings=0,
        write_conflicts=0,
        governance_blocks=0,
        pending_jobs=0,
        running_jobs=0,
        active_leases=0,
        posture="operate",
        recent_lineage_depth=None,  # force fallback path
    )
    assert snap.recent_lineage_depth == 0, (
        "refresh fallback must not fabricate depth=1 from parent_run_id alone"
    )


def test_refresh_operator_panel_respects_stamped_depth_in_fallback() -> None:
    """When the latest receipt does carry the authoritative depth, the
    fallback threads that depth through."""
    hub = _make_hub()
    hub.ingest_receipt(
        {
            "run_id": "run:c",
            "parent_run_id": "run:p",
            "lineage_depth": 5,
            "status": "succeeded",
        }
    )
    snap = hub.refresh_operator_panel(
        circuit_breakers={},
        loop_warnings=0,
        write_conflicts=0,
        governance_blocks=0,
        pending_jobs=0,
        running_jobs=0,
        active_leases=0,
        posture="operate",
        recent_lineage_depth=None,
    )
    assert snap.recent_lineage_depth == 5
