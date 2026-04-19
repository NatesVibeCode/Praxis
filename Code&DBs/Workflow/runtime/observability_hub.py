"""Observability hub: single coordinator wiring quality views, bug tracker, and operator panel.

Ingests workflow receipts, materializes quality rollups, and maintains operator
panel state. Workflow-result bug filing is owned by ``runtime.receipt_store``;
the hub keeps a manual bug-tracker passthrough but does not auto-file from
receipts.

Uses importlib-based direct file imports to avoid triggering the runtime
package __init__.py (which requires Python 3.10+ features).
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection


# ---------------------------------------------------------------------------
# Direct-file import helper (bypasses runtime/__init__.py)
# ---------------------------------------------------------------------------

_RUNTIME_DIR = str(Path(__file__).resolve().parent)


def _direct_import(module_name: str, file_name: str):
    """Import a sibling module by filename, bypassing package __init__."""
    key = f"runtime.{module_name}"
    if key in sys.modules:
        return sys.modules[key]
    path = os.path.join(_RUNTIME_DIR, file_name)
    spec = importlib.util.spec_from_file_location(key, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[key] = mod
    spec.loader.exec_module(mod)
    return mod


_quality_views = _direct_import("quality_views", "quality_views.py")
_bug_tracker = _direct_import("bug_tracker", "bug_tracker.py")
_operator_panel = _direct_import("operator_panel", "operator_panel.py")
_health_mod = _direct_import("health", "health.py")

QualityViewMaterializer = _quality_views.QualityViewMaterializer
QualityWindow = _quality_views.QualityWindow
QualityRollup = _quality_views.QualityRollup

BugTracker = _bug_tracker.BugTracker
BugSeverity = _bug_tracker.BugSeverity
BugCategory = _bug_tracker.BugCategory

OperatorPanel = _operator_panel.OperatorPanel
OperatorSnapshot = _operator_panel.OperatorSnapshot

HealthProbe = _health_mod.HealthProbe
PreflightRunner = _health_mod.PreflightRunner
PreflightResult = _health_mod.PreflightResult


# ---------------------------------------------------------------------------
# ReceiptIngester — loads receipt JSON files from a directory
# ---------------------------------------------------------------------------


class ReceiptIngester:
    """Loads and summarizes workflow receipt JSON files from a directory."""

    def __init__(self, receipts_dir: str) -> None:
        self._receipts_dir = receipts_dir

    def load_recent(self, since_hours: int = 1) -> list:
        """Load receipt JSON files modified within the last *since_hours*."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        results: list = []
        if not os.path.isdir(self._receipts_dir):
            return results
        for name in os.listdir(self._receipts_dir):
            if not name.endswith(".json"):
                continue
            path = os.path.join(self._receipts_dir, name)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
                if mtime < cutoff:
                    continue
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                results.append(data)
            except Exception:
                continue
        return results

    @staticmethod
    def compute_pass_rate(receipts: list) -> float:
        """Return the fraction of receipts with status 'succeeded'."""
        if not receipts:
            return 0.0
        succeeded = sum(1 for r in receipts if r.get("status") == "succeeded")
        return succeeded / len(receipts)

    @staticmethod
    def top_failure_codes(receipts: list, limit: int = 10) -> dict:
        """Return a dict of failure_code -> count, most frequent first."""
        counts: dict = defaultdict(int)
        for r in receipts:
            if r.get("status") != "succeeded":
                code = r.get("failure_code")
                if code:
                    counts[code] += 1
        # Sort by count descending
        sorted_items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_items[:limit])


class ObservabilityHub:
    """Single coordinator that wires quality views, bug tracker, and operator panel."""

    def __init__(self, conn: "SyncPostgresConnection") -> None:
        self._conn = conn

        # Lazy-initialized subsystems
        self._quality = None
        self._bug_tracker_inst = None
        self._panel = None

        # In-memory failure code counter for auto-bug-filing
        self._failure_code_counts: dict = defaultdict(int)
        self._failure_category_counts: dict[str, int] = defaultdict(int)
        self._recent_receipts: deque[dict[str, Any]] = deque(maxlen=50)

    # -- lazy initialization ------------------------------------------------

    def _get_quality(self):
        if self._quality is None:
            self._quality = QualityViewMaterializer(self._conn)
        return self._quality

    def _get_bug_tracker(self):
        if self._bug_tracker_inst is None:
            self._bug_tracker_inst = BugTracker(self._conn)
        return self._bug_tracker_inst

    def _get_panel(self):
        if self._panel is None:
            self._panel = OperatorPanel()
        return self._panel

    # -- receipt ingestion --------------------------------------------------

    def ingest_receipt(self, receipt: dict) -> None:
        """Feed a workflow receipt into quality materializer and operator panel."""
        # (a) Feed into quality materializer
        self._get_quality().ingest_receipt(receipt)

        # (b) Track failure signals for the operator panel.
        status = receipt.get("status", "")
        failure_code = receipt.get("failure_code")
        failure_category = str(receipt.get("failure_category") or "").strip()
        parent_run_id = receipt.get("parent_run_id")
        run_id = receipt.get("run_id")
        timestamp_raw = receipt.get("timestamp")
        timestamp = None
        if isinstance(timestamp_raw, str):
            try:
                timestamp = datetime.fromisoformat(timestamp_raw)
            except ValueError:
                timestamp = None

        self._recent_receipts.append(dict(receipt))
        if failure_category:
            self._failure_category_counts[failure_category] += 1

        panel = self._get_panel()
        if run_id:
            panel.register_last_run_id(str(run_id))
        panel.register_last_failure_category(failure_category or None)
        panel.register_last_activity_at(timestamp)
        panel.register_failure_categories(dict(self._failure_category_counts))
        if parent_run_id:
            try:
                panel.register_lineage_depth(1)
            except Exception:
                pass

        if status != "succeeded" and failure_code:
            self._failure_code_counts[failure_code] += 1
            panel.register_failure_codes(dict(self._failure_code_counts))

    # -- operator panel -----------------------------------------------------

    def refresh_operator_panel(
        self,
        circuit_breakers: dict,
        loop_warnings: int,
        write_conflicts: int,
        governance_blocks: int,
        pending_jobs: int,
        running_jobs: int,
        active_leases: int,
        posture: str,
        *,
        recent_failure_categories: dict[str, int] | None = None,
        recent_lineage_depth: int | None = None,
        last_run_id: str | None = None,
        last_failure_category: str | None = None,
        last_activity_at: datetime | None = None,
    ):
        """Push all subsystem signals into the panel and return a snapshot."""
        panel = self._get_panel()
        panel.register_circuit_breakers(circuit_breakers)
        panel.register_loop_warnings(loop_warnings)
        panel.register_write_conflicts(write_conflicts)
        panel.register_governance_blocks(governance_blocks)
        panel.register_job_counts(pending_jobs, running_jobs)
        panel.register_lease_count(active_leases)
        panel.register_posture(posture)
        latest_receipt = self._recent_receipts[-1] if self._recent_receipts else None
        fallback_timestamp = None
        fallback_failure_category = None
        fallback_run_id = None
        fallback_lineage_depth = 0
        if latest_receipt and isinstance(latest_receipt.get("timestamp"), str):
            try:
                fallback_timestamp = datetime.fromisoformat(str(latest_receipt["timestamp"]))
            except ValueError:
                fallback_timestamp = None
        if latest_receipt:
            if latest_receipt.get("run_id"):
                fallback_run_id = str(latest_receipt.get("run_id"))
            if latest_receipt.get("failure_category"):
                fallback_failure_category = str(latest_receipt.get("failure_category"))
            if latest_receipt.get("parent_run_id"):
                fallback_lineage_depth = 1

        panel.register_failure_categories(
            recent_failure_categories
            if recent_failure_categories is not None
            else dict(self._failure_category_counts)
        )
        panel.register_lineage_depth(
            recent_lineage_depth
            if recent_lineage_depth is not None
            else fallback_lineage_depth
        )
        panel.register_last_run_id(
            last_run_id if last_run_id is not None else fallback_run_id
        )
        panel.register_last_failure_category(
            last_failure_category if last_failure_category is not None else fallback_failure_category
        )
        panel.register_last_activity_at(
            last_activity_at if last_activity_at is not None else fallback_timestamp
        )
        return panel.snapshot()

    def operator_snapshot(self):
        """Return the current operator panel snapshot without updating signals."""
        return self._get_panel().snapshot()

    # -- quality materialization --------------------------------------------

    def materialize_quality(self, window: str, window_start: datetime):
        """Trigger quality materialization for the given window."""
        qw = QualityWindow(window)
        return self._get_quality().materialize(qw, window_start)

    # -- bug tracker passthrough --------------------------------------------

    def file_bug(
        self,
        title: str,
        severity: str,
        category: str,
        description: str,
        filed_by: str,
        *,
        source_kind: str = "manual",
        decision_ref: str = "",
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        owner_ref: str | None = None,
        source_issue_id: str | None = None,
        tags: tuple[str, ...] = (),
        resume_context: dict[str, Any] | None = None,
    ):
        """File a bug through the tracker."""
        normalized_severity = BugTracker._normalize_severity(
            severity, default=BugSeverity.P2
        )
        normalized_category = BugTracker._normalize_category(
            category, default=BugCategory.OTHER
        )
        bug, _similar_bugs = self._get_bug_tracker().file_bug(
            title=title,
            severity=normalized_severity,
            category=normalized_category,
            description=description,
            filed_by=filed_by,
            source_kind=source_kind,
            decision_ref=decision_ref,
            discovered_in_run_id=discovered_in_run_id,
            discovered_in_receipt_id=discovered_in_receipt_id,
            owner_ref=owner_ref,
            source_issue_id=source_issue_id,
            tags=tags,
            resume_context=resume_context,
        )
        return bug

    def get_bugs(
        self,
        status: str = None,
        severity: str = None,
        category: str | None = None,
        title_like: str | None = None,
        tags: tuple[str, ...] = (),
        exclude_tags: tuple[str, ...] = (),
        source_issue_id: str | None = None,
        open_only: bool = False,
        limit: int = 50,
    ) -> list:
        """List bugs with optional filters."""
        bt = self._get_bug_tracker()
        normalized_status = BugTracker._normalize_status(status, default=None) if status else None
        normalized_severity = (
            BugTracker._normalize_severity(severity, default=None) if severity else None
        )
        normalized_category = (
            BugTracker._normalize_category(category, default=None) if category else None
        )
        return bt.list_bugs(
            status=normalized_status,
            severity=normalized_severity,
            category=normalized_category,
            title_like=title_like,
            tags=tags,
            exclude_tags=exclude_tags,
            source_issue_id=source_issue_id,
            open_only=open_only,
            limit=limit,
        )

    # -- health checks ------------------------------------------------------

    def health_check(self, probes: list):
        """Run health probes and return aggregated result."""
        runner = PreflightRunner(probes)
        return runner.run()
