"""Heartbeat module: drain the governance change-feed ledger.

Each cycle, reads unprocessed rows from
`data_dictionary_governance_change_ledger`, scans affected objects,
files governance bugs for newly surfaced violations, and marks the
ledger rows processed.

This runs BEFORE the full governance scan module, so the full scan
sees an already-mostly-clean ledger. It also gives near-real-time
reaction: a stewardship change that clears a violation is reflected in
the scorecard on the next cycle (typically <1 minute in practice).
"""
from __future__ import annotations

import time
from typing import Any

from runtime.bug_tracker import BugTracker
from runtime.data_dictionary_governance_change_feed import drain_change_feed
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok


class DataDictionaryGovernanceChangeFeedModule(HeartbeatModule):
    """Drain the governance change-feed once per heartbeat cycle."""

    def __init__(self, conn: Any, *, batch_limit: int = 100) -> None:
        self._conn = conn
        self._batch_limit = max(1, int(batch_limit))

    @property
    def name(self) -> str:
        return "data_dictionary_governance_change_feed"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            tracker = BugTracker(self._conn)
            drain_change_feed(
                self._conn,
                tracker=tracker,
                limit=self._batch_limit,
                triggered_by="change_feed",
            )
        except Exception as exc:
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = ["DataDictionaryGovernanceChangeFeedModule"]
