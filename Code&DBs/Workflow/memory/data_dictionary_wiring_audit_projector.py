"""Heartbeat module: run the wiring audit and snapshot the totals.

The audit is expensive on cold cache (scans every .py file in the
workflow tree + joins three authority tables) but cheap on warm cache
after Postgres has the query plans ready — 1-2 seconds per cycle,
which is fine for a background projector.

We DON'T auto-file bugs for findings. The operator explicitly asked
for no noise; this projector is here to keep a running count, not to
escalate. The scorecard picks up those counts and surfaces them as
platform health numbers.

Retention: each heartbeat inserts one row. After 60 days, old rows are
pruned by the same projector so the snapshot table stays small.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from runtime.data_dictionary_wiring_audit import (
    audit_code_orphan_tables,
    audit_hard_paths,
    audit_unreferenced_decisions,
)
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


_RETENTION_DAYS = 60


class DataDictionaryWiringAuditProjector(HeartbeatModule):
    """Run the three wiring audits each heartbeat and snapshot counts."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_wiring_audit_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            hp = audit_hard_paths()
            ud = audit_unreferenced_decisions(self._conn)
            co = audit_code_orphan_tables(self._conn)

            # Break hard-paths out by kind for the scorecard.
            counts = {"absolute_user_path": 0, "hardcoded_localhost": 0, "hardcoded_port": 0}
            for f in hp:
                if f.kind in counts:
                    counts[f.kind] += 1

            duration_ms = int((time.monotonic() - t0) * 1000)

            self._conn.execute(
                """
                INSERT INTO data_dictionary_wiring_audit_snapshots (
                    triggered_by,
                    hard_path_total,
                    absolute_user_paths,
                    hardcoded_localhost,
                    hardcoded_ports,
                    unreferenced_decisions,
                    code_orphan_tables,
                    duration_ms
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                "heartbeat",
                len(hp),
                counts["absolute_user_path"],
                counts["hardcoded_localhost"],
                counts["hardcoded_port"],
                len(ud),
                len(co),
                duration_ms,
            )

            # Retention
            try:
                self._conn.execute(
                    "DELETE FROM data_dictionary_wiring_audit_snapshots "
                    f"WHERE taken_at < now() - interval '{_RETENTION_DAYS} days'"
                )
            except Exception:
                pass
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = ["DataDictionaryWiringAuditProjector"]
