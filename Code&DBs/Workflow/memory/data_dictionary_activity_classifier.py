"""Auto-tag objects with activity signals from pg_stat_user_tables.

Two tag keys emitted at source=auto:

* ``activity = recent``  — table's last autoanalyze/analyze is within the
  recent window (default 7 days). A rough "something's been happening
  here" signal.
* ``activity = quiet``   — last stats timestamp is older than the quiet
  window (default 30 days). Candidate for deletion or dormancy review.

Tables in between (no signal, or timestamps between 7 and 30 days) get
no emission — absence of tag is the neutral state.

This is a demonstration that the classifications mechanism can carry
multiple *independent* tag categories. PII was one; activity is
another; future projectors can stack `ship_blocker`, `decision_gap`,
etc. without conflicting.

The projector is idempotent — re-running replaces all prior rows
tagged with this projector. Operator-layer tags are never touched.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterable

from runtime.data_dictionary_classifications import apply_projected_classifications
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


_PROJECTOR_TAG = "activity_classifier"
_RECENT_DAYS = 7
_QUIET_DAYS = 30


_SQL_ACTIVITY = """
SELECT relname AS table_name,
       GREATEST(
         COALESCE(last_autoanalyze, '1970-01-01'::timestamptz),
         COALESCE(last_analyze,     '1970-01-01'::timestamptz)
       ) AS last_seen,
       n_tup_ins + n_tup_upd + n_tup_del AS total_writes
FROM pg_stat_user_tables
WHERE schemaname = 'public'
"""


class DataDictionaryActivityClassifier(HeartbeatModule):
    """Emit activity:recent / activity:quiet tags for every public table."""

    def __init__(
        self,
        conn: Any,
        *,
        recent_days: int = _RECENT_DAYS,
        quiet_days: int = _QUIET_DAYS,
    ) -> None:
        self._conn = conn
        self._recent_days = int(recent_days)
        self._quiet_days = int(quiet_days)

    @property
    def name(self) -> str:
        return "data_dictionary_activity_classifier"

    def _known_object_kinds(self) -> set[str]:
        """Only emit tags for object_kinds the data dictionary knows about."""
        rows = self._conn.execute(
            "SELECT object_kind FROM data_dictionary_objects"
        ) or []
        return {str(r.get("object_kind") or "") for r in rows}

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            known = self._known_object_kinds()
            rows = self._conn.execute(_SQL_ACTIVITY) or []

            entries: list[dict[str, Any]] = []
            now_sql = self._conn.execute("SELECT now() AS now")
            now_val = now_sql[0]["now"] if now_sql else None

            for r in rows:
                table_name = str(r.get("table_name") or "")
                last_seen = r.get("last_seen")
                total_writes = int(r.get("total_writes") or 0)
                object_kind = f"table:{table_name}"
                if object_kind not in known:
                    continue
                if last_seen is None or now_val is None:
                    continue

                age_seconds = (now_val - last_seen).total_seconds()
                age_days = age_seconds / 86400.0

                # Never-touched tables report a 1970 epoch; those are
                # semantically "quiet", not missing data.
                if last_seen.year < 2000 and total_writes == 0:
                    tag_value = "quiet"
                elif age_days <= self._recent_days:
                    tag_value = "recent"
                elif age_days >= self._quiet_days:
                    tag_value = "quiet"
                else:
                    # Middle ground → no tag (absence is the neutral state)
                    continue

                entries.append({
                    "object_kind": object_kind,
                    "field_path": "",
                    "tag_key": "activity",
                    "tag_value": tag_value,
                    "confidence": 0.75,
                    "origin_ref": {
                        "projector": _PROJECTOR_TAG,
                        "days_since_last_seen": round(age_days, 1),
                        "total_writes": total_writes,
                    },
                })

            apply_projected_classifications(
                self._conn,
                projector_tag=_PROJECTOR_TAG,
                entries=entries,
                source="auto",
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = ["DataDictionaryActivityClassifier"]
