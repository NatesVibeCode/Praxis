"""Cron-backed workflow trigger scheduler."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import TYPE_CHECKING

from runtime.system_events import emit_system_event

try:
    from croniter import croniter as _croniter
except Exception:  # pragma: no cover - optional dependency
    _croniter = None

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

_NICKNAME_INTERVALS = {
    "@hourly": timedelta(hours=1),
    "@daily": timedelta(days=1),
    "@weekly": timedelta(weeks=1),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def _derive_interval(cron_expression: str) -> timedelta | None:
    expr = (cron_expression or "").strip()
    if not expr:
        return None
    if expr in _NICKNAME_INTERVALS:
        return _NICKNAME_INTERVALS[expr]

    fields = expr.split()
    if len(fields) != 5:
        return None

    minute = fields[0]
    if minute.startswith("*/") and fields[1:] == ["*", "*", "*", "*"]:
        try:
            minutes = int(minute[2:])
        except ValueError:
            return None
        if minutes <= 0:
            return None
        return timedelta(minutes=minutes)

    return None


def _is_due(
    cron_expression: str,
    *,
    last_fired_at: object,
    now: datetime | None = None,
) -> bool:
    current_time = _utc_now() if now is None else _normalize_datetime(now) or _utc_now()
    normalized_last_fired = _normalize_datetime(last_fired_at)
    if normalized_last_fired is None:
        return True

    expr = (cron_expression or "").strip()
    if _croniter is not None:
        try:
            next_fire = _croniter(expr, normalized_last_fired).get_next(datetime)
            next_fire = _normalize_datetime(next_fire) or next_fire
            return next_fire <= current_time
        except Exception:
            logger.debug("croniter could not parse cron expression: %s", expr, exc_info=True)

    interval = _derive_interval(expr)
    if interval is None:
        logger.debug("unsupported cron expression without croniter: %s", expr)
        return False
    return normalized_last_fired <= current_time - interval


class CronScheduler:
    """Evaluates schedule triggers and emits system events when due."""

    def __init__(self, conn: "SyncPostgresConnection") -> None:
        self._conn = conn

    def tick(self) -> int:
        fired = 0
        now = _utc_now()
        try:
            emit_system_event(
                self._conn,
                event_type="scheduler.tick",
                source_id="workflow_triggers",
                source_type="cron_scheduler",
                payload={"ticked_at": now.isoformat()},
            )
        except Exception:
            logger.debug("scheduler.tick emission unavailable", exc_info=True)
        try:
            triggers = self._conn.execute(
                "SELECT id, workflow_id, cron_expression, last_fired_at "
                "FROM workflow_triggers "
                "WHERE event_type = 'schedule' "
                "  AND cron_expression IS NOT NULL "
                "  AND enabled = TRUE"
            )
            for trigger in triggers or []:
                try:
                    trigger_id = trigger["id"]
                    workflow_id = trigger["workflow_id"]
                    cron_expression = (trigger.get("cron_expression") or "").strip()
                    if not cron_expression:
                        continue
                    if not _is_due(
                        cron_expression,
                        last_fired_at=trigger.get("last_fired_at"),
                        now=now,
                    ):
                        continue

                    emit_system_event(
                        self._conn,
                        event_type="schedule.fired",
                        source_id=str(trigger_id),
                        source_type="workflow_trigger",
                        payload={
                            "trigger_id": trigger_id,
                            "workflow_id": workflow_id,
                            "cron_expression": cron_expression,
                        },
                    )
                    self._conn.execute(
                        "UPDATE workflow_triggers "
                        "SET last_fired_at = NOW(), fire_count = fire_count + 1 "
                        "WHERE id = $1",
                        trigger_id,
                    )
                    fired += 1
                except Exception:
                    logger.exception("cron scheduler trigger failed: trigger_id=%s", trigger.get("id"))
        except Exception:
            logger.exception("cron scheduler tick failed")
        return fired
