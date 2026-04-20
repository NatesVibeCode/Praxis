"""Heartbeat module: snapshot the data dictionary, diff against prior,
file governance bugs for breaking changes.

Each cycle:
  1. take_snapshot() — capture current field inventory
  2. diff against the most recent prior snapshot
  3. for every P0 / P1 impact, file (or skip-if-already-open) a
     governance bug whose decision_ref is keyed on the change so
     duplicate detection works across cycles
  4. prune snapshots older than 30 days

Bugs use decision_ref:  drift.<change_kind>.<object_kind>[.<field_path>]
This is the same dedupe key the governance projector uses, so an open
drift bug isn't re-filed on every heartbeat tick.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from runtime.data_dictionary_drift import (
    ChangeImpact,
    detect_drift,
    prune_snapshot_history,
)
from runtime.bug_tracker import BugCategory, BugSeverity, BugTracker
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok
from runtime.primitive_contracts import bug_open_status_values

logger = logging.getLogger(__name__)


_FILED_BY = "data_dictionary_drift_projector"
_SOURCE_KIND = "drift"
_CATEGORY = BugCategory.ARCHITECTURE
_SNAPSHOT_RETENTION_DAYS = 30

_SEVERITY_MAP = {
    "P0": BugSeverity.P0,
    "P1": BugSeverity.P1,
    "P2": BugSeverity.P2,
    "P3": BugSeverity.P3,
}


def _drift_decision_ref(impact: ChangeImpact) -> str:
    c = impact.change
    if c.field_path:
        return f"drift.{c.change_kind}.{c.object_kind}.{c.field_path}"
    return f"drift.{c.change_kind}.{c.object_kind}"


def _drift_bug_title(impact: ChangeImpact) -> str:
    c = impact.change
    if c.change_kind == "drop_object":
        return f"Drift: object {c.object_kind} dropped"
    if c.change_kind == "drop_field":
        return f"Drift: field {c.object_kind}.{c.field_path} dropped"
    if c.change_kind == "change_field":
        return f"Drift: field {c.object_kind}.{c.field_path} changed shape"
    if c.change_kind == "add_object":
        return f"Drift: new object {c.object_kind} appeared (unowned)"
    if c.change_kind == "add_field":
        return f"Drift: new field {c.object_kind}.{c.field_path} appeared"
    return f"Drift: {c.change_kind} on {c.object_kind}"


def _drift_bug_description(impact: ChangeImpact) -> str:
    c = impact.change
    parts = [
        f"change_kind:    {c.change_kind}",
        f"object_kind:    {c.object_kind}",
    ]
    if c.field_path:
        parts.append(f"field_path:     {c.field_path}")
    if c.before:
        parts.append(f"before:         {c.before}")
    if c.after:
        parts.append(f"after:          {c.after}")
    parts.append(f"severity:       {impact.severity}")
    parts.append(f"pii_dropped:    {impact.pii_dropped}")
    parts.append(f"downstream:     {impact.downstream_count} consumer(s)")
    parts.append(f"quality_rules:  {impact.quality_rule_count} attached")
    if impact.stewards_to_notify:
        sids = ", ".join(
            f"{s['steward_kind']}:{s['steward_id']}"
            for s in impact.stewards_to_notify[:5]
        )
        parts.append(f"notify:         {sids}")
    parts.append("")
    parts.append("Reasons:")
    for r in impact.reasons:
        parts.append(f"  - {r}")
    parts.append("")
    parts.append("Filed automatically by the schema-drift heartbeat.")
    return "\n".join(parts)


_OPEN_STATUSES = bug_open_status_values()


def _existing_open_bug_id(conn: Any, decision_ref: str) -> str | None:
    rows = conn.execute(
        "SELECT bug_id FROM bugs "
        "WHERE decision_ref = $1 AND status = ANY($2) "
        "ORDER BY opened_at DESC LIMIT 1",
        decision_ref,
        list(_OPEN_STATUSES),
    )
    if rows:
        return str(rows[0]["bug_id"])
    return None


def _file_bugs(
    conn: Any, tracker: BugTracker, impacts: list[ChangeImpact],
) -> dict[str, Any]:
    """File a bug per high-severity impact; dedupe against open ones."""
    filed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for impact in impacts:
        if impact.severity not in ("P0", "P1"):
            continue
        ref = _drift_decision_ref(impact)
        try:
            existing = _existing_open_bug_id(conn, ref)
        except Exception as exc:
            errors.append({"decision_ref": ref, "error": f"dedup: {exc}"})
            continue
        if existing:
            skipped.append({"decision_ref": ref, "bug_id": existing})
            continue

        try:
            assignee = (
                impact.stewards_to_notify[0]["steward_id"]
                if impact.stewards_to_notify else None
            )
            bug, _ = tracker.file_bug(
                title=_drift_bug_title(impact),
                severity=_SEVERITY_MAP.get(impact.severity, BugSeverity.P2),
                category=_CATEGORY,
                description=_drift_bug_description(impact),
                filed_by=_FILED_BY,
                source_kind=_SOURCE_KIND,
                decision_ref=ref,
                tags=("drift", impact.change.change_kind),
            )
            if assignee:
                try:
                    tracker.assign(bug.bug_id, assignee)
                except Exception:
                    pass
            filed.append({
                "decision_ref": ref,
                "bug_id": bug.bug_id,
                "severity": impact.severity,
                "assigned_to": assignee,
            })
        except Exception as exc:
            errors.append({"decision_ref": ref, "error": f"file: {exc}"})

    return {"filed": filed, "skipped": skipped, "errors": errors}


class DataDictionaryDriftProjector(HeartbeatModule):
    """Snapshot + diff + governance escalation, every heartbeat cycle."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_drift_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            result = detect_drift(self._conn, triggered_by="heartbeat")
            impacts_payload = result.get("impact") or []
            if impacts_payload:
                # Re-build ChangeImpact-like view for the bug filer.
                # _file_bugs only needs severity, change.{kind,object_kind,
                # field_path}, downstream/quality counts, stewards.
                from runtime.data_dictionary_drift import ChangeImpact, FieldChange

                impacts = []
                for ip in impacts_payload:
                    cp = ip["change"]
                    impacts.append(ChangeImpact(
                        change=FieldChange(
                            change_kind=cp["change_kind"],
                            object_kind=cp["object_kind"],
                            field_path=cp.get("field_path", ""),
                            before=cp.get("before"),
                            after=cp.get("after"),
                        ),
                        severity=ip["severity"],
                        reasons=list(ip.get("reasons") or []),
                        pii_dropped=bool(ip.get("pii_dropped")),
                        quality_rule_count=int(ip.get("quality_rule_count", 0)),
                        downstream_count=int(ip.get("downstream_count", 0)),
                        stewards_to_notify=list(ip.get("stewards_to_notify") or []),
                    ))
                tracker = BugTracker(self._conn)
                _file_bugs(self._conn, tracker, impacts)

            # Retention.
            try:
                prune_snapshot_history(
                    self._conn, keep_days=_SNAPSHOT_RETENTION_DAYS,
                )
            except Exception:
                pass
        except Exception as exc:
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = ["DataDictionaryDriftProjector"]
