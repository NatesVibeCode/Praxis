"""Schema drift detection over the data dictionary.

Captures point-in-time snapshots of the field inventory, diffs
successive snapshots, and produces cross-axis impact assessments for
every detected change. Closes the loop with the existing axes:

* lineage      → which downstream consumers reference a dropped object
* classifications → was the dropped/changed field tagged PII / sensitive?
* stewardship  → who should be notified when their owned object drifts?
* quality      → which rules now reference vanished objects/fields?

Public surface:

    take_snapshot(conn, *, triggered_by="heartbeat") -> dict
    diff_snapshots(conn, *, old_id, new_id)         -> SchemaDiff
    impact_of_diff(conn, diff)                      -> list[ChangeImpact]
    detect_drift(conn, *, triggered_by="heartbeat")
        Convenience: snapshot, diff against latest, return diff + impact.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from storage.postgres.data_dictionary_drift_repository import (
    fetch_latest_snapshot,
    fetch_snapshot_before,
    fetch_snapshot_by_id,
    fetch_snapshot_fields,
    insert_snapshot,
    insert_snapshot_fields,
    list_snapshots,
    prune_snapshots_older_than,
)


class DataDictionaryDriftError(RuntimeError):
    """Raised when a drift call cannot execute."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Diff dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FieldChange:
    """One change between two snapshots, scoped to a single (object, field)."""

    change_kind: str  # add_object, drop_object, add_field, drop_field, change_field
    object_kind: str
    field_path: str = ""
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        out = {
            "change_kind": self.change_kind,
            "object_kind": self.object_kind,
            "field_path": self.field_path,
        }
        if self.before is not None:
            out["before"] = dict(self.before)
        if self.after is not None:
            out["after"] = dict(self.after)
        return out


@dataclass(frozen=True)
class SchemaDiff:
    old_snapshot_id: str
    new_snapshot_id: str
    changes: list[FieldChange] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        by_kind: dict[str, int] = {}
        for c in self.changes:
            by_kind[c.change_kind] = by_kind.get(c.change_kind, 0) + 1
        return {
            "old_snapshot_id": self.old_snapshot_id,
            "new_snapshot_id": self.new_snapshot_id,
            "total_changes": len(self.changes),
            "by_change_kind": by_kind,
            "changes": [c.to_payload() for c in self.changes],
        }


# ---------------------------------------------------------------------------
# Snapshot capture
# ---------------------------------------------------------------------------

_SQL_FIELD_INVENTORY = """
SELECT object_kind, field_path, field_kind, required,
       array_agg(DISTINCT source ORDER BY source) AS sources
FROM data_dictionary_entries
GROUP BY object_kind, field_path, field_kind, required
ORDER BY object_kind, field_path
"""


def _compute_fingerprint(rows: list[dict[str, Any]]) -> str:
    """Stable hash of (object_kind, field_path, field_kind, required) tuples."""
    h = hashlib.sha256()
    for r in rows:
        h.update(
            f"{r['object_kind']}\x1f{r['field_path']}\x1f"
            f"{r['field_kind']}\x1f{int(bool(r['required']))}\n".encode()
        )
    return h.hexdigest()


def take_snapshot(
    conn: Any,
    *,
    triggered_by: str = "heartbeat",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture the current field inventory as a new snapshot."""
    rows = conn.execute(_SQL_FIELD_INVENTORY) or []
    field_dicts = [dict(r) for r in rows]

    fingerprint = _compute_fingerprint(field_dicts)
    object_count = len({r["object_kind"] for r in field_dicts})
    field_count = len(field_dicts)

    snap = insert_snapshot(
        conn,
        fingerprint=fingerprint,
        object_count=object_count,
        field_count=field_count,
        triggered_by=triggered_by,
        metadata=metadata or {},
    )
    insert_snapshot_fields(conn, snapshot_id=snap["snapshot_id"], fields=field_dicts)
    snap["fields_written"] = field_count
    return snap


# ---------------------------------------------------------------------------
# Diff computation
# ---------------------------------------------------------------------------

def _index_fields(
    fields: list[dict[str, Any]],
) -> tuple[
    set[str],                               # object_kinds
    dict[tuple[str, str], dict[str, Any]],  # (object, field_path) → row
]:
    objects: set[str] = set()
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for r in fields:
        ok = str(r.get("object_kind") or "").strip()
        if not ok:
            continue
        fp = str(r.get("field_path") or "")
        objects.add(ok)
        by_key[(ok, fp)] = r
    return objects, by_key


def diff_snapshots(
    conn: Any,
    *,
    old_id: str,
    new_id: str,
) -> SchemaDiff:
    if not old_id or not new_id:
        raise DataDictionaryDriftError("old_id and new_id are required")

    old_fields = fetch_snapshot_fields(conn, snapshot_id=old_id)
    new_fields = fetch_snapshot_fields(conn, snapshot_id=new_id)
    old_objects, old_by_key = _index_fields(old_fields)
    new_objects, new_by_key = _index_fields(new_fields)

    changes: list[FieldChange] = []

    # Object-level changes (table added / dropped wholesale).
    for ok in sorted(new_objects - old_objects):
        changes.append(FieldChange(
            change_kind="add_object",
            object_kind=ok,
            after={"field_count": sum(1 for k in new_by_key if k[0] == ok)},
        ))
    for ok in sorted(old_objects - new_objects):
        changes.append(FieldChange(
            change_kind="drop_object",
            object_kind=ok,
            before={"field_count": sum(1 for k in old_by_key if k[0] == ok)},
        ))

    # Field-level changes within objects that exist in both snapshots.
    common_objects = old_objects & new_objects
    for ok in sorted(common_objects):
        old_keys = {k for k in old_by_key if k[0] == ok}
        new_keys = {k for k in new_by_key if k[0] == ok}

        for k in sorted(new_keys - old_keys):
            after = new_by_key[k]
            changes.append(FieldChange(
                change_kind="add_field",
                object_kind=ok,
                field_path=k[1],
                after={
                    "field_kind": after.get("field_kind"),
                    "required": bool(after.get("required")),
                },
            ))
        for k in sorted(old_keys - new_keys):
            before = old_by_key[k]
            changes.append(FieldChange(
                change_kind="drop_field",
                object_kind=ok,
                field_path=k[1],
                before={
                    "field_kind": before.get("field_kind"),
                    "required": bool(before.get("required")),
                },
            ))
        for k in sorted(old_keys & new_keys):
            before = old_by_key[k]
            after = new_by_key[k]
            if (before.get("field_kind") != after.get("field_kind")
                    or bool(before.get("required")) != bool(after.get("required"))):
                changes.append(FieldChange(
                    change_kind="change_field",
                    object_kind=ok,
                    field_path=k[1],
                    before={
                        "field_kind": before.get("field_kind"),
                        "required": bool(before.get("required")),
                    },
                    after={
                        "field_kind": after.get("field_kind"),
                        "required": bool(after.get("required")),
                    },
                ))

    return SchemaDiff(
        old_snapshot_id=old_id,
        new_snapshot_id=new_id,
        changes=changes,
    )


# ---------------------------------------------------------------------------
# Cross-axis impact assessment
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ChangeImpact:
    change: FieldChange
    severity: str  # P0 | P1 | P2 | P3
    reasons: list[str] = field(default_factory=list)
    pii_dropped: bool = False
    quality_rule_count: int = 0
    downstream_count: int = 0
    stewards_to_notify: list[dict[str, str]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "change": self.change.to_payload(),
            "severity": self.severity,
            "reasons": list(self.reasons),
            "pii_dropped": self.pii_dropped,
            "quality_rule_count": self.quality_rule_count,
            "downstream_count": self.downstream_count,
            "stewards_to_notify": list(self.stewards_to_notify),
        }


def _classifications_for(conn: Any, object_kind: str) -> list[dict[str, Any]]:
    try:
        from runtime.data_dictionary_classifications import describe_classifications

        payload = describe_classifications(
            conn, object_kind=object_kind, field_path=None, include_layers=False,
        )
        return list(payload.get("effective") or [])
    except Exception:
        return []


def _rules_for(conn: Any, object_kind: str) -> list[dict[str, Any]]:
    try:
        from runtime.data_dictionary_quality import describe_rules

        payload = describe_rules(
            conn, object_kind=object_kind, field_path=None, include_layers=False,
        )
        return list(payload.get("effective") or [])
    except Exception:
        return []


def _stewards_for(conn: Any, object_kind: str) -> list[dict[str, Any]]:
    try:
        from runtime.data_dictionary_stewardship import describe_stewards

        payload = describe_stewards(
            conn, object_kind=object_kind, field_path=None, include_layers=False,
        )
        return list(payload.get("effective") or [])
    except Exception:
        return []


def _downstream_count(conn: Any, object_kind: str) -> int:
    try:
        from runtime.data_dictionary_lineage import walk_impact

        walk = walk_impact(
            conn, object_kind=object_kind, direction="downstream", max_depth=3,
        )
        return max(0, len([n for n in (walk.get("nodes") or []) if n != object_kind]))
    except Exception:
        return 0


def _assess_change(conn: Any, change: FieldChange) -> ChangeImpact:
    reasons: list[str] = []
    pii_dropped = False
    quality_rule_count = 0
    downstream = 0
    stewards: list[dict[str, str]] = []
    severity = "P3"

    cls = _classifications_for(conn, change.object_kind)
    rules = _rules_for(conn, change.object_kind)
    stws = _stewards_for(conn, change.object_kind)

    pii_keys = {c.get("tag_key") for c in cls if c.get("tag_key") in {"pii", "sensitive"}}

    if change.change_kind in ("drop_object", "drop_field"):
        # Was the dropped surface PII / sensitive?
        if change.field_path:
            field_pii = any(
                c.get("field_path") == change.field_path
                and c.get("tag_key") in {"pii", "sensitive"}
                for c in cls
            )
            if field_pii:
                pii_dropped = True
                reasons.append(
                    f"dropped field carried tag(s): {', '.join(sorted(pii_keys))}"
                )
        else:
            if pii_keys:
                pii_dropped = True
                reasons.append(
                    f"dropped object carried object-level tag(s): "
                    f"{', '.join(sorted(pii_keys))}"
                )

    # Lineage downstream impact.
    if change.change_kind in ("drop_object", "drop_field", "change_field"):
        downstream = _downstream_count(conn, change.object_kind)
        if downstream:
            reasons.append(f"{downstream} downstream consumer(s) in lineage")

    # Quality rules attached to this surface.
    if change.field_path:
        quality_rule_count = sum(
            1 for r in rules if r.get("field_path") == change.field_path
        )
    else:
        quality_rule_count = len(rules)
    if quality_rule_count and change.change_kind in ("drop_object", "drop_field", "change_field"):
        reasons.append(f"{quality_rule_count} effective quality rule(s) attached")

    # Stewards to notify.
    for s in stws:
        kind = str(s.get("steward_kind") or "")
        sid = str(s.get("steward_id") or "")
        if kind and sid:
            stewards.append({"steward_kind": kind, "steward_id": sid})

    # Severity scoring.
    if pii_dropped:
        severity = "P0"
    elif change.change_kind == "drop_object" and downstream > 0:
        severity = "P1"
    elif change.change_kind == "drop_field" and (quality_rule_count or downstream >= 5):
        severity = "P1"
    elif change.change_kind in ("drop_object", "drop_field"):
        severity = "P2"
    elif change.change_kind == "change_field" and quality_rule_count:
        severity = "P2"
    elif change.change_kind == "add_object" and not stewards:
        # Newborn objects with no owner are immediate governance debt.
        severity = "P2"
        reasons.append("new object has no owner steward")
    else:
        severity = "P3"

    if not reasons:
        reasons.append("informational change")

    return ChangeImpact(
        change=change,
        severity=severity,
        reasons=reasons,
        pii_dropped=pii_dropped,
        quality_rule_count=quality_rule_count,
        downstream_count=downstream,
        stewards_to_notify=stewards,
    )


def impact_of_diff(conn: Any, diff: SchemaDiff) -> list[ChangeImpact]:
    return [_assess_change(conn, c) for c in diff.changes]


# ---------------------------------------------------------------------------
# Convenience entry points
# ---------------------------------------------------------------------------

_SEVERITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


def detect_drift(
    conn: Any,
    *,
    triggered_by: str = "heartbeat",
    snapshot_first: bool = True,
) -> dict[str, Any]:
    """Take a fresh snapshot, diff against the prior snapshot, return both."""
    prior = fetch_latest_snapshot(conn) if snapshot_first else None

    if snapshot_first:
        new_snap = take_snapshot(conn, triggered_by=triggered_by)
    else:
        # Diff the two most recent existing snapshots without writing new state.
        latest = fetch_latest_snapshot(conn)
        if not latest:
            return {"snapshot": None, "diff": None, "impact": []}
        new_snap = latest
        prior = fetch_snapshot_before(conn, taken_at=latest["taken_at"])

    if not prior:
        return {
            "snapshot": new_snap,
            "diff": None,
            "impact": [],
            "note": "no prior snapshot — first capture, drift baseline established",
        }

    if prior["fingerprint"] == new_snap["fingerprint"]:
        return {
            "snapshot": new_snap,
            "diff": {
                "old_snapshot_id": prior["snapshot_id"],
                "new_snapshot_id": new_snap["snapshot_id"],
                "total_changes": 0,
                "by_change_kind": {},
                "changes": [],
            },
            "impact": [],
            "note": "fingerprint match — no schema drift",
        }

    diff = diff_snapshots(
        conn, old_id=prior["snapshot_id"], new_id=new_snap["snapshot_id"],
    )
    impacts = impact_of_diff(conn, diff)
    impacts.sort(key=lambda i: _SEVERITY_ORDER.get(i.severity, 99))
    return {
        "snapshot": new_snap,
        "diff": diff.to_payload(),
        "impact": [i.to_payload() for i in impacts],
    }


def drift_history(conn: Any, *, limit: int = 50) -> dict[str, Any]:
    snaps = list_snapshots(conn, limit=limit)
    return {
        "count": len(snaps),
        "snapshots": [
            {
                "snapshot_id": s["snapshot_id"],
                "taken_at": (
                    s["taken_at"].isoformat()
                    if hasattr(s["taken_at"], "isoformat") else str(s["taken_at"])
                ),
                "fingerprint": s["fingerprint"][:12],
                "object_count": s["object_count"],
                "field_count": s["field_count"],
                "triggered_by": s["triggered_by"],
            }
            for s in snaps
        ],
    }


def prune_snapshot_history(conn: Any, *, keep_days: int = 30) -> int:
    return prune_snapshots_older_than(conn, days=keep_days)


__all__ = [
    "ChangeImpact",
    "DataDictionaryDriftError",
    "FieldChange",
    "SchemaDiff",
    "detect_drift",
    "diff_snapshots",
    "drift_history",
    "impact_of_diff",
    "prune_snapshot_history",
    "take_snapshot",
]
