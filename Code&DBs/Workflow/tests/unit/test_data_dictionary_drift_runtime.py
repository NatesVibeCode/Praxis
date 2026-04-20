"""Unit tests for `runtime.data_dictionary_drift`.

The runtime is largely glue over four sub-axes plus the snapshot
repository. These tests stub the storage + cross-axis lookups so they
exercise the diff/impact logic without a live Postgres.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary_drift as drift
from runtime.data_dictionary_drift import (
    ChangeImpact,
    DataDictionaryDriftError,
    FieldChange,
    SchemaDiff,
    detect_drift,
    diff_snapshots,
    impact_of_diff,
    take_snapshot,
)


# ---------------------------------------------------------------------------
# Snapshot capture
# ---------------------------------------------------------------------------

def test_take_snapshot_writes_inventory_and_returns_metadata(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _C:
        def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
            assert "FROM data_dictionary_entries" in sql
            return [
                {"object_kind": "table:bugs", "field_path": "title",
                 "field_kind": "text", "required": True, "sources": ["auto"]},
                {"object_kind": "table:bugs", "field_path": "severity",
                 "field_kind": "text", "required": True, "sources": ["auto"]},
            ]

    def _insert_snapshot(conn, **kw):
        captured["snapshot"] = kw
        return {"snapshot_id": "S1", "taken_at": "T", **kw}

    def _insert_fields(conn, **kw):
        captured["fields"] = list(kw["fields"])
        return len(captured["fields"])

    monkeypatch.setattr(drift, "insert_snapshot", _insert_snapshot)
    monkeypatch.setattr(drift, "insert_snapshot_fields", _insert_fields)

    snap = take_snapshot(_C())
    assert snap["object_count"] == 1     # one distinct object
    assert snap["field_count"] == 2      # two fields
    assert snap["fields_written"] == 2
    assert len(captured["fields"]) == 2
    assert len(captured["snapshot"]["fingerprint"]) == 64   # sha256 hex


def test_take_snapshot_fingerprint_is_deterministic(monkeypatch) -> None:
    rows = [
        {"object_kind": "a", "field_path": "x", "field_kind": "text",
         "required": True, "sources": []},
        {"object_kind": "b", "field_path": "y", "field_kind": "int",
         "required": False, "sources": []},
    ]
    fp1 = drift._compute_fingerprint(rows)
    fp2 = drift._compute_fingerprint(rows)
    assert fp1 == fp2
    rows2 = list(reversed(rows))
    # Order-sensitive: caller is expected to ORDER BY in SQL.
    assert drift._compute_fingerprint(rows2) != fp1


# ---------------------------------------------------------------------------
# Diff computation
# ---------------------------------------------------------------------------

def _stub_snapshot_fields(monkeypatch, by_id: dict[str, list[dict[str, Any]]]) -> None:
    monkeypatch.setattr(
        drift, "fetch_snapshot_fields",
        lambda conn, *, snapshot_id: list(by_id.get(snapshot_id, [])),
    )


def test_diff_detects_added_object(monkeypatch) -> None:
    _stub_snapshot_fields(monkeypatch, {
        "OLD": [{"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True}],
        "NEW": [
            {"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True},
            {"object_kind": "b", "field_path": "y", "field_kind": "int", "required": False},
        ],
    })
    diff = diff_snapshots(object(), old_id="OLD", new_id="NEW")
    kinds = [c.change_kind for c in diff.changes]
    assert kinds == ["add_object"]
    assert diff.changes[0].object_kind == "b"


def test_diff_detects_dropped_object(monkeypatch) -> None:
    _stub_snapshot_fields(monkeypatch, {
        "OLD": [
            {"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True},
            {"object_kind": "b", "field_path": "y", "field_kind": "int", "required": False},
        ],
        "NEW": [
            {"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True},
        ],
    })
    diff = diff_snapshots(object(), old_id="OLD", new_id="NEW")
    assert [c.change_kind for c in diff.changes] == ["drop_object"]
    assert diff.changes[0].object_kind == "b"


def test_diff_detects_added_dropped_changed_fields(monkeypatch) -> None:
    _stub_snapshot_fields(monkeypatch, {
        "OLD": [
            {"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True},
            {"object_kind": "a", "field_path": "z", "field_kind": "int", "required": True},
        ],
        "NEW": [
            {"object_kind": "a", "field_path": "x", "field_kind": "text", "required": False},  # nullability change
            {"object_kind": "a", "field_path": "y", "field_kind": "int", "required": True},    # added
            # z dropped
        ],
    })
    diff = diff_snapshots(object(), old_id="OLD", new_id="NEW")
    kinds_set = {c.change_kind for c in diff.changes}
    assert kinds_set == {"add_field", "drop_field", "change_field"}
    by_kind = {c.change_kind: c for c in diff.changes}
    assert by_kind["add_field"].field_path == "y"
    assert by_kind["drop_field"].field_path == "z"
    assert by_kind["change_field"].field_path == "x"
    assert by_kind["change_field"].before["required"] is True
    assert by_kind["change_field"].after["required"] is False


def test_diff_payload_includes_summary_counts(monkeypatch) -> None:
    _stub_snapshot_fields(monkeypatch, {
        "OLD": [],
        "NEW": [
            {"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True},
            {"object_kind": "b", "field_path": "y", "field_kind": "int", "required": False},
        ],
    })
    diff = diff_snapshots(object(), old_id="OLD", new_id="NEW")
    payload = diff.to_payload()
    assert payload["total_changes"] == 2
    assert payload["by_change_kind"] == {"add_object": 2}


def test_diff_rejects_missing_ids() -> None:
    with pytest.raises(DataDictionaryDriftError):
        diff_snapshots(object(), old_id="", new_id="x")


# ---------------------------------------------------------------------------
# Impact assessment severity scoring
# ---------------------------------------------------------------------------

def _stub_axes(
    monkeypatch,
    *,
    classifications=None, rules=None, stewards=None, downstream=0,
) -> None:
    monkeypatch.setattr(drift, "_classifications_for", lambda c, k: list(classifications or []))
    monkeypatch.setattr(drift, "_rules_for",          lambda c, k: list(rules or []))
    monkeypatch.setattr(drift, "_stewards_for",       lambda c, k: list(stewards or []))
    monkeypatch.setattr(drift, "_downstream_count",    lambda c, k: int(downstream))


def test_dropped_pii_object_is_p0(monkeypatch) -> None:
    _stub_axes(monkeypatch, classifications=[
        {"tag_key": "pii", "field_path": ""},
    ])
    impact = drift._assess_change(
        object(),
        FieldChange(change_kind="drop_object", object_kind="table:users"),
    )
    assert impact.severity == "P0"
    assert impact.pii_dropped is True


def test_dropped_object_with_downstream_is_p1(monkeypatch) -> None:
    _stub_axes(monkeypatch, downstream=4)
    impact = drift._assess_change(
        object(),
        FieldChange(change_kind="drop_object", object_kind="table:users"),
    )
    assert impact.severity == "P1"
    assert impact.downstream_count == 4


def test_dropped_field_with_quality_rule_is_p1(monkeypatch) -> None:
    _stub_axes(monkeypatch, rules=[{"field_path": "email", "rule_kind": "not_null"}])
    impact = drift._assess_change(
        object(),
        FieldChange(change_kind="drop_field", object_kind="table:users", field_path="email"),
    )
    assert impact.severity == "P1"
    assert impact.quality_rule_count == 1


def test_dropped_pii_field_is_p0(monkeypatch) -> None:
    _stub_axes(monkeypatch, classifications=[
        {"tag_key": "pii", "field_path": "email"},
    ])
    impact = drift._assess_change(
        object(),
        FieldChange(change_kind="drop_field", object_kind="table:users", field_path="email"),
    )
    assert impact.severity == "P0"
    assert impact.pii_dropped is True


def test_changed_field_with_rules_is_p2(monkeypatch) -> None:
    _stub_axes(monkeypatch, rules=[{"field_path": "x", "rule_kind": "not_null"}])
    impact = drift._assess_change(
        object(),
        FieldChange(
            change_kind="change_field", object_kind="table:a", field_path="x",
            before={"field_kind": "text"}, after={"field_kind": "int"},
        ),
    )
    assert impact.severity == "P2"


def test_unowned_new_object_is_p2(monkeypatch) -> None:
    _stub_axes(monkeypatch)  # no stewards
    impact = drift._assess_change(
        object(),
        FieldChange(change_kind="add_object", object_kind="table:newbie"),
    )
    assert impact.severity == "P2"
    assert any("no owner" in r for r in impact.reasons)


def test_owned_new_object_is_informational(monkeypatch) -> None:
    _stub_axes(monkeypatch, stewards=[
        {"steward_kind": "owner", "steward_id": "alice"},
    ])
    impact = drift._assess_change(
        object(),
        FieldChange(change_kind="add_object", object_kind="table:newbie"),
    )
    assert impact.severity == "P3"


def test_impact_of_diff_iterates_all_changes(monkeypatch) -> None:
    _stub_axes(monkeypatch)
    diff = SchemaDiff(
        old_snapshot_id="A", new_snapshot_id="B",
        changes=[
            FieldChange(change_kind="add_object", object_kind="table:a"),
            FieldChange(change_kind="drop_object", object_kind="table:b"),
        ],
    )
    impacts = impact_of_diff(object(), diff)
    assert len(impacts) == 2
    assert {i.change.change_kind for i in impacts} == {"add_object", "drop_object"}


# ---------------------------------------------------------------------------
# detect_drift composite
# ---------------------------------------------------------------------------

def test_detect_drift_first_capture_returns_baseline_note(monkeypatch) -> None:
    monkeypatch.setattr(drift, "fetch_latest_snapshot", lambda conn: None)
    monkeypatch.setattr(drift, "take_snapshot", lambda c, **kw: {"snapshot_id": "S1"})
    result = detect_drift(object())
    assert result["diff"] is None
    assert "baseline" in result["note"]


def test_detect_drift_skips_diff_when_fingerprint_unchanged(monkeypatch) -> None:
    monkeypatch.setattr(
        drift, "fetch_latest_snapshot",
        lambda conn: {"snapshot_id": "S0", "fingerprint": "abc",
                      "taken_at": "t"},
    )
    monkeypatch.setattr(
        drift, "take_snapshot",
        lambda c, **kw: {"snapshot_id": "S1", "fingerprint": "abc"},
    )
    result = detect_drift(object())
    assert result["diff"]["total_changes"] == 0
    assert "no schema drift" in result["note"]


def test_detect_drift_returns_changes_when_fingerprint_differs(monkeypatch) -> None:
    monkeypatch.setattr(
        drift, "fetch_latest_snapshot",
        lambda conn: {"snapshot_id": "S0", "fingerprint": "old", "taken_at": "t"},
    )
    monkeypatch.setattr(
        drift, "take_snapshot",
        lambda c, **kw: {"snapshot_id": "S1", "fingerprint": "new"},
    )
    _stub_snapshot_fields(monkeypatch, {
        "S0": [],
        "S1": [{"object_kind": "a", "field_path": "x", "field_kind": "text", "required": True}],
    })
    _stub_axes(monkeypatch)
    result = detect_drift(object())
    assert result["diff"]["total_changes"] == 1
    assert result["impact"][0]["change"]["change_kind"] == "add_object"


def test_detect_drift_orders_impacts_by_severity(monkeypatch) -> None:
    monkeypatch.setattr(
        drift, "fetch_latest_snapshot",
        lambda conn: {"snapshot_id": "S0", "fingerprint": "old", "taken_at": "t"},
    )
    monkeypatch.setattr(
        drift, "take_snapshot",
        lambda c, **kw: {"snapshot_id": "S1", "fingerprint": "new"},
    )
    _stub_snapshot_fields(monkeypatch, {
        "S0": [
            {"object_kind": "table:users", "field_path": "email",
             "field_kind": "text", "required": True},
        ],
        "S1": [
            {"object_kind": "table:newbie", "field_path": "x",
             "field_kind": "text", "required": True},
        ],
    })

    def _classifications_for(conn, k):
        # users.email is PII → P0 when dropped
        if k == "table:users":
            return [{"tag_key": "pii", "field_path": "email"}]
        return []

    monkeypatch.setattr(drift, "_classifications_for", _classifications_for)
    monkeypatch.setattr(drift, "_rules_for",     lambda c, k: [])
    monkeypatch.setattr(drift, "_stewards_for",  lambda c, k: [])
    monkeypatch.setattr(drift, "_downstream_count", lambda c, k: 0)

    result = detect_drift(object())
    severities = [i["severity"] for i in result["impact"]]
    # P0 (drop_field on PII) must come before P2 (unowned new object).
    assert severities == sorted(severities, key=lambda s: drift._SEVERITY_ORDER[s])
    assert severities[0] == "P0"
