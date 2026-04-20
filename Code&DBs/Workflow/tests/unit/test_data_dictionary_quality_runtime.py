"""Unit tests for `runtime.data_dictionary_quality`.

Exercises the orchestration + evaluator layer. Storage is stubbed; the
evaluator uses a fake conn that returns scripted scalars so rule_kind
dispatch + observation shaping can be tested without a live Postgres.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary_quality as runtime
from runtime.data_dictionary_quality import (
    DataDictionaryQualityError,
    apply_projected_rules,
    clear_operator_rule,
    describe_rules,
    evaluate_rule,
    quality_summary,
    set_operator_rule,
)


# --- fakes ---------------------------------------------------------------


class _FakeStore:
    def __init__(self, known_objects: set[str] | None = None) -> None:
        self.known = known_objects or set()
        self.upserted: list[dict[str, Any]] = []
        self.replaced: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.runs: list[dict[str, Any]] = []
        self.effective: list[dict[str, Any]] = []
        self.layers: list[dict[str, Any]] = []
        self.latest_runs: list[dict[str, Any]] = []
        self.rule_counts: dict[str, int] = {}
        self.run_counts: dict[str, int] = {}

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            runtime, "get_object",
            lambda conn, *, object_kind: (
                {"object_kind": object_kind} if object_kind in self.known else None
            ),
        )
        monkeypatch.setattr(
            runtime, "upsert_rule",
            lambda conn, **kw: (self.upserted.append(kw) or {**kw}),
        )
        monkeypatch.setattr(
            runtime, "replace_projected_rules",
            lambda conn, **kw: (self.replaced.append(kw) or len(kw["rules"])),
        )
        monkeypatch.setattr(
            runtime, "delete_rule",
            lambda conn, **kw: (self.deleted.append(kw) or True),
        )
        monkeypatch.setattr(
            runtime, "insert_run",
            lambda conn, **kw: (self.runs.append(kw) or {**kw, "run_id": "fake"}),
        )
        monkeypatch.setattr(
            runtime, "list_effective_rules",
            lambda conn, *, object_kind=None, field_path=None: self.effective,
        )
        monkeypatch.setattr(
            runtime, "list_rule_layers",
            lambda conn, *, object_kind, field_path=None: self.layers,
        )
        monkeypatch.setattr(
            runtime, "list_latest_runs",
            lambda conn, *, object_kind=None, status=None, limit=100: self.latest_runs,
        )
        monkeypatch.setattr(
            runtime, "count_rules_by_source", lambda conn: self.rule_counts,
        )
        monkeypatch.setattr(
            runtime, "count_runs_by_status", lambda conn: self.run_counts,
        )


class _ScriptedConn:
    """Fake conn that answers fetchrow() with scripted responses.

    The evaluator calls `conn.fetchrow(sql, *args)` and treats the first
    column value as the count. We keep a simple queue of returned rows.
    """

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.queue = list(rows)
        self.calls: list[tuple[str, tuple]] = []

    def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        if not self.queue:
            return None
        return self.queue.pop(0)


# --- apply_projected_rules ----------------------------------------------


def test_apply_projected_rules_rejects_unknown_rule_kind(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryQualityError):
        apply_projected_rules(
            conn=None, projector_tag="t",
            rules=[{
                "object_kind": "table:a",
                "rule_kind": "does_not_exist",
            }],
        )


def test_apply_projected_rules_requires_non_empty_tag(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryQualityError):
        apply_projected_rules(conn=None, projector_tag="  ", rules=[])


def test_apply_projected_rules_refuses_operator_source(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryQualityError):
        apply_projected_rules(
            conn=None, projector_tag="t", rules=[], source="operator",
        )


def test_apply_projected_rules_defaults_origin_projector(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    apply_projected_rules(
        conn=None,
        projector_tag="not_null",
        rules=[{
            "object_kind": "table:a",
            "field_path": "x",
            "rule_kind": "not_null",
        }],
    )
    stored = store.replaced[0]["rules"][0]
    assert stored["origin_ref"]["projector"] == "not_null"


# --- set_operator_rule / clear_operator_rule ----------------------------


def test_set_operator_rule_requires_known_object(monkeypatch) -> None:
    store = _FakeStore()  # no objects known
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryQualityError) as exc_info:
        set_operator_rule(
            conn=None, object_kind="table:missing", rule_kind="not_null",
        )
    assert exc_info.value.status_code == 404


def test_set_operator_rule_rejects_unknown_rule_kind(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a"})
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryQualityError):
        set_operator_rule(conn=None, object_kind="table:a", rule_kind="bogus")


def test_set_operator_rule_writes_operator_source(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a"})
    store.install(monkeypatch)
    set_operator_rule(
        conn=None, object_kind="table:a", rule_kind="not_null", field_path="x",
    )
    assert store.upserted[0]["source"] == "operator"
    assert store.upserted[0]["origin_ref"] == {"source": "operator"}


def test_clear_operator_rule_forwards_keys(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    clear_operator_rule(
        conn=None, object_kind="table:a", rule_kind="not_null", field_path="x",
    )
    deleted = store.deleted[0]
    assert deleted["rule_kind"] == "not_null"
    assert deleted["source"] == "operator"


# --- describe_rules / quality_summary -----------------------------------


def test_describe_rules_includes_layers_when_requested(monkeypatch) -> None:
    store = _FakeStore()
    store.effective = [{"rule_kind": "not_null"}]
    store.layers = [{"rule_kind": "not_null", "source": "auto"}]
    store.install(monkeypatch)
    payload = describe_rules(
        conn=None, object_kind="table:a", include_layers=True,
    )
    assert payload["effective"] == store.effective
    assert payload["layers"] == store.layers


def test_quality_summary_combines_counts(monkeypatch) -> None:
    store = _FakeStore()
    store.rule_counts = {"auto": 5, "operator": 1}
    store.run_counts = {"pass": 4, "fail": 1}
    store.install(monkeypatch)
    payload = quality_summary(None)
    assert payload == {
        "rules_by_source": {"auto": 5, "operator": 1},
        "latest_runs_by_status": {"pass": 4, "fail": 1},
    }


# --- evaluate_rule: dispatch + observation shaping ----------------------


def test_evaluate_rule_not_null_pass(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([{"n": 0}])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "title",
        "rule_kind": "not_null",
        "effective_source": "auto",
        "expression": {},
    })
    # The run got inserted with status=pass and failing_rows=0.
    assert outcome["status"] == "pass"
    assert outcome["observed"] == {"failing_rows": 0}


def test_evaluate_rule_not_null_fail(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([{"n": 3}])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "title",
        "rule_kind": "not_null",
        "effective_source": "auto",
        "expression": {},
    })
    assert outcome["status"] == "fail"
    assert outcome["observed"]["failing_rows"] == 3


def test_evaluate_rule_unsafe_identifier_errors(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bad; drop table foo",
        "field_path": "x",
        "rule_kind": "not_null",
        "effective_source": "auto",
        "expression": {},
    })
    # Dangerous identifier must be refused and run inserted with status=error.
    assert outcome["status"] == "error"
    assert "identifier" in outcome["error_message"]


def test_evaluate_rule_rejects_custom_sql_with_multiple_statements(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "",
        "rule_kind": "custom_sql",
        "effective_source": "operator",
        "expression": {"sql": "SELECT 1; DELETE FROM bugs"},
    })
    assert outcome["status"] == "error"
    assert "single statement" in outcome["error_message"]


def test_evaluate_rule_row_count_min_pass(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([{"n": 100}])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "",
        "rule_kind": "row_count_min",
        "effective_source": "operator",
        "expression": {"min": 50},
    })
    assert outcome["status"] == "pass"
    assert outcome["observed"]["row_count"] == 100


def test_evaluate_rule_row_count_min_fail(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([{"n": 10}])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "",
        "rule_kind": "row_count_min",
        "effective_source": "operator",
        "expression": {"min": 50},
    })
    assert outcome["status"] == "fail"


def test_evaluate_rule_enum_requires_values(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "status",
        "rule_kind": "enum",
        "effective_source": "operator",
        "expression": {},
    })
    assert outcome["status"] == "error"
    assert "expression.values" in outcome["error_message"]


def test_evaluate_rule_unknown_rule_kind(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    conn = _ScriptedConn([])
    outcome = evaluate_rule(conn, {
        "object_kind": "table:bugs",
        "field_path": "x",
        "rule_kind": "does_not_exist",
        "effective_source": "auto",
        "expression": {},
    })
    assert outcome["status"] == "error"
    assert "unsupported" in outcome["error_message"]
