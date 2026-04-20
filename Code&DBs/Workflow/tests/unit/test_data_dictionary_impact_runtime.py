"""Unit tests for `runtime.data_dictionary_impact`.

Impact analysis is a pure read composition over four existing axes —
lineage, classifications, stewardship, quality + latest runs. These
tests stub every axis call and assert:

* input validation (object_kind required, direction enum)
* the root is always present in the node list even if `walk_impact`
  omits it
* per-axis errors are collected into a node-level `errors` field rather
  than failing the whole report
* aggregate rollups count pii/sensitive fields, rules, failing/erroring
  runs, and dedupe owners/publishers by `(kind, id)`
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary_impact as impact
from runtime.data_dictionary_impact import (
    DataDictionaryImpactError,
    impact_analysis,
)


def _install(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> dict[str, Any]:
    """Install stubs for every axis call; returns a dict of captured args."""
    captured: dict[str, Any] = {}

    def _walk(conn, **kw):
        captured["walk"] = kw
        return overrides.get(
            "walk",
            {"nodes": [kw["object_kind"]], "edges": [], "max_depth": kw["max_depth"]},
        )

    def _cls(conn, *, object_kind, field_path, include_layers):
        captured.setdefault("cls_calls", []).append(object_kind)
        fn = overrides.get("cls")
        if callable(fn):
            return fn(object_kind)
        return {"effective": []}

    def _stw(conn, *, object_kind, field_path, include_layers):
        captured.setdefault("stw_calls", []).append(object_kind)
        fn = overrides.get("stw")
        if callable(fn):
            return fn(object_kind)
        return {"effective": []}

    def _rules(conn, *, object_kind, field_path, include_layers):
        captured.setdefault("rules_calls", []).append(object_kind)
        fn = overrides.get("rules")
        if callable(fn):
            return fn(object_kind)
        return {"effective": []}

    def _runs(conn, *, object_kind, status, limit):
        captured.setdefault("runs_calls", []).append(object_kind)
        fn = overrides.get("runs")
        if callable(fn):
            return fn(object_kind)
        return {"runs": []}

    monkeypatch.setattr(impact, "walk_impact", _walk)
    monkeypatch.setattr(impact, "describe_classifications", _cls)
    monkeypatch.setattr(impact, "describe_stewards", _stw)
    monkeypatch.setattr(impact, "describe_rules", _rules)
    monkeypatch.setattr(impact, "latest_runs", _runs)
    return captured


# --- input validation ----------------------------------------------------


def test_empty_object_kind_raises() -> None:
    with pytest.raises(DataDictionaryImpactError) as exc:
        impact_analysis(object(), object_kind="  ")
    assert "object_kind" in str(exc.value)


def test_invalid_direction_raises(monkeypatch) -> None:
    _install(monkeypatch)
    with pytest.raises(DataDictionaryImpactError) as exc:
        impact_analysis(object(), object_kind="table:bugs", direction="sideways")
    assert "direction" in str(exc.value)


# --- root handling -------------------------------------------------------


def test_root_is_always_in_nodes_even_if_walk_omits_it(monkeypatch) -> None:
    captured = _install(
        monkeypatch,
        walk={"nodes": ["table:child"], "edges": [], "max_depth": 3},
    )
    payload = impact_analysis(
        object(), object_kind="table:root", direction="downstream", max_depth=3,
    )
    kinds = [n["object_kind"] for n in payload["nodes"]]
    assert kinds == ["table:root", "table:child"]
    assert captured["walk"]["object_kind"] == "table:root"


def test_forwards_max_depth_and_edge_kind(monkeypatch) -> None:
    captured = _install(monkeypatch)
    impact_analysis(
        object(),
        object_kind="table:bugs",
        direction="upstream",
        max_depth=7,
        edge_kind="produces",
    )
    assert captured["walk"] == {
        "object_kind": "table:bugs",
        "direction": "upstream",
        "max_depth": 7,
        "edge_kind": "produces",
    }


# --- per-node collection -------------------------------------------------


def test_per_node_shape_includes_every_axis(monkeypatch) -> None:
    _install(
        monkeypatch,
        cls=lambda k: {
            "effective": [
                {"tag_key": "pii", "tag_value": "true", "field_path": "email",
                 "effective_source": "auto"},
            ],
        },
        stw=lambda k: {
            "effective": [
                {"steward_kind": "owner", "steward_id": "svc", "steward_type": "service",
                 "effective_source": "auto"},
            ],
        },
        rules=lambda k: {
            "effective": [
                {"rule_kind": "not_null", "field_path": "id", "severity": "error",
                 "effective_source": "auto"},
            ],
        },
        runs=lambda k: {"runs": [{"status": "pass"}, {"status": "fail"}]},
    )
    payload = impact_analysis(object(), object_kind="table:x")
    node = payload["nodes"][0]
    assert node["object_kind"] == "table:x"
    assert node["tags"][0]["tag_key"] == "pii"
    assert node["stewards"][0]["steward_kind"] == "owner"
    assert node["rules"][0]["rule_kind"] == "not_null"
    assert node["run_status"] == {"pass": 1, "fail": 1}
    assert "errors" not in node


def test_per_axis_errors_captured_in_errors_field(monkeypatch) -> None:
    def _raise(_k):
        raise RuntimeError("boom")

    _install(monkeypatch, cls=_raise, stw=_raise, rules=_raise)
    payload = impact_analysis(object(), object_kind="table:x")
    node = payload["nodes"][0]
    assert node["tags"] == []
    assert node["stewards"] == []
    assert node["rules"] == []
    assert set(node["errors"].keys()) == {"tags_error", "stewards_error", "rules_error"}


def test_run_status_failure_swallowed_silently(monkeypatch) -> None:
    def _raise(_k):
        raise RuntimeError("db down")

    _install(monkeypatch, runs=_raise)
    payload = impact_analysis(object(), object_kind="table:x")
    assert payload["nodes"][0]["run_status"] == {}


# --- aggregate rollups ---------------------------------------------------


def test_aggregate_counts_pii_and_sensitive(monkeypatch) -> None:
    _install(
        monkeypatch,
        walk={"nodes": ["a", "b"], "edges": [], "max_depth": 5},
        cls=lambda k: {
            "a": {"effective": [
                {"tag_key": "pii", "tag_value": "true"},
                {"tag_key": "sensitive", "tag_value": "true"},
            ]},
            "b": {"effective": [
                {"tag_key": "pii", "tag_value": "true"},
                {"tag_key": "domain", "tag_value": "hr"},
            ]},
        }[k],
    )
    payload = impact_analysis(object(), object_kind="a")
    agg = payload["aggregate"]
    # Two nodes, each with a pii tag → 2 pii fields; a's sensitive tag + both
    # pii tags count as sensitive (superset).
    assert agg["pii_fields"] == 2
    assert agg["sensitive_fields"] == 3
    assert agg["total_nodes"] == 2


def test_aggregate_dedupes_owners_and_publishers(monkeypatch) -> None:
    _install(
        monkeypatch,
        walk={"nodes": ["a", "b"], "edges": [], "max_depth": 5},
        stw=lambda k: {"effective": [
            {"steward_kind": "owner", "steward_id": "alice", "steward_type": "person"},
            {"steward_kind": "publisher", "steward_id": "svc", "steward_type": "agent"},
        ]},
    )
    payload = impact_analysis(object(), object_kind="a")
    agg = payload["aggregate"]
    # Both nodes saw the same pair — dedup by (kind, id).
    assert agg["distinct_owners"] == ["owner:alice"]
    assert agg["distinct_publishers"] == ["publisher:svc"]


def test_aggregate_counts_rules_and_failures(monkeypatch) -> None:
    _install(
        monkeypatch,
        walk={"nodes": ["a", "b"], "edges": [], "max_depth": 5},
        rules=lambda k: {"effective": [
            {"rule_kind": "not_null"}, {"rule_kind": "unique"},
        ]},
        runs=lambda k: {"runs": [
            {"status": "fail"}, {"status": "fail"}, {"status": "error"},
            {"status": "pass"},
        ]},
    )
    payload = impact_analysis(object(), object_kind="a")
    agg = payload["aggregate"]
    assert agg["rule_count"] == 4   # 2 rules × 2 nodes
    assert agg["failing_runs"] == 4  # 2 fail × 2 nodes
    assert agg["erroring_runs"] == 2  # 1 err × 2 nodes


def test_envelope_preserves_walk_metadata(monkeypatch) -> None:
    _install(
        monkeypatch,
        walk={
            "nodes": ["a"],
            "edges": [{"source": "a", "target": "b", "kind": "produces"}],
            "max_depth": 2,
        },
    )
    payload = impact_analysis(object(), object_kind="a", max_depth=2)
    assert payload["root"] == "a"
    assert payload["direction"] == "downstream"
    assert payload["max_depth"] == 2
    assert payload["edges"] == [{"source": "a", "target": "b", "kind": "produces"}]
