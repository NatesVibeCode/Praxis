"""Unit tests for `runtime.data_dictionary_lineage`.

Exercises the thin orchestration layer: input validation, edge normalization,
and one-hop / multi-hop read paths. Storage is stubbed so these tests can
run without a live Postgres.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary_lineage as runtime
from runtime.data_dictionary_lineage import (
    DataDictionaryLineageError,
    apply_projected_edges,
    clear_operator_edge,
    describe_edges,
    set_operator_edge,
    walk_impact,
)


# --- fakes ------------------------------------------------------------------


class _FakeStore:
    """In-memory stand-in for the lineage repository functions."""

    def __init__(self, known_objects: set[str] | None = None) -> None:
        self.known = known_objects or set()
        self.upserted: list[dict[str, Any]] = []
        self.replaced: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.edges_from: dict[str, list[dict[str, Any]]] = {}
        self.edges_to: dict[str, list[dict[str, Any]]] = {}

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            runtime, "get_object",
            lambda conn, *, object_kind: (
                {"object_kind": object_kind} if object_kind in self.known else None
            ),
        )
        monkeypatch.setattr(
            runtime, "upsert_edge",
            lambda conn, **kw: (self.upserted.append(kw) or {**kw, "source": kw["source"]}),
        )
        monkeypatch.setattr(
            runtime, "replace_projected_edges",
            lambda conn, **kw: (self.replaced.append(kw) or len(kw["edges"])),
        )
        monkeypatch.setattr(
            runtime, "delete_edge",
            lambda conn, **kw: (self.deleted.append(kw) or True),
        )
        monkeypatch.setattr(
            runtime, "list_edges_from",
            lambda conn, *, src_object_kind, edge_kind=None:
                self.edges_from.get(src_object_kind, []),
        )
        monkeypatch.setattr(
            runtime, "list_edges_to",
            lambda conn, *, dst_object_kind, edge_kind=None:
                self.edges_to.get(dst_object_kind, []),
        )
        monkeypatch.setattr(runtime, "list_edges_layers", lambda *a, **kw: [])


# --- apply_projected_edges (projector-facing) -------------------------------


def test_apply_projected_edges_rejects_unknown_edge_kind(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        apply_projected_edges(
            conn=None,
            projector_tag="t",
            edges=[{
                "src_object_kind": "a",
                "dst_object_kind": "b",
                "edge_kind": "bogus",
            }],
        )


def test_apply_projected_edges_requires_non_empty_tag(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        apply_projected_edges(conn=None, projector_tag="   ", edges=[])


def test_apply_projected_edges_defaults_origin_projector(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    apply_projected_edges(
        conn=None,
        projector_tag="fk_edges",
        edges=[{
            "src_object_kind": "table:a",
            "dst_object_kind": "table:b",
            "edge_kind": "references",
        }],
    )
    # Normalization must inject `origin_ref.projector = tag` when caller omits it.
    stored = store.replaced[0]["edges"][0]
    assert stored["origin_ref"]["projector"] == "fk_edges"


def test_apply_projected_edges_refuses_operator_source(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        apply_projected_edges(conn=None, projector_tag="t", edges=[], source="operator")


# --- set_operator_edge / clear_operator_edge --------------------------------


def test_set_operator_edge_requires_known_objects(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a"})  # dst is missing
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError) as exc_info:
        set_operator_edge(
            conn=None,
            src_object_kind="table:a",
            dst_object_kind="table:b",
            edge_kind="references",
        )
    assert exc_info.value.status_code == 404


def test_set_operator_edge_writes_operator_source(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a", "table:b"})
    store.install(monkeypatch)
    set_operator_edge(
        conn=None,
        src_object_kind="table:a",
        dst_object_kind="table:b",
        edge_kind="same_as",
    )
    assert store.upserted[0]["source"] == "operator"
    assert store.upserted[0]["origin_ref"] == {"source": "operator"}


def test_set_operator_edge_rejects_unknown_edge_kind(monkeypatch) -> None:
    store = _FakeStore(known_objects={"a", "b"})
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        set_operator_edge(
            conn=None,
            src_object_kind="a", dst_object_kind="b",
            edge_kind="bogus",
        )


def test_clear_operator_edge_requires_all_keys(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        clear_operator_edge(
            conn=None,
            src_object_kind="a", dst_object_kind="",
            edge_kind="references",
        )


# --- describe_edges ---------------------------------------------------------


def test_describe_edges_both_directions(monkeypatch) -> None:
    store = _FakeStore()
    store.edges_from["table:a"] = [
        {"src_object_kind": "table:a", "dst_object_kind": "table:b",
         "edge_kind": "references"},
    ]
    store.edges_to["table:a"] = [
        {"src_object_kind": "table:z", "dst_object_kind": "table:a",
         "edge_kind": "derives_from"},
    ]
    store.install(monkeypatch)
    payload = describe_edges(conn=None, object_kind="table:a")
    assert len(payload["downstream"]) == 1
    assert len(payload["upstream"]) == 1


def test_describe_edges_rejects_invalid_direction(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        describe_edges(conn=None, object_kind="a", direction="sideways")


# --- walk_impact ------------------------------------------------------------


def test_walk_impact_follows_downstream_edges(monkeypatch) -> None:
    store = _FakeStore()
    # a -> b -> c, with a detour b -> d.
    store.edges_from["a"] = [
        {"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "references"},
    ]
    store.edges_from["b"] = [
        {"src_object_kind": "b", "dst_object_kind": "c", "edge_kind": "references"},
        {"src_object_kind": "b", "dst_object_kind": "d", "edge_kind": "references"},
    ]
    store.install(monkeypatch)
    result = walk_impact(
        conn=None, object_kind="a", direction="downstream", max_depth=5
    )
    assert set(result["nodes"]) == {"a", "b", "c", "d"}
    assert len(result["edges"]) == 3


def test_walk_impact_honors_max_depth(monkeypatch) -> None:
    store = _FakeStore()
    store.edges_from["a"] = [
        {"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "references"},
    ]
    store.edges_from["b"] = [
        {"src_object_kind": "b", "dst_object_kind": "c", "edge_kind": "references"},
    ]
    store.install(monkeypatch)
    result = walk_impact(
        conn=None, object_kind="a", direction="downstream", max_depth=1
    )
    # depth=1 means only the immediate neighbors of `a` are walked.
    assert set(result["nodes"]) == {"a", "b"}


def test_walk_impact_rejects_invalid_direction(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryLineageError):
        walk_impact(conn=None, object_kind="a", direction="sideways")


def test_walk_impact_no_self_loop_revisit(monkeypatch) -> None:
    store = _FakeStore()
    # Cycle a -> b -> a. The walker must not infinite-loop.
    store.edges_from["a"] = [
        {"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "references"},
    ]
    store.edges_from["b"] = [
        {"src_object_kind": "b", "dst_object_kind": "a", "edge_kind": "references"},
    ]
    store.install(monkeypatch)
    result = walk_impact(
        conn=None, object_kind="a", direction="downstream", max_depth=10
    )
    assert set(result["nodes"]) == {"a", "b"}
