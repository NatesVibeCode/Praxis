"""Unit tests for the classifications lineage-propagation projector.

The projector walks `data_dictionary_lineage_effective` forward from
every tagged root and emits inherited tags on every reachable
downstream object at `source=auto`. Tests stub the two query shapes
(tagged roots + flow edges) and assert the BFS output.
"""
from __future__ import annotations

from typing import Any

from memory import data_dictionary_classifications_propagation_projector as projector
from memory.data_dictionary_classifications_propagation_projector import (
    DataDictionaryClassificationsPropagationProjector,
)


class _FakeConn:
    def __init__(
        self,
        *,
        tagged_roots: list[dict[str, Any]] | None = None,
        edges: list[dict[str, Any]] | None = None,
    ) -> None:
        self._tagged = list(tagged_roots or [])
        self._edges = list(edges or [])

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM data_dictionary_classifications_effective" in sql:
            return list(self._tagged)
        if "FROM data_dictionary_lineage_effective" in sql:
            return list(self._edges)
        return []


def _install_catcher(monkeypatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _apply(conn, **kw):
        calls.append(kw)
        return {"classifications_written": len(kw.get("entries") or [])}

    monkeypatch.setattr(projector, "apply_projected_classifications", _apply)
    return calls


def test_projector_emits_empty_batch_when_no_tagged_roots(monkeypatch) -> None:
    """No roots still calls apply() with entries=[] so stale rows get pruned."""
    calls = _install_catcher(monkeypatch)
    result = DataDictionaryClassificationsPropagationProjector(_FakeConn()).run()
    assert result.ok is True
    assert len(calls) == 1
    assert calls[0]["entries"] == []


def test_projector_propagates_one_hop_downstream(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "table:users", "tag_key": "pii"}],
        edges=[
            {"src_object_kind": "table:users", "dst_object_kind": "table:audit",
             "edge_kind": "produces"},
        ],
    )
    result = DataDictionaryClassificationsPropagationProjector(conn).run()
    assert result.ok is True
    assert len(calls) == 1
    entries = calls[0]["entries"]
    assert len(entries) == 1
    e = entries[0]
    assert e["object_kind"] == "table:audit"
    assert e["tag_key"] == "pii"
    assert e["tag_value"] == "inherited"
    assert e["origin_ref"]["source_object_kind"] == "table:users"
    assert e["origin_ref"]["distance"] == 1


def test_projector_propagates_multi_hop_with_decaying_confidence(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "a", "tag_key": "pii"}],
        edges=[
            {"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "produces"},
            {"src_object_kind": "b", "dst_object_kind": "c", "edge_kind": "derives_from"},
            {"src_object_kind": "c", "dst_object_kind": "d", "edge_kind": "produces"},
        ],
    )
    DataDictionaryClassificationsPropagationProjector(conn).run()
    entries = {e["object_kind"]: e for e in calls[0]["entries"]}
    # Depth 3 is the max — d should still be included.
    assert set(entries) == {"b", "c", "d"}
    assert entries["b"]["origin_ref"]["distance"] == 1
    assert entries["c"]["origin_ref"]["distance"] == 2
    assert entries["d"]["origin_ref"]["distance"] == 3
    # Confidence monotonically decays.
    assert entries["b"]["confidence"] > entries["c"]["confidence"]
    assert entries["c"]["confidence"] > entries["d"]["confidence"]


def test_projector_keeps_shortest_distance_when_multiple_paths(monkeypatch) -> None:
    """If A→B and A→C→B both lead to B, keep the depth-1 emission."""
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "a", "tag_key": "pii"}],
        edges=[
            {"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "produces"},
            {"src_object_kind": "a", "dst_object_kind": "c", "edge_kind": "produces"},
            {"src_object_kind": "c", "dst_object_kind": "b", "edge_kind": "produces"},
        ],
    )
    DataDictionaryClassificationsPropagationProjector(conn).run()
    entries = {e["object_kind"]: e for e in calls[0]["entries"]}
    assert entries["b"]["origin_ref"]["distance"] == 1


def test_projector_does_not_cycle(monkeypatch) -> None:
    """Cyclic lineage shouldn't cause infinite loops or dup emissions."""
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "a", "tag_key": "sensitive"}],
        edges=[
            {"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "produces"},
            {"src_object_kind": "b", "dst_object_kind": "a", "edge_kind": "produces"},
        ],
    )
    DataDictionaryClassificationsPropagationProjector(conn).run()
    entries = calls[0]["entries"]
    # b gets tainted; a is the root so we never re-emit it.
    object_kinds = [e["object_kind"] for e in entries]
    assert "b" in object_kinds
    assert "a" not in object_kinds
    assert len(object_kinds) == len(set(object_kinds))


def test_projector_sql_includes_both_forward_and_reverse_edge_kinds(monkeypatch) -> None:
    seen_args: list[Any] = []

    class _CapturingConn(_FakeConn):
        def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM data_dictionary_lineage_effective" in sql:
                seen_args.append(args)
            return super().execute(sql, *args)

    _install_catcher(monkeypatch)
    conn = _CapturingConn(
        tagged_roots=[{"object_kind": "table:users", "tag_key": "pii"}],
        edges=[],
    )
    DataDictionaryClassificationsPropagationProjector(conn).run()
    assert len(seen_args) == 1
    edge_kinds = seen_args[0][0]
    # Forward flow
    assert "produces" in edge_kinds
    assert "derives_from" in edge_kinds
    # Reverse flow (FK-based linkability)
    assert "references" in edge_kinds


def test_projector_tags_fk_references_as_linkable(monkeypatch) -> None:
    """`references` edges propagate taint in REVERSE (dst → src)."""
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "table:users", "tag_key": "pii"}],
        edges=[
            # bugs has FK to users → bugs is linkable-PII
            {"src_object_kind": "table:bugs", "dst_object_kind": "table:users",
             "edge_kind": "references"},
        ],
    )
    DataDictionaryClassificationsPropagationProjector(conn).run()
    entries = calls[0]["entries"]
    assert len(entries) == 1
    e = entries[0]
    assert e["object_kind"] == "table:bugs"
    assert e["tag_value"] == "linkable"
    assert e["origin_ref"]["direction"] == "reverse"
    assert e["origin_ref"]["via_edge"] == "references"
    assert e["origin_ref"]["source_object_kind"] == "table:users"


def test_projector_with_no_flow_edges_emits_empty_batch(monkeypatch) -> None:
    """When no downstream flow exists, projector still calls apply
    (with entries=[]) so stale prior-cycle emissions get pruned."""
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "table:users", "tag_key": "pii"}],
        edges=[],
    )
    DataDictionaryClassificationsPropagationProjector(conn).run()
    assert len(calls) == 1
    assert calls[0]["entries"] == []
    assert calls[0]["projector_tag"] == "classifications_lineage_propagation"


def test_projector_fails_softly_when_storage_raises(monkeypatch) -> None:
    def _boom(conn, **kw):
        raise RuntimeError("storage down")

    monkeypatch.setattr(projector, "apply_projected_classifications", _boom)
    conn = _FakeConn(
        tagged_roots=[{"object_kind": "a", "tag_key": "pii"}],
        edges=[{"src_object_kind": "a", "dst_object_kind": "b", "edge_kind": "produces"}],
    )
    result = DataDictionaryClassificationsPropagationProjector(conn).run()
    assert result.ok is False
    assert "storage down" in (result.error or "")
