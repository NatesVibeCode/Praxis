"""Unit tests for the data_dictionary_lineage_admin HTTP handlers.

Covers GET / PUT / DELETE / POST /api/data-dictionary/lineage routes.
Runtime calls are monkeypatched — these tests focus on path parsing,
query-string handling, status codes, and error translation.
"""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

import pytest

from runtime.data_dictionary_lineage import DataDictionaryLineageError
from surfaces.api.handlers import data_dictionary_lineage_admin as handler


class _RequestStub:
    def __init__(self, path: str, body: dict | None = None) -> None:
        raw = json.dumps(body or {}).encode()
        self.rfile = io.BytesIO(raw)
        self.headers = {"Content-Length": str(len(raw))}
        self.path = path
        self._conn = object()
        self.subsystems = SimpleNamespace(get_pg_conn=lambda: self._conn)
        self.sent: tuple[int, Any] | None = None

    def _send_json(self, status: int, payload: Any) -> None:
        self.sent = (status, payload)


# --- GET /api/data-dictionary/lineage (summary) -------------------------


def test_summary_returns_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        handler, "lineage_summary",
        lambda conn: {"edges_by_source": {"auto": 170, "operator": 3}},
    )
    stub = _RequestStub("/api/data-dictionary/lineage")
    handler._handle_summary(stub, stub.path)
    assert stub.sent == (200, {"edges_by_source": {"auto": 170, "operator": 3}})


# --- GET /api/data-dictionary/lineage/<object_kind> ---------------------


def test_describe_extracts_object_kind_and_direction(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_describe(conn, *, object_kind, direction, edge_kind, include_layers):
        captured.update(
            object_kind=object_kind, direction=direction,
            edge_kind=edge_kind, include_layers=include_layers,
        )
        return {"upstream": [], "downstream": []}

    monkeypatch.setattr(handler, "describe_edges", fake_describe)
    stub = _RequestStub(
        "/api/data-dictionary/lineage/table:workflow_runs"
        "?direction=upstream&edge_kind=references&include_layers=1"
    )
    handler._handle_describe(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured == {
        "object_kind": "table:workflow_runs",
        "direction": "upstream",
        "edge_kind": "references",
        "include_layers": True,
    }


def test_describe_decodes_percent_encoded_slash_in_object_kind(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_describe(conn, *, object_kind, **_kw):
        captured["object_kind"] = object_kind
        return {"upstream": [], "downstream": []}

    monkeypatch.setattr(handler, "describe_edges", fake_describe)
    # dataset:slm/review with '/' URL-encoded so the router doesn't split on it.
    stub = _RequestStub("/api/data-dictionary/lineage/dataset%3Aslm%2Freview")
    handler._handle_describe(stub, stub.path)
    assert captured["object_kind"] == "dataset:slm/review"


def test_describe_rejects_reproject_as_object_kind(monkeypatch) -> None:
    # `/api/data-dictionary/lineage/reproject` is the POST endpoint, not a
    # describable object_kind.
    stub = _RequestStub("/api/data-dictionary/lineage/reproject")
    handler._handle_describe(stub, stub.path)
    assert stub.sent[0] == 404


def test_describe_404_on_unknown_object(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryLineageError("unknown", status_code=404)

    monkeypatch.setattr(handler, "describe_edges", boom)
    stub = _RequestStub("/api/data-dictionary/lineage/table:nope")
    handler._handle_describe(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown"})


# --- GET /api/data-dictionary/lineage/<object_kind>/impact --------------


def test_impact_forwards_max_depth_and_direction(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_walk(conn, *, object_kind, direction, max_depth, edge_kind):
        captured.update(
            object_kind=object_kind, direction=direction,
            max_depth=max_depth, edge_kind=edge_kind,
        )
        return {"root": object_kind, "nodes": [object_kind], "edges": []}

    monkeypatch.setattr(handler, "walk_impact", fake_walk)
    stub = _RequestStub(
        "/api/data-dictionary/lineage/table:a/impact?direction=upstream&max_depth=4"
    )
    handler._handle_impact(stub, stub.path)
    assert captured == {
        "object_kind": "table:a",
        "direction": "upstream",
        "max_depth": 4,
        "edge_kind": None,
    }


def test_impact_rejects_non_integer_max_depth(monkeypatch) -> None:
    stub = _RequestStub(
        "/api/data-dictionary/lineage/table:a/impact?max_depth=notanumber"
    )
    handler._handle_impact(stub, stub.path)
    assert stub.sent[0] == 400


def test_impact_404_when_path_missing_impact_segment(monkeypatch) -> None:
    # The impact suffix is required; the generic describe route handles the rest.
    stub = _RequestStub("/api/data-dictionary/lineage/table:a")
    handler._handle_impact(stub, stub.path)
    assert stub.sent[0] == 404


# --- POST /api/data-dictionary/lineage/reproject ------------------------


def test_reproject_invokes_projector(monkeypatch) -> None:
    import memory.data_dictionary_lineage_projector as projector_module

    class _FakeProjector:
        def __init__(self, conn):
            self._conn = conn

        def run(self):
            return SimpleNamespace(ok=True, duration_ms=42.0, error=None)

    monkeypatch.setattr(
        projector_module, "DataDictionaryLineageProjector", _FakeProjector
    )
    stub = _RequestStub("/api/data-dictionary/lineage/reproject")
    handler._handle_reproject(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload == {"ok": True, "duration_ms": 42.0, "error": None}


# --- PUT /api/data-dictionary/lineage (set operator edge) ---------------


def test_set_edge_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_set(conn, **kw):
        captured.update(kw)
        return {"edge": {"source": "operator", **kw}}

    monkeypatch.setattr(handler, "set_operator_edge", fake_set)
    stub = _RequestStub(
        "/api/data-dictionary/lineage",
        body={
            "src_object_kind": "table:a",
            "dst_object_kind": "table:b",
            "edge_kind": "same_as",
            "confidence": 0.75,
            "metadata": {"note": "manual"},
        },
    )
    handler._handle_set_edge(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured["src_object_kind"] == "table:a"
    assert captured["dst_object_kind"] == "table:b"
    assert captured["edge_kind"] == "same_as"
    assert captured["confidence"] == 0.75
    assert captured["metadata"] == {"note": "manual"}


def test_set_edge_invalid_json_returns_400() -> None:
    stub = _RequestStub("/api/data-dictionary/lineage")
    # Overwrite rfile with invalid JSON body.
    stub.rfile = io.BytesIO(b"{not json")
    stub.headers["Content-Length"] = "9"
    handler._handle_set_edge(stub, stub.path)
    assert stub.sent[0] == 400


def test_set_edge_boundary_error_status_code(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryLineageError("bad edge_kind", status_code=400)

    monkeypatch.setattr(handler, "set_operator_edge", boom)
    stub = _RequestStub(
        "/api/data-dictionary/lineage",
        body={
            "src_object_kind": "a", "dst_object_kind": "b",
            "edge_kind": "bogus",
        },
    )
    handler._handle_set_edge(stub, stub.path)
    assert stub.sent == (400, {"error": "bad edge_kind"})


# --- DELETE /api/data-dictionary/lineage (clear operator edge) ----------


def test_clear_edge_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_clear(conn, **kw):
        captured.update(kw)
        return {"removed": True}

    monkeypatch.setattr(handler, "clear_operator_edge", fake_clear)
    stub = _RequestStub(
        "/api/data-dictionary/lineage",
        body={
            "src_object_kind": "table:a",
            "dst_object_kind": "table:b",
            "edge_kind": "references",
        },
    )
    handler._handle_clear_edge(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured["src_object_kind"] == "table:a"
    assert captured["dst_object_kind"] == "table:b"
    assert captured["edge_kind"] == "references"
