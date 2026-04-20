"""Unit tests for the data_dictionary_impact_admin HTTP handler."""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from runtime.data_dictionary_impact import DataDictionaryImpactError
from surfaces.api.handlers import data_dictionary_impact_admin as handler


class _RequestStub:
    def __init__(self, path: str) -> None:
        self.rfile = io.BytesIO(b"")
        self.headers = {"Content-Length": "0"}
        self.path = path
        self._conn = object()
        self.subsystems = SimpleNamespace(get_pg_conn=lambda: self._conn)
        self.sent: tuple[int, Any] | None = None

    def _send_json(self, status: int, payload: Any) -> None:
        self.sent = (status, payload)


def test_extracts_object_kind_and_query_params(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"root": kw["object_kind"], "nodes": [], "edges": [], "aggregate": {}}

    monkeypatch.setattr(handler, "impact_analysis", fake)
    stub = _RequestStub(
        "/api/data-dictionary/impact/table:bugs"
        "?direction=upstream&max_depth=3&edge_kind=produces"
    )
    handler._handle_impact(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {
        "object_kind": "table:bugs",
        "direction": "upstream",
        "max_depth": 3,
        "edge_kind": "produces",
    }


def test_defaults_direction_and_max_depth(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"nodes": []}

    monkeypatch.setattr(handler, "impact_analysis", fake)
    stub = _RequestStub("/api/data-dictionary/impact/table:bugs")
    handler._handle_impact(stub, stub.path)
    assert captured["direction"] == "downstream"
    assert captured["max_depth"] == 5
    assert captured["edge_kind"] is None


def test_decodes_percent_encoded_slash(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"nodes": []}

    monkeypatch.setattr(handler, "impact_analysis", fake)
    stub = _RequestStub("/api/data-dictionary/impact/dataset%3Aslm%2Freview")
    handler._handle_impact(stub, stub.path)
    assert captured["object_kind"] == "dataset:slm/review"


def test_rejects_non_integer_max_depth() -> None:
    stub = _RequestStub("/api/data-dictionary/impact/table:bugs?max_depth=abc")
    handler._handle_impact(stub, stub.path)
    assert stub.sent[0] == 400
    assert "max_depth" in stub.sent[1]["error"]


def test_404_when_no_object_segment() -> None:
    stub = _RequestStub("/api/data-dictionary/impact/")
    handler._handle_impact(stub, stub.path)
    assert stub.sent[0] == 404


def test_boundary_error_uses_status_code(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryImpactError("bad direction", status_code=400)

    monkeypatch.setattr(handler, "impact_analysis", boom)
    stub = _RequestStub("/api/data-dictionary/impact/table:bugs?direction=nope")
    handler._handle_impact(stub, stub.path)
    assert stub.sent == (400, {"error": "bad direction"})


def test_unexpected_exception_maps_to_500(monkeypatch) -> None:
    def boom(conn, **kw):
        raise RuntimeError("db gone")

    monkeypatch.setattr(handler, "impact_analysis", boom)
    stub = _RequestStub("/api/data-dictionary/impact/table:bugs")
    handler._handle_impact(stub, stub.path)
    assert stub.sent[0] == 500
    assert "db gone" in stub.sent[1]["error"]
