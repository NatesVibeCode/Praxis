"""Unit tests for the data_dictionary_admin HTTP handlers.

Covers GET / PUT / DELETE / POST /api/data-dictionary routes. Runtime calls
(`list_object_kinds`, `describe_object`, `set_operator_override`,
`clear_operator_override`) are monkeypatched — these tests focus on request
parsing, status codes, and error translation.
"""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

import pytest

from runtime.data_dictionary import DataDictionaryBoundaryError
from surfaces.api.handlers import data_dictionary_admin as handler


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


# --- GET /api/data-dictionary -------------------------------------------


def test_list_returns_rows_with_count(monkeypatch) -> None:
    monkeypatch.setattr(
        handler,
        "list_object_kinds",
        lambda conn, category=None: [
            {"object_kind": "table:a"},
            {"object_kind": "table:b"},
        ],
    )
    stub = _RequestStub("/api/data-dictionary")
    handler._handle_list(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload["count"] == 2
    assert payload["objects"][0]["object_kind"] == "table:a"


def test_list_parses_category_query_param(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_list(conn, category=None):
        captured["category"] = category
        return []

    monkeypatch.setattr(handler, "list_object_kinds", fake_list)
    stub = _RequestStub("/api/data-dictionary?category=integration")
    handler._handle_list(stub, stub.path)
    assert captured["category"] == "integration"


def test_list_sends_400_on_boundary_error(monkeypatch) -> None:
    def boom(conn, category=None):
        raise DataDictionaryBoundaryError("bad category", status_code=400)

    monkeypatch.setattr(handler, "list_object_kinds", boom)
    stub = _RequestStub("/api/data-dictionary?category=junk")
    handler._handle_list(stub, stub.path)
    assert stub.sent == (400, {"error": "bad category"})


def test_list_sends_500_on_unexpected_error(monkeypatch) -> None:
    def boom(conn, category=None):
        raise RuntimeError("db down")

    monkeypatch.setattr(handler, "list_object_kinds", boom)
    stub = _RequestStub("/api/data-dictionary")
    handler._handle_list(stub, stub.path)
    status, payload = stub.sent
    assert status == 500
    assert "db down" in payload["error"]


# --- GET /api/data-dictionary/<kind> ------------------------------------


def test_describe_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        handler,
        "describe_object",
        lambda conn, object_kind, include_layers: {
            "object": {"object_kind": object_kind},
            "fields": [],
            "entries_by_source": {},
        },
    )
    stub = _RequestStub("/api/data-dictionary/table:orders")
    handler._handle_describe(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload["object"]["object_kind"] == "table:orders"


def test_describe_forwards_include_layers_flag(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_describe(conn, object_kind, include_layers):
        captured["include_layers"] = include_layers
        return {"object": {"object_kind": object_kind}, "fields": [], "entries_by_source": {}}

    monkeypatch.setattr(handler, "describe_object", fake_describe)
    stub = _RequestStub("/api/data-dictionary/table:orders?include_layers=1")
    handler._handle_describe(stub, stub.path)
    assert captured["include_layers"] is True

    stub = _RequestStub("/api/data-dictionary/table:orders?include_layers=true")
    handler._handle_describe(stub, stub.path)
    assert captured["include_layers"] is True

    stub = _RequestStub("/api/data-dictionary/table:orders?include_layers=no")
    handler._handle_describe(stub, stub.path)
    assert captured["include_layers"] is False


def test_describe_404_when_reproject_collides_with_kind_segment() -> None:
    # The describe handler explicitly refuses /reproject so the POST route wins.
    stub = _RequestStub("/api/data-dictionary/reproject")
    handler._handle_describe(stub, stub.path)
    assert stub.sent == (404, {"error": "not found"})


def test_describe_decodes_percent_encoded_slash_in_object_kind(monkeypatch) -> None:
    """object_kind can contain '/' (e.g. 'dataset:slm/review').

    The frontend URL-encodes it with encodeURIComponent so it lands as
    ``dataset%3Aslm%2Freview`` in the URL path. The handler must split
    on ``/`` first (so %2F stays inside one segment) and *then* unquote,
    producing one segment that describes the real kind.
    """
    captured: dict[str, Any] = {}

    def fake_describe(conn, object_kind, include_layers):
        captured["object_kind"] = object_kind
        return {"object": {"object_kind": object_kind}, "fields": [], "entries_by_source": {}}

    monkeypatch.setattr(handler, "describe_object", fake_describe)
    stub = _RequestStub("/api/data-dictionary/dataset%3Aslm%2Freview")
    handler._handle_describe(stub, stub.path)
    assert captured["object_kind"] == "dataset:slm/review"
    assert stub.sent[0] == 200


def test_describe_404_when_more_than_one_segment() -> None:
    # Two segments belongs to PUT/DELETE, not GET describe.
    stub = _RequestStub("/api/data-dictionary/table:orders/id")
    handler._handle_describe(stub, stub.path)
    assert stub.sent == (404, {"error": "not found"})


def test_describe_sends_404_from_boundary_error(monkeypatch) -> None:
    def missing(conn, object_kind, include_layers):
        raise DataDictionaryBoundaryError("unknown", status_code=404)

    monkeypatch.setattr(handler, "describe_object", missing)
    stub = _RequestStub("/api/data-dictionary/ghost")
    handler._handle_describe(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown"})


# --- PUT /api/data-dictionary/<kind>/<path> -----------------------------


def test_set_override_requires_object_body(monkeypatch) -> None:
    stub = _RequestStub("/api/data-dictionary/table:orders/status", body=None)
    # _read_json_body returns {} for empty — handler treats empty dict as ok since it's an object
    monkeypatch.setattr(
        handler,
        "set_operator_override",
        lambda conn, **k: {"object_kind": k["object_kind"], "field_path": k["field_path"]},
    )
    handler._handle_set_override(stub, stub.path)
    assert stub.sent[0] == 200


def test_set_override_rejects_non_object_body(monkeypatch) -> None:
    stub = _RequestStub("/api/data-dictionary/table:orders/status")
    # Force a non-dict body
    raw = json.dumps([1, 2, 3]).encode()
    stub.rfile = io.BytesIO(raw)
    stub.headers = {"Content-Length": str(len(raw))}
    handler._handle_set_override(stub, stub.path)
    assert stub.sent == (400, {"error": "body must be an object"})


def test_set_override_rejects_invalid_json() -> None:
    stub = _RequestStub("/api/data-dictionary/table:orders/status")
    stub.rfile = io.BytesIO(b"{not json")
    stub.headers = {"Content-Length": "9"}
    handler._handle_set_override(stub, stub.path)
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 400
    assert "invalid JSON" in payload["error"]


def test_set_override_404_when_segments_mismatched() -> None:
    stub = _RequestStub("/api/data-dictionary/table:orders")
    handler._handle_set_override(stub, stub.path)
    assert stub.sent == (404, {"error": "not found"})


def test_set_override_forwards_fields_to_authority(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_set(conn, **kw):
        captured.update(kw)
        return {"object_kind": kw["object_kind"], "field_path": kw["field_path"]}

    monkeypatch.setattr(handler, "set_operator_override", fake_set)
    stub = _RequestStub(
        "/api/data-dictionary/table:orders/status",
        body={
            "field_kind": "enum",
            "label": "Order Status",
            "description": "lifecycle state",
            "required": True,
            "default_value": "open",
            "valid_values": ["open", "closed"],
            "examples": ["open"],
            "deprecation_notes": "",
            "display_order": 20,
            "metadata": {"owner": "orders"},
        },
    )
    handler._handle_set_override(stub, stub.path)
    assert stub.sent is not None
    assert stub.sent[0] == 200
    assert captured["object_kind"] == "table:orders"
    assert captured["field_path"] == "status"
    assert captured["field_kind"] == "enum"
    assert captured["required"] is True
    assert captured["metadata"] == {"owner": "orders"}


def test_set_override_surfaces_boundary_error_as_client_error(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryBoundaryError("invalid override", status_code=400)

    monkeypatch.setattr(handler, "set_operator_override", boom)
    stub = _RequestStub(
        "/api/data-dictionary/table:orders/status",
        body={"label": "X"},
    )
    handler._handle_set_override(stub, stub.path)
    assert stub.sent == (400, {"error": "invalid override"})


# --- DELETE /api/data-dictionary/<kind>/<path> --------------------------


def test_clear_override_delegates_to_authority(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_clear(conn, **kw):
        captured.update(kw)
        return {"object_kind": kw["object_kind"], "field_path": kw["field_path"], "removed": True}

    monkeypatch.setattr(handler, "clear_operator_override", fake_clear)
    stub = _RequestStub("/api/data-dictionary/table:orders/status")
    handler._handle_clear_override(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload["removed"] is True
    assert captured == {"object_kind": "table:orders", "field_path": "status"}


def test_clear_override_404_when_segments_mismatched() -> None:
    stub = _RequestStub("/api/data-dictionary/table:orders")
    handler._handle_clear_override(stub, stub.path)
    assert stub.sent == (404, {"error": "not found"})


# --- POST /api/data-dictionary/reproject --------------------------------


def test_reproject_runs_projector_and_returns_status(monkeypatch) -> None:
    monkeypatch.setattr(
        handler,
        "refresh_data_dictionary_authority",
        lambda conn: {
            "ok": True,
            "duration_ms": 12.5,
            "error": None,
            "modules": [{"name": "data_dictionary_projector", "ok": True}],
        },
    )

    stub = _RequestStub("/api/data-dictionary/reproject")
    handler._handle_reproject(stub, stub.path)
    assert stub.sent == (
        200,
        {
            "ok": True,
            "duration_ms": 12.5,
            "error": None,
            "modules": [{"name": "data_dictionary_projector", "ok": True}],
        },
    )


def test_reproject_reports_errors_from_projector(monkeypatch) -> None:
    monkeypatch.setattr(
        handler,
        "refresh_data_dictionary_authority",
        lambda conn: {
            "ok": False,
            "duration_ms": 5.0,
            "error": "DataDictionaryProjector: tables: boom",
            "modules": [{"name": "data_dictionary_projector", "ok": False}],
        },
    )

    stub = _RequestStub("/api/data-dictionary/reproject")
    handler._handle_reproject(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload["ok"] is False
    assert "tables" in payload["error"]


# --- Route tuple shape --------------------------------------------------


def test_get_routes_cover_list_and_describe() -> None:
    # Ensure list matches exact prefix and describe matches everything under it
    list_matcher, list_handler = handler.DATA_DICTIONARY_GET_ROUTES[0]
    describe_matcher, describe_handler = handler.DATA_DICTIONARY_GET_ROUTES[1]
    assert list_matcher("/api/data-dictionary") is True
    assert list_matcher("/api/data-dictionary/x") is False
    assert describe_matcher("/api/data-dictionary/x") is True
    assert describe_matcher("/api/data-dictionary") is False
    assert list_handler is handler._handle_list
    assert describe_handler is handler._handle_describe


def test_reproject_route_is_exact() -> None:
    matcher, target = handler.DATA_DICTIONARY_POST_ROUTES[0]
    assert matcher("/api/data-dictionary/reproject") is True
    assert matcher("/api/data-dictionary/reproject/") is False
    assert target is handler._handle_reproject


def test_put_and_delete_routes_share_prefix_matcher() -> None:
    put_matcher, put_handler = handler.DATA_DICTIONARY_PUT_ROUTES[0]
    delete_matcher, delete_handler = handler.DATA_DICTIONARY_DELETE_ROUTES[0]
    assert put_matcher("/api/data-dictionary/x/y") is True
    assert delete_matcher("/api/data-dictionary/x/y") is True
    assert put_handler is handler._handle_set_override
    assert delete_handler is handler._handle_clear_override
