"""Unit tests for the data_dictionary_stewardship_admin HTTP handlers."""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from runtime.data_dictionary_stewardship import DataDictionaryStewardshipError
from surfaces.api.handlers import data_dictionary_stewardship_admin as handler


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


# --- GET /api/data-dictionary/stewardship (summary) --------------------


def test_summary_returns_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        handler, "stewardship_summary",
        lambda conn: {
            "stewards_by_source": {"auto": 5},
            "stewards_by_kind": {"owner": 3, "publisher": 2},
        },
    )
    stub = _RequestStub("/api/data-dictionary/stewardship")
    handler._handle_summary(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload["stewards_by_source"] == {"auto": 5}


# --- GET /api/data-dictionary/stewardship/<object_kind> ---------------


def test_describe_extracts_object_kind_and_field_path(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, field_path, include_layers):
        captured.update(
            object_kind=object_kind,
            field_path=field_path,
            include_layers=include_layers,
        )
        return {"effective": []}

    monkeypatch.setattr(handler, "describe_stewards", fake)
    stub = _RequestStub(
        "/api/data-dictionary/stewardship/table:bugs"
        "?field_path=title&include_layers=1"
    )
    handler._handle_describe(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {
        "object_kind": "table:bugs",
        "field_path": "title",
        "include_layers": True,
    }


def test_describe_decodes_percent_encoded_slash(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, **_kw):
        captured["object_kind"] = object_kind
        return {"effective": []}

    monkeypatch.setattr(handler, "describe_stewards", fake)
    stub = _RequestStub("/api/data-dictionary/stewardship/dataset%3Aslm%2Freview")
    handler._handle_describe(stub, stub.path)
    assert captured["object_kind"] == "dataset:slm/review"


def test_describe_rejects_reproject_as_object_kind() -> None:
    stub = _RequestStub("/api/data-dictionary/stewardship/reproject")
    handler._handle_describe(stub, stub.path)
    assert stub.sent[0] == 404


def test_describe_rejects_by_steward_as_object_kind() -> None:
    stub = _RequestStub("/api/data-dictionary/stewardship/by-steward")
    handler._handle_describe(stub, stub.path)
    assert stub.sent[0] == 404


def test_describe_404_on_unknown_object(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryStewardshipError("unknown", status_code=404)

    monkeypatch.setattr(handler, "describe_stewards", boom)
    stub = _RequestStub("/api/data-dictionary/stewardship/table:nope")
    handler._handle_describe(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown"})


# --- GET /api/data-dictionary/stewardship/by-steward ------------------


def test_by_steward_forwards_params(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, steward_id, steward_kind):
        captured.update(steward_id=steward_id, steward_kind=steward_kind)
        return {"matches": []}

    monkeypatch.setattr(handler, "find_by_steward", fake)
    stub = _RequestStub(
        "/api/data-dictionary/stewardship/by-steward"
        "?steward_id=alice%40company.com&steward_kind=owner"
    )
    handler._handle_by_steward(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {"steward_id": "alice@company.com", "steward_kind": "owner"}


def test_by_steward_without_steward_id_returns_400() -> None:
    stub = _RequestStub("/api/data-dictionary/stewardship/by-steward")
    handler._handle_by_steward(stub, stub.path)
    assert stub.sent[0] == 400


# --- POST /api/data-dictionary/stewardship/reproject -----------------


def test_reproject_invokes_projector(monkeypatch) -> None:
    import memory.data_dictionary_stewardship_projector as projector_module

    class _FakeProjector:
        def __init__(self, conn):
            self._conn = conn

        def run(self):
            return SimpleNamespace(ok=True, duration_ms=33.0, error=None)

    monkeypatch.setattr(
        projector_module,
        "DataDictionaryStewardshipProjector",
        _FakeProjector,
    )
    stub = _RequestStub("/api/data-dictionary/stewardship/reproject")
    handler._handle_reproject(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload == {"ok": True, "duration_ms": 33.0, "error": None}


# --- PUT /api/data-dictionary/stewardship -----------------------------


def test_set_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"steward": {"source": "operator", **kw}}

    monkeypatch.setattr(handler, "set_operator_steward", fake)
    stub = _RequestStub(
        "/api/data-dictionary/stewardship",
        body={
            "object_kind": "table:bugs",
            "steward_kind": "owner",
            "steward_id": "alice@company.com",
            "steward_type": "person",
        },
    )
    handler._handle_set(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured["object_kind"] == "table:bugs"
    assert captured["steward_id"] == "alice@company.com"
    assert captured["steward_type"] == "person"


def test_set_invalid_json_returns_400() -> None:
    stub = _RequestStub("/api/data-dictionary/stewardship")
    stub.rfile = io.BytesIO(b"{not json")
    stub.headers["Content-Length"] = "9"
    handler._handle_set(stub, stub.path)
    assert stub.sent[0] == 400


def test_set_boundary_error_status_code(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryStewardshipError("unknown object", status_code=404)

    monkeypatch.setattr(handler, "set_operator_steward", boom)
    stub = _RequestStub(
        "/api/data-dictionary/stewardship",
        body={"object_kind": "table:nope", "steward_kind": "owner", "steward_id": "x"},
    )
    handler._handle_set(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown object"})


# --- DELETE /api/data-dictionary/stewardship -------------------------


def test_clear_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"removed": True}

    monkeypatch.setattr(handler, "clear_operator_steward", fake)
    stub = _RequestStub(
        "/api/data-dictionary/stewardship",
        body={
            "object_kind": "table:bugs",
            "steward_kind": "owner",
            "steward_id": "alice",
        },
    )
    handler._handle_clear(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {
        "object_kind": "table:bugs",
        "field_path": "",
        "steward_kind": "owner",
        "steward_id": "alice",
    }
