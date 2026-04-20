"""Unit tests for the data_dictionary_classifications_admin HTTP handlers.

Covers GET / PUT / DELETE / POST routes under
/api/data-dictionary/classifications. Runtime calls are monkeypatched — these
tests focus on path parsing, query-string handling, status codes, and error
translation.
"""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from runtime.data_dictionary_classifications import DataDictionaryClassificationError
from surfaces.api.handlers import data_dictionary_classifications_admin as handler


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


# --- GET /api/data-dictionary/classifications (summary) ------------------


def test_summary_returns_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        handler, "classification_summary",
        lambda conn: {"classifications_by_source": {"auto": 12, "operator": 1}},
    )
    stub = _RequestStub("/api/data-dictionary/classifications")
    handler._handle_summary(stub, stub.path)
    assert stub.sent == (200, {"classifications_by_source": {"auto": 12, "operator": 1}})


# --- GET /api/data-dictionary/classifications/<object_kind> -------------


def test_describe_extracts_object_kind_and_field_path(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_describe(conn, *, object_kind, field_path, include_layers):
        captured.update(
            object_kind=object_kind,
            field_path=field_path,
            include_layers=include_layers,
        )
        return {"effective": [], "object_kind": object_kind}

    monkeypatch.setattr(handler, "describe_classifications", fake_describe)
    stub = _RequestStub(
        "/api/data-dictionary/classifications/table:users"
        "?field_path=email&include_layers=1"
    )
    handler._handle_describe(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured == {
        "object_kind": "table:users",
        "field_path": "email",
        "include_layers": True,
    }


def test_describe_decodes_percent_encoded_slash(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_describe(conn, *, object_kind, **_kw):
        captured["object_kind"] = object_kind
        return {"effective": []}

    monkeypatch.setattr(handler, "describe_classifications", fake_describe)
    stub = _RequestStub("/api/data-dictionary/classifications/dataset%3Aslm%2Freview")
    handler._handle_describe(stub, stub.path)
    assert captured["object_kind"] == "dataset:slm/review"


def test_describe_rejects_reproject_as_object_kind(monkeypatch) -> None:
    stub = _RequestStub("/api/data-dictionary/classifications/reproject")
    handler._handle_describe(stub, stub.path)
    assert stub.sent[0] == 404


def test_describe_rejects_by_tag_as_object_kind(monkeypatch) -> None:
    stub = _RequestStub("/api/data-dictionary/classifications/by-tag")
    handler._handle_describe(stub, stub.path)
    assert stub.sent[0] == 404


def test_describe_404_on_unknown_object(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryClassificationError("unknown", status_code=404)

    monkeypatch.setattr(handler, "describe_classifications", boom)
    stub = _RequestStub("/api/data-dictionary/classifications/table:nope")
    handler._handle_describe(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown"})


# --- GET /api/data-dictionary/classifications/by-tag --------------------


def test_by_tag_forwards_params(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_find(conn, *, tag_key, tag_value):
        captured.update(tag_key=tag_key, tag_value=tag_value)
        return {"matches": []}

    monkeypatch.setattr(handler, "find_by_tag", fake_find)
    stub = _RequestStub(
        "/api/data-dictionary/classifications/by-tag?tag_key=pii&tag_value=email"
    )
    handler._handle_by_tag(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {"tag_key": "pii", "tag_value": "email"}


def test_by_tag_without_tag_key_returns_400() -> None:
    stub = _RequestStub("/api/data-dictionary/classifications/by-tag")
    handler._handle_by_tag(stub, stub.path)
    assert stub.sent[0] == 400


# --- POST /api/data-dictionary/classifications/reproject ---------------


def test_reproject_invokes_projector(monkeypatch) -> None:
    import memory.data_dictionary_classifications_projector as projector_module

    class _FakeProjector:
        def __init__(self, conn):
            self._conn = conn

        def run(self):
            return SimpleNamespace(ok=True, duration_ms=17.0, error=None)

    monkeypatch.setattr(
        projector_module,
        "DataDictionaryClassificationsProjector",
        _FakeProjector,
    )
    stub = _RequestStub("/api/data-dictionary/classifications/reproject")
    handler._handle_reproject(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload == {"ok": True, "duration_ms": 17.0, "error": None}


# --- PUT /api/data-dictionary/classifications --------------------------


def test_set_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_set(conn, **kw):
        captured.update(kw)
        return {"classification": {"source": "operator", **kw}}

    monkeypatch.setattr(handler, "set_operator_classification", fake_set)
    stub = _RequestStub(
        "/api/data-dictionary/classifications",
        body={
            "object_kind": "table:users",
            "field_path": "email",
            "tag_key": "pii",
            "tag_value": "email",
            "confidence": 0.9,
            "metadata": {"note": "manual"},
        },
    )
    handler._handle_set(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured["object_kind"] == "table:users"
    assert captured["field_path"] == "email"
    assert captured["tag_key"] == "pii"
    assert captured["tag_value"] == "email"
    assert captured["confidence"] == 0.9


def test_set_invalid_json_returns_400() -> None:
    stub = _RequestStub("/api/data-dictionary/classifications")
    stub.rfile = io.BytesIO(b"{not json")
    stub.headers["Content-Length"] = "9"
    handler._handle_set(stub, stub.path)
    assert stub.sent[0] == 400


def test_set_boundary_error_status_code(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryClassificationError("unknown object", status_code=404)

    monkeypatch.setattr(handler, "set_operator_classification", boom)
    stub = _RequestStub(
        "/api/data-dictionary/classifications",
        body={"object_kind": "table:missing", "tag_key": "pii"},
    )
    handler._handle_set(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown object"})


# --- DELETE /api/data-dictionary/classifications -----------------------


def test_clear_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_clear(conn, **kw):
        captured.update(kw)
        return {"removed": True}

    monkeypatch.setattr(handler, "clear_operator_classification", fake_clear)
    stub = _RequestStub(
        "/api/data-dictionary/classifications",
        body={
            "object_kind": "table:users",
            "field_path": "email",
            "tag_key": "pii",
        },
    )
    handler._handle_clear(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured["object_kind"] == "table:users"
    assert captured["tag_key"] == "pii"
    assert captured["field_path"] == "email"
