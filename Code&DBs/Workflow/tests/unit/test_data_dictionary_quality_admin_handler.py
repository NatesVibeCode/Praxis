"""Unit tests for the data_dictionary_quality_admin HTTP handlers.

Covers GET / POST / PUT / DELETE routes under
/api/data-dictionary/quality. Runtime calls are monkeypatched — these
tests focus on path parsing, query-string handling, status codes, and
error translation.
"""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from runtime.data_dictionary_quality import DataDictionaryQualityError
from surfaces.api.handlers import data_dictionary_quality_admin as handler


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


# --- GET /api/data-dictionary/quality (summary) ---------------------------


def test_summary_returns_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        handler, "quality_summary",
        lambda conn: {
            "rules_by_source": {"auto": 7},
            "latest_runs_by_status": {"pass": 5, "fail": 2},
        },
    )
    stub = _RequestStub("/api/data-dictionary/quality")
    handler._handle_summary(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload["rules_by_source"] == {"auto": 7}


def test_summary_translates_boundary_error(monkeypatch) -> None:
    def boom(conn):
        raise DataDictionaryQualityError("nope", status_code=500)

    monkeypatch.setattr(handler, "quality_summary", boom)
    stub = _RequestStub("/api/data-dictionary/quality")
    handler._handle_summary(stub, stub.path)
    assert stub.sent == (500, {"error": "nope"})


# --- GET /api/data-dictionary/quality/rules ------------------------------


def test_list_rules_forwards_filters(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, field_path, include_layers):
        captured.update(
            object_kind=object_kind,
            field_path=field_path,
            include_layers=include_layers,
        )
        return {"effective": [], "object_kind": object_kind}

    monkeypatch.setattr(handler, "describe_rules", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality/rules"
        "?object_kind=table:bugs&field_path=title&include_layers=1"
    )
    handler._handle_list_rules(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured == {
        "object_kind": "table:bugs",
        "field_path": "title",
        "include_layers": True,
    }


def test_list_rules_defaults_no_filters(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, field_path, include_layers):
        captured.update(
            object_kind=object_kind,
            field_path=field_path,
            include_layers=include_layers,
        )
        return {"effective": []}

    monkeypatch.setattr(handler, "describe_rules", fake)
    stub = _RequestStub("/api/data-dictionary/quality/rules")
    handler._handle_list_rules(stub, stub.path)
    assert captured["object_kind"] is None
    assert captured["include_layers"] is False


# --- GET /api/data-dictionary/quality/runs -------------------------------


def test_list_runs_forwards_params(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, status, limit):
        captured.update(object_kind=object_kind, status=status, limit=limit)
        return {"runs": []}

    monkeypatch.setattr(handler, "latest_runs", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality/runs"
        "?object_kind=table:bugs&status=fail&limit=25"
    )
    handler._handle_list_runs(stub, stub.path)
    status, _ = stub.sent
    assert status == 200
    assert captured == {"object_kind": "table:bugs", "status": "fail", "limit": 25}


def test_list_runs_rejects_non_int_limit() -> None:
    stub = _RequestStub("/api/data-dictionary/quality/runs?limit=abc")
    handler._handle_list_runs(stub, stub.path)
    assert stub.sent[0] == 400


# --- GET /api/data-dictionary/quality/runs/<obj>/<rule_kind> -------------


def test_run_history_parses_path(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, rule_kind, field_path, limit):
        captured.update(
            object_kind=object_kind,
            rule_kind=rule_kind,
            field_path=field_path,
            limit=limit,
        )
        return {"history": []}

    monkeypatch.setattr(handler, "run_history", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality/runs/table:bugs/not_null"
        "?field_path=title&limit=10"
    )
    handler._handle_run_history(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {
        "object_kind": "table:bugs",
        "rule_kind": "not_null",
        "field_path": "title",
        "limit": 10,
    }


def test_run_history_decodes_percent_encoded_object_kind(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind, rule_kind, **_kw):
        captured.update(object_kind=object_kind, rule_kind=rule_kind)
        return {"history": []}

    monkeypatch.setattr(handler, "run_history", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality/runs/dataset%3Aslm%2Freview/row_count_min"
    )
    handler._handle_run_history(stub, stub.path)
    assert captured == {
        "object_kind": "dataset:slm/review",
        "rule_kind": "row_count_min",
    }


def test_run_history_404_on_short_path() -> None:
    # Missing rule_kind segment.
    stub = _RequestStub("/api/data-dictionary/quality/runs/table:bugs")
    handler._handle_run_history(stub, stub.path)
    assert stub.sent[0] == 404


# --- POST /api/data-dictionary/quality/reproject -------------------------


def test_reproject_invokes_projector(monkeypatch) -> None:
    import memory.data_dictionary_quality_projector as projector_module

    class _FakeProjector:
        def __init__(self, conn):
            self._conn = conn

        def run(self):
            return SimpleNamespace(ok=True, duration_ms=42.0, error=None)

    monkeypatch.setattr(
        projector_module, "DataDictionaryQualityProjector", _FakeProjector,
    )
    stub = _RequestStub("/api/data-dictionary/quality/reproject")
    handler._handle_reproject(stub, stub.path)
    status, payload = stub.sent
    assert status == 200
    assert payload == {"ok": True, "duration_ms": 42.0, "error": None}


# --- POST /api/data-dictionary/quality/evaluate --------------------------


def test_evaluate_forwards_object_kind(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind):
        captured["object_kind"] = object_kind
        return {"evaluated": 0, "passed": 0, "failed": 0, "errored": 0}

    monkeypatch.setattr(handler, "evaluate_all", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality/evaluate",
        body={"object_kind": "table:bugs"},
    )
    handler._handle_evaluate(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {"object_kind": "table:bugs"}


def test_evaluate_handles_missing_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, *, object_kind):
        captured["object_kind"] = object_kind
        return {"evaluated": 0}

    monkeypatch.setattr(handler, "evaluate_all", fake)
    # Empty body -> object_kind=None.
    stub = _RequestStub("/api/data-dictionary/quality/evaluate")
    handler._handle_evaluate(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {"object_kind": None}


# --- PUT /api/data-dictionary/quality ------------------------------------


def test_set_rule_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"rule": {"source": "operator", **kw}}

    monkeypatch.setattr(handler, "set_operator_rule", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality",
        body={
            "object_kind": "table:bugs",
            "field_path": "title",
            "rule_kind": "not_null",
            "severity": "error",
            "description": "titles must be non-null",
            "expression": {},
        },
    )
    handler._handle_set_rule(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured["object_kind"] == "table:bugs"
    assert captured["rule_kind"] == "not_null"
    assert captured["severity"] == "error"


def test_set_rule_boundary_error_404(monkeypatch) -> None:
    def boom(conn, **kw):
        raise DataDictionaryQualityError("unknown object", status_code=404)

    monkeypatch.setattr(handler, "set_operator_rule", boom)
    stub = _RequestStub(
        "/api/data-dictionary/quality",
        body={"object_kind": "table:nope", "rule_kind": "not_null"},
    )
    handler._handle_set_rule(stub, stub.path)
    assert stub.sent == (404, {"error": "unknown object"})


def test_set_rule_rejects_non_object_body() -> None:
    stub = _RequestStub("/api/data-dictionary/quality")
    stub.rfile = io.BytesIO(b"[1, 2, 3]")
    stub.headers["Content-Length"] = "9"
    handler._handle_set_rule(stub, stub.path)
    assert stub.sent[0] == 400


# --- DELETE /api/data-dictionary/quality --------------------------------


def test_clear_rule_forwards_body(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake(conn, **kw):
        captured.update(kw)
        return {"removed": True}

    monkeypatch.setattr(handler, "clear_operator_rule", fake)
    stub = _RequestStub(
        "/api/data-dictionary/quality",
        body={
            "object_kind": "table:bugs",
            "field_path": "title",
            "rule_kind": "not_null",
        },
    )
    handler._handle_clear_rule(stub, stub.path)
    assert stub.sent[0] == 200
    assert captured == {
        "object_kind": "table:bugs",
        "field_path": "title",
        "rule_kind": "not_null",
    }


def test_clear_rule_invalid_json_returns_400() -> None:
    stub = _RequestStub("/api/data-dictionary/quality")
    stub.rfile = io.BytesIO(b"{bad")
    stub.headers["Content-Length"] = "4"
    handler._handle_clear_rule(stub, stub.path)
    assert stub.sent[0] == 400
