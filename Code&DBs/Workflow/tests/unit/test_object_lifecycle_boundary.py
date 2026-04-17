from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from runtime import object_lifecycle
from surfaces.api.handlers import workflow_query


class _RequestStub:
    def __init__(self, body: dict[str, Any] | None = None, *, subsystems: Any | None = None) -> None:
        raw = json.dumps(body or {}).encode("utf-8")
        self.headers = {"Content-Length": str(len(raw))}
        self.rfile = io.BytesIO(raw)
        self.subsystems = subsystems or SimpleNamespace(get_pg_conn=lambda: object())
        self.sent: tuple[int, dict[str, Any]] | None = None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.sent = (status, payload)


class _RuntimeConn:
    def __init__(self) -> None:
        self.object_types: dict[str, dict[str, Any]] = {}
        self.object_fields: dict[tuple[str, str], dict[str, Any]] = {}
        self.objects: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _json_value(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def fetchval(self, query: str, *params: Any) -> Any:
        normalized = " ".join(query.split())
        if normalized == "SELECT 1 FROM object_types WHERE type_id = $1":
            return 1 if str(params[0]) in self.object_types else None
        raise AssertionError(f"unexpected fetchval query: {normalized}")

    def fetchrow(self, query: str, *params: Any) -> dict[str, Any] | None:
        normalized = " ".join(query.split())

        if normalized == "SELECT type_id, name, description, icon, created_at FROM object_types WHERE type_id = $1":
            row = self.object_types.get(str(params[0]))
            return None if row is None else dict(row)

        if normalized.startswith("INSERT INTO object_types"):
            row = {
                "type_id": str(params[0]),
                "name": params[1],
                "description": params[2],
                "icon": params[3],
                "created_at": "now",
            }
            self.object_types[row["type_id"]] = row
            return dict(row)

        if normalized.startswith("INSERT INTO object_field_registry"):
            row = {
                "type_id": str(params[0]),
                "field_name": str(params[1]),
                "label": str(params[2]),
                "field_kind": str(params[3]),
                "description": str(params[4]),
                "required": bool(params[5]),
                "default_value": self._json_value(params[6]),
                "options": self._json_value(params[7]),
                "display_order": int(params[8]),
                "binding_revision": str(params[9]),
                "decision_ref": str(params[10]),
                "retired_at": None,
            }
            self.object_fields[(row["type_id"], row["field_name"])] = row
            return dict(row)

        if normalized.startswith("INSERT INTO objects"):
            row = {
                "object_id": str(params[0]),
                "type_id": str(params[1]),
                "properties": self._json_value(params[2]),
                "status": "active",
            }
            self.objects[row["object_id"]] = row
            return dict(row)

        if normalized.startswith(
            "UPDATE objects SET properties = properties || $2::jsonb, updated_at = now() WHERE object_id = $1 RETURNING *"
        ):
            object_id = str(params[0])
            row = self.objects.get(object_id)
            if row is None:
                return None
            updated = dict(row)
            props = dict(updated.get("properties") or {})
            props.update(self._json_value(params[1]) or {})
            updated["properties"] = props
            self.objects[object_id] = updated
            return dict(updated)

        if normalized.startswith("UPDATE objects SET properties = jsonb_set("):
            document_id = str(params[0])
            card_id = str(params[1])
            row = self.objects.get(document_id)
            if row is None or row.get("type_id") != "doc_type_document":
                return None
            updated = dict(row)
            props = dict(updated.get("properties") or {})
            attached = list(props.get("attached_to") or [])
            attached.append(card_id)
            props["attached_to"] = attached
            updated["properties"] = props
            self.objects[document_id] = updated
            return {"object_id": document_id, "properties": props}

        raise AssertionError(f"unexpected fetchrow query: {normalized}")

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        normalized = " ".join(query.split())
        if normalized.startswith("DELETE FROM object_field_registry WHERE type_id = $1"):
            type_id = str(params[0])
            for key in [key for key in self.object_fields if key[0] == type_id]:
                self.object_fields.pop(key, None)
            return []
        if normalized.startswith("SELECT type_id, name, description, icon, created_at FROM object_types WHERE search_vector @@ plainto_tsquery('english', $1) ORDER BY name LIMIT $2"):
            return [dict(row) for row in self.object_types.values()][: int(params[1])]
        if normalized.startswith("SELECT type_id, name, description, icon, created_at FROM object_types ORDER BY name LIMIT $1"):
            return [dict(row) for row in self.object_types.values()][: int(params[0])]
        if normalized.startswith("SELECT type_id, field_name, label, field_kind, description, required, default_value, options, display_order, retired_at FROM object_field_registry WHERE type_id = $1 AND retired_at IS NULL ORDER BY display_order ASC, field_name ASC"):
            type_id = str(params[0])
            return [
                dict(row)
                for row in self.object_fields.values()
                if row["type_id"] == type_id and row.get("retired_at") is None
            ]
        if normalized.startswith("SELECT type_id, field_name, label, field_kind, description, required, default_value, options, display_order, retired_at FROM object_field_registry WHERE type_id = $1 ORDER BY display_order ASC, field_name ASC"):
            type_id = str(params[0])
            return [dict(row) for row in self.object_fields.values() if row["type_id"] == type_id]
        if normalized.startswith("SELECT type_id, field_name, label, field_kind, description, required, default_value, options, display_order, retired_at FROM object_field_registry WHERE retired_at IS NULL ORDER BY type_id ASC, display_order ASC, field_name ASC"):
            return [dict(row) for row in self.object_fields.values() if row.get("retired_at") is None]
        if normalized.startswith("SELECT type_id, field_name, label, field_kind, description, required, default_value, options, display_order, retired_at FROM object_field_registry ORDER BY type_id ASC, display_order ASC, field_name ASC"):
            return [dict(row) for row in self.object_fields.values()]
        if normalized == "UPDATE objects SET status = 'deleted', updated_at = now() WHERE object_id = $1":
            object_id = str(params[0])
            row = self.objects.get(object_id)
            if row is not None:
                updated = dict(row)
                updated["status"] = "deleted"
                self.objects[object_id] = updated
            return []
        raise AssertionError(f"unexpected execute query: {normalized}")

    def execute_many(self, query: str, rows: list[tuple[Any, ...]]) -> None:
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO object_field_registry ("):
            for params in rows:
                row = {
                    "type_id": str(params[0]),
                    "field_name": str(params[1]),
                    "label": str(params[2]),
                    "field_kind": str(params[3]),
                    "description": str(params[4]),
                    "required": bool(params[5]),
                    "default_value": self._json_value(params[6]),
                    "options": self._json_value(params[7]),
                    "display_order": int(params[8]),
                    "binding_revision": str(params[9]),
                    "decision_ref": str(params[10]),
                    "retired_at": None,
                }
                self.object_fields[(row["type_id"], row["field_name"])] = row
            return
        raise AssertionError(f"unexpected execute_many query: {normalized}")


def test_object_type_handler_delegates_to_runtime_owner() -> None:
    request = _RequestStub({"name": "Widget", "fields": [{"name": "title", "type": "string"}]})

    with patch.object(
        workflow_query,
        "create_object_type",
        return_value={"type_id": "widget-123456", "name": "Widget"},
    ) as create_mock:
        workflow_query._handle_object_types_post(request, "/api/object-types")

    create_mock.assert_called_once()
    assert request.sent == (200, {"type_id": "widget-123456", "name": "Widget"})


def test_object_handlers_delegate_to_runtime_owner() -> None:
    request = _RequestStub({"type_id": "widget-123456", "properties": {"title": "Initial"}})

    with patch.object(
        workflow_query,
        "create_object",
        return_value={"object_id": "obj-123", "type_id": "widget-123456", "properties": {"title": "Initial"}},
    ) as create_mock:
        workflow_query._handle_objects_post(request, "/api/objects")

    create_mock.assert_called_once()
    assert request.sent == (
        200,
        {"object_id": "obj-123", "type_id": "widget-123456", "properties": {"title": "Initial"}},
    )

    request = _RequestStub({"object_id": "obj-123", "properties": {"state": "ready"}})
    with patch.object(
        workflow_query,
        "update_object",
        return_value={"object_id": "obj-123", "properties": {"title": "Initial", "state": "ready"}},
    ) as update_mock:
        workflow_query._handle_objects_post(request, "/api/objects/update")

    update_mock.assert_called_once()
    assert request.sent == (
        200,
        {"object_id": "obj-123", "properties": {"title": "Initial", "state": "ready"}},
    )

    request = _RequestStub({"object_id": "obj-123"})
    with patch.object(workflow_query, "delete_object", return_value={"deleted": True}) as delete_mock:
        workflow_query._handle_objects_delete(request, "/api/objects/delete")

    delete_mock.assert_called_once()
    assert request.sent == (200, {"deleted": True})


def test_document_handlers_delegate_to_runtime_owner() -> None:
    request = _RequestStub(
        {
            "title": "Escalation Policy",
            "content": "Escalate after triage.",
            "doc_type": "policy",
            "tags": ["ops"],
            "attached_to": ["card-1"],
        }
    )

    with patch.object(
        workflow_query,
        "create_document",
        return_value={"document": {"id": "obj-doc-1", "title": "Escalation Policy", "doc_type": "policy"}},
    ) as create_mock:
        workflow_query._handle_documents_post(request, "/api/documents")

    create_mock.assert_called_once()
    assert request.sent == (
        200,
        {"document": {"id": "obj-doc-1", "title": "Escalation Policy", "doc_type": "policy"}},
    )

    request = _RequestStub({"card_id": "card-2"})
    with patch.object(
        workflow_query,
        "attach_document",
        return_value={"id": "obj-doc-1", "attached_to": ["card-1", "card-2"]},
    ) as attach_mock:
        workflow_query._handle_documents_post(request, "/api/documents/obj-doc-1/attach")

    attach_mock.assert_called_once()
    assert request.sent == (200, {"id": "obj-doc-1", "attached_to": ["card-1", "card-2"]})


def test_handlers_surface_runtime_validation_errors() -> None:
    request = _RequestStub({})
    workflow_query._handle_object_types_post(request, "/api/object-types")
    assert request.sent == (400, {"error": "name is required"})

    request = _RequestStub(
        {
            "title": "Escalation Policy",
            "content": "Escalate after triage.",
        }
    )
    workflow_query._handle_documents_post(request, "/api/documents")
    assert request.sent == (
        400,
        {"error": "doc_type must be one of: context, evidence, policy, reference, sop"},
    )

    request = _RequestStub({})
    workflow_query._handle_documents_post(request, "/api/documents/obj-doc-1/attach")
    assert request.sent == (400, {"error": "card_id is required"})


def test_handlers_surface_runtime_not_found_errors() -> None:
    conn = _RuntimeConn()
    subsystems = SimpleNamespace(get_pg_conn=lambda: conn)

    request = _RequestStub(
        {"type_id": "missing-type", "properties": {"title": "Initial"}},
        subsystems=subsystems,
    )
    workflow_query._handle_objects_post(request, "/api/objects")
    assert request.sent == (404, {"error": "Object type not found: missing-type"})

    request = _RequestStub(
        {
            "title": "Escalation Policy",
            "content": "Escalate after triage.",
            "doc_type": "policy",
            "tags": ["ops"],
            "attached_to": ["card-1"],
        },
        subsystems=subsystems,
    )
    workflow_query._handle_documents_post(request, "/api/documents")
    assert request.sent == (404, {"error": "Object type not found: doc_type_document"})


def test_create_object_rejects_unknown_type() -> None:
    conn = _RuntimeConn()

    try:
        object_lifecycle.create_object(conn, type_id="missing-type", properties={"title": "Initial"})
    except object_lifecycle.ObjectLifecycleBoundaryError as exc:
        assert exc.status_code == 404
        assert str(exc) == "Object type not found: missing-type"
    else:
        raise AssertionError("expected missing object type to be rejected")


def test_create_document_requires_seeded_document_type() -> None:
    conn = _RuntimeConn()

    try:
        object_lifecycle.create_document(
            conn,
            title="Escalation Policy",
            content="Escalate after triage.",
            doc_type="policy",
            tags=["ops"],
            attached_to=["card-1"],
        )
    except object_lifecycle.ObjectLifecycleBoundaryError as exc:
        assert exc.status_code == 404
        assert str(exc) == "Object type not found: doc_type_document"
    else:
        raise AssertionError("expected missing document type to be rejected")


def test_create_document_and_attach_flow_preserves_response_shape() -> None:
    conn = _RuntimeConn()
    conn.object_types["doc_type_document"] = {
        "type_id": "doc_type_document",
        "name": "Document",
    }

    created = object_lifecycle.create_document(
        conn,
        title="Escalation Policy",
        content="Escalate after triage.",
        doc_type="policy",
        tags=["ops"],
        attached_to=["card-1"],
    )

    assert created["document"]["title"] == "Escalation Policy"
    document_id = created["document"]["id"]
    assert conn.objects[document_id]["properties"]["attached_to"] == ["card-1"]

    attached = object_lifecycle.attach_document(
        conn,
        document_id=document_id,
        card_id="card-2",
    )

    assert attached == {"id": document_id, "attached_to": ["card-1", "card-2"]}
    assert conn.objects[document_id]["properties"]["attached_to"] == ["card-1", "card-2"]


def test_delete_object_marks_row_deleted_and_keeps_contract() -> None:
    conn = _RuntimeConn()
    conn.object_types["widget"] = {"type_id": "widget", "name": "Widget"}
    created = object_lifecycle.create_object(conn, type_id="widget", properties={"title": "Initial"})

    deleted = object_lifecycle.delete_object(conn, object_id=created["object_id"])

    assert deleted == {"deleted": True}
    assert conn.objects[created["object_id"]]["status"] == "deleted"


def test_upsert_object_type_returns_compiled_fields() -> None:
    conn = _RuntimeConn()

    created = object_lifecycle.upsert_object_type(
        conn,
        type_id="ticket",
        name="Ticket",
        fields=[
            {"name": "title", "type": "text", "required": True},
            {"name": "status", "type": "enum", "options": ["open", "closed"]},
        ],
    )

    assert [field["name"] for field in created["fields"]] == ["title", "status"]
    assert "property_definitions" not in created
