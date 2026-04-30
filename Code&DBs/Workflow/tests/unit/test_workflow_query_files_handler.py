from __future__ import annotations

from typing import Any

from surfaces.api.handlers import workflow_query


class _Subsystems:
    def get_pg_conn(self) -> object:
        return object()


class _Request:
    def __init__(self, path: str) -> None:
        self.path = path
        self.subsystems = _Subsystems()
        self.status_code: int | None = None
        self.payload: dict[str, Any] | None = None

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self.payload = payload


def test_files_get_strips_query_before_deciding_route_shape(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_list_files(_pg: object, **kwargs: object) -> list[dict[str, object]]:
        captured.update(kwargs)
        return [{"id": "file_1", "filename": "notes.md"}]

    monkeypatch.setattr(workflow_query, "list_files", _fake_list_files)
    request = _Request("/api/files?scope=instance")

    workflow_query._handle_files_get(request, "/api/files?scope=instance")

    assert request.status_code == 200
    assert request.payload == {"files": [{"id": "file_1", "filename": "notes.md"}], "count": 1}
    assert captured == {
        "scope": "instance",
        "workflow_id": None,
        "step_id": None,
    }

