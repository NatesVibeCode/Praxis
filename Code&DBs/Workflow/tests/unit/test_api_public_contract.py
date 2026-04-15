"""Public `/v1` API contract tests."""

from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import surfaces.api.rest as rest
from surfaces.api import handlers as api_handlers
from runtime import command_handlers


def test_public_v1_requires_bearer_when_configured(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "secret-token")

    with TestClient(rest.app) as client:
        response = client.get("/v1/catalog")

    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["error_code"] == "public_api_auth_required"
    assert "traceback" not in response.text


def test_public_v1_create_run_uses_idempotency_and_returns_public_links(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.delenv("PRAXIS_API_TOKEN", raising=False)
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: "public-api-pg")

    def _fake_submit(conn, **kwargs):
        captured["conn"] = conn
        captured["kwargs"] = kwargs
        return {"command_id": "command-123", "command_status": "succeeded"}

    def _fake_render(command, *, spec_name: str | None = None, total_jobs: int | None = None):
        captured["render"] = {"command": command, "spec_name": spec_name, "total_jobs": total_jobs}
        return {
            "status": "queued",
            "run_id": "run_123",
            "command_id": "command-123",
            "command_status": "succeeded",
        }

    monkeypatch.setattr(command_handlers, "request_workflow_submit_command", _fake_submit)
    monkeypatch.setattr(command_handlers, "render_workflow_submit_response", _fake_render)

    with TestClient(rest.app) as client:
        response = client.post(
            "/v1/runs",
            headers={
                "Idempotency-Key": "idem-123",
                "X-Request-Id": "req-public-123",
            },
            json={
                "name": "Public Build",
                "phase": "build",
                "jobs": [
                    {
                        "label": "build",
                        "prompt": "Implement the change",
                    }
                ],
            },
        )

    assert response.status_code == 202
    assert response.headers["X-Request-Id"] == "req-public-123"
    assert response.headers["X-Praxis-Api-Version"] == "v1"
    assert response.json() == {
        "run_id": "run_123",
        "workflow_id": "workflow.api.v1.public.build",
        "status": "queued",
        "command_id": "command-123",
        "command_status": "succeeded",
        "request_id": "req-public-123",
        "idempotency_key": "idem-123",
        "links": {
            "self": "/v1/runs/run_123",
            "jobs": "/v1/runs/run_123/jobs",
            "cancel": "/v1/runs/run_123:cancel",
        },
    }
    assert captured["conn"] == "public-api-pg"
    assert captured["kwargs"] == {
        "requested_by_kind": "http",
        "requested_by_ref": "public_api.runs.req-public-123",
        "inline_spec": {
            "name": "Public Build",
            "workflow_id": "workflow.api.v1.public.build",
            "phase": "build",
            "workspace_ref": rest._default_workspace_ref(),
            "runtime_profile_ref": rest._default_runtime_profile_ref(),
            "jobs": [
                {
                    "label": "build",
                    "agent": "auto/build",
                    "prompt": "Implement the change",
                    "depends_on": [],
                    "read_scope": [],
                    "write_scope": [],
                    "max_attempts": 1,
                }
            ],
        },
        "repo_root": str(rest.REPO_ROOT),
        "force_fresh_run": False,
        "idempotency_key": "idem-123",
    }


def test_public_v1_validation_errors_use_problem_json(monkeypatch) -> None:
    monkeypatch.delenv("PRAXIS_API_TOKEN", raising=False)

    with TestClient(rest.app) as client:
        response = client.post("/v1/runs", json={"name": "Missing jobs"})

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    payload = response.json()
    assert payload["error_code"] == "validation_error"
    assert payload["request_id"]
    assert "traceback" not in response.text


def test_public_v1_catalog_lists_only_public_routes(monkeypatch) -> None:
    monkeypatch.delenv("PRAXIS_API_TOKEN", raising=False)
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: "public-api-pg")
    monkeypatch.setattr(rest, "build_catalog_payload", lambda _conn: {"items": []})

    with TestClient(rest.app) as client:
        response = client.get("/v1/catalog")

    assert response.status_code == 200
    payload = response.json()
    route_paths = {row["path"] for row in payload["routes"]["routes"]}
    assert "/v1/runs" in route_paths
    assert "/api/health" not in route_paths
    assert payload["runtime_catalog"] == {"items": []}


def test_internal_routes_no_longer_emit_tracebacks(monkeypatch) -> None:
    monkeypatch.setitem(
        api_handlers.ROUTES,
        "/bugs",
        lambda _subs, _body: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: object())
    monkeypatch.setattr(rest, "_record_api_route_usage", lambda *_args, **_kwargs: None)

    with TestClient(rest.app) as client:
        response = client.post("/bugs", json={"question": "status"})

    assert response.status_code == 500
    assert response.json()["error"] == "RuntimeError: boom"
    assert "traceback" not in response.text
