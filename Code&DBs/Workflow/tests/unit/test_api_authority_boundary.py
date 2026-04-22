"""API mutation authority boundary tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import agent_sessions
from surfaces.api import rest
from surfaces.api.api_authority import (
    ApiAuthorityBoundaryError,
    assert_api_mutation_routes_classified,
    classify_mutating_routes,
)


def _rows_by_key(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    rows = payload["routes"]
    assert isinstance(rows, list)
    return {str(row["route_key"]): row for row in rows if isinstance(row, dict)}


def test_rest_mutating_routes_have_authority_boundaries() -> None:
    payload = classify_mutating_routes(rest.app)

    assert payload["drift"]["unknown_routes"] == []
    assert payload["drift"]["duplicate_declarations"] == []
    assert payload["mutating_route_count"] == payload["classified_route_count"]

    rows = _rows_by_key(payload)
    assert rows["POST /v1/runs"]["boundary_kind"] == "control_command_bus"
    assert rows["POST /api/auth/bootstrap/exchange"]["authority_domain_ref"] == "authority.mobile_access"
    assert rows["POST /api/operate"]["boundary_kind"] == "operation_gateway"


def test_agent_session_mutating_routes_have_authority_boundaries() -> None:
    assert_api_mutation_routes_classified(agent_sessions.app)

    rows = _rows_by_key(classify_mutating_routes(agent_sessions.app))
    assert rows["POST /agents"]["authority_domain_ref"] == "authority.agent_sessions"
    assert rows["POST /agents/{agent_id}/messages"]["authority_domain_ref"] == "authority.agent_sessions"
    assert rows["DELETE /agents/{agent_id}"]["authority_domain_ref"] == "authority.agent_sessions"


def test_unclassified_mutating_route_fails_closed() -> None:
    target_app = FastAPI()

    @target_app.post("/unclassified")
    def unclassified() -> dict[str, bool]:
        return {"ok": True}

    with pytest.raises(ApiAuthorityBoundaryError) as exc_info:
        assert_api_mutation_routes_classified(target_app)

    drift = exc_info.value.drift
    assert drift["unknown_routes"] == [
        {
            "method": "POST",
            "path": "/unclassified",
            "route_key": "POST /unclassified",
            "route_name": "unclassified",
        }
    ]


def test_rest_startup_boundary_check_fails_unclassified_routes() -> None:
    target_app = FastAPI()

    @target_app.delete("/unsafe")
    def unsafe() -> dict[str, bool]:
        return {"ok": True}

    with pytest.raises(ApiAuthorityBoundaryError):
        rest._assert_api_authority_boundary(target_app)


def test_operation_catalog_routes_classify_from_openapi_contract() -> None:
    target_app = FastAPI()

    @target_app.post(
        "/cataloged",
        openapi_extra={
            "x-praxis-operation-name": "operator.example.write",
            "x-praxis-authority-domain": "authority.example",
            "x-praxis-event-policy": "example.event.required",
            "x-praxis-projection-ref": "projection.example",
        },
    )
    def cataloged() -> dict[str, bool]:
        return {"ok": True}

    payload = classify_mutating_routes(target_app)
    assert payload["drift"]["unknown_routes"] == []

    row = _rows_by_key(payload)["POST /cataloged"]
    assert row["boundary_kind"] == "operation_catalog_gateway"
    assert row["authority_domain_ref"] == "authority.example"
    assert row["operation_name"] == "operator.example.write"
    assert row["projection_ref"] == "projection.example"


def test_api_authority_catalog_endpoint_is_queryable(monkeypatch) -> None:
    expected = {"ok": True, "routed_to": "api_authority_boundary"}
    monkeypatch.setattr(rest, "build_api_authority_payload", lambda _app: expected)
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)

    with TestClient(rest.app) as client:
        response = client.get("/api/catalog/api-authority")

    assert response.status_code == 200
    assert response.json() == expected
