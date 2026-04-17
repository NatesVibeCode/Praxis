from __future__ import annotations

from dataclasses import asdict
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

import runtime.operation_catalog as runtime_operation_catalog
import surfaces.api.handlers.workflow_query as workflow_query
import surfaces.api.operation_catalog_authority as operation_catalog_authority
import surfaces.api.rest as rest


class _RequestStub:
    def __init__(self, pg: Any) -> None:
        self.subsystems = SimpleNamespace(get_pg_conn=lambda: pg)
        self.path = "/api/catalog/operations"
        self.sent: tuple[int, dict[str, Any]] | None = None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.sent = (status, payload)


def test_build_operation_catalog_payload_returns_versioned_contract(monkeypatch) -> None:
    resolved = runtime_operation_catalog.ResolvedOperationDefinition(
        operation_ref="workflow-build-suggest-next",
        operation_name="workflow_build.suggest_next",
        source_kind="operation_query",
        operation_kind="query",
        http_method="POST",
        http_path="/api/workflows/{workflow_id}/build/suggest-next",
        input_model_ref="runtime.operations.commands.suggest_next.SuggestNextNodesCommand",
        handler_ref="runtime.operations.commands.suggest_next.handle_suggest_next_nodes",
        authority_ref="authority.capability_catalog",
        projection_ref="projection.capability_catalog",
        posture="observe",
        idempotency_policy="read_only",
        enabled=True,
        operation_enabled=True,
        source_policy_ref="operation-query",
        source_policy_enabled=True,
        binding_revision="binding.operation_catalog_registry.bootstrap.20260416",
        decision_ref="decision.operation_catalog_registry.bootstrap.20260416",
    )
    source_policy = runtime_operation_catalog.OperationSourcePolicyRecord(
        policy_ref="operation-query",
        source_kind="operation_query",
        posture="observe",
        idempotency_policy="read_only",
        enabled=True,
        binding_revision="binding.operation_catalog_source_policy_registry.bootstrap.20260416",
        decision_ref="decision.operation_catalog_source_policy_registry.bootstrap.20260416",
    )

    monkeypatch.setattr(
        operation_catalog_authority,
        "list_resolved_operation_definitions",
        lambda pg, include_disabled=True, limit=500: [resolved],
    )
    monkeypatch.setattr(
        operation_catalog_authority,
        "list_operation_source_policies",
        lambda pg, include_disabled=True, limit=50: [source_policy],
    )

    payload = operation_catalog_authority.build_operation_catalog_payload(object())

    assert payload["routed_to"] == "operation_catalog"
    assert payload["contract_version"] == 1
    assert payload["contract"]["query_path"] == "/api/catalog/operations"
    assert payload["count"] == 1
    assert payload["source_policy_count"] == 1
    assert payload["operations"][0] == asdict(resolved)
    assert payload["source_policies"][0] == asdict(source_policy)


def test_legacy_operation_catalog_handler_uses_shared_authority(monkeypatch) -> None:
    expected = {"operations": [{"operation_name": "workflow_build.mutate"}], "source_policies": []}
    monkeypatch.setattr(workflow_query, "build_operation_catalog_payload", lambda _pg: expected)
    request = _RequestStub(pg=object())

    workflow_query._handle_operation_catalog_get(request, "/api/catalog/operations")

    assert request.sent == (200, expected)


def test_rest_operation_catalog_endpoint_uses_shared_authority(monkeypatch) -> None:
    expected = {
        "routed_to": "operation_catalog",
        "contract_version": 1,
        "contract": {"query_path": "/api/catalog/operations"},
        "operations": [],
        "count": 0,
        "source_policies": [],
        "source_policy_count": 0,
        "generated_at": "2026-04-16T00:00:00+00:00",
    }
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: object())
    monkeypatch.setattr(rest, "build_operation_catalog_payload", lambda _pg: expected)
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)

    with TestClient(rest.app) as client:
        response = client.get("/api/catalog/operations")

    assert response.status_code == 200
    assert response.json() == expected
