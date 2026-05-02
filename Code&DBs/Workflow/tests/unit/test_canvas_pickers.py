from __future__ import annotations

import json

from surfaces.api.handlers import canvas_pickers


class _FakePg:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    def fetch(self, query: str, *args):
        self.queries.append((query, args))
        normalized = " ".join(query.split())
        assert "FROM webhook_endpoints e" in normalized
        assert "workflow_triggers" in normalized
        assert "e.enabled = TRUE" in normalized
        assert "t.trigger_type = 'workflow'" in normalized
        assert "t.event_type = 'db.webhook_events.insert'" in normalized
        assert "COALESCE(t.filter_policy->>'source_id', '') = e.endpoint_id" in normalized
        return [
            {
                "endpoint_id": "endpoint-123",
                "slug": "stripe",
                "provider": "stripe",
                "enabled": True,
            }
        ]


class _FakeRequest:
    def __init__(self, pg: _FakePg) -> None:
        self.subsystems = type("_Subs", (), {"get_pg_conn": lambda _self: pg})()
        self.status_code: int | None = None
        self.body: dict[str, object] | None = None

    def _send_json(self, status_code: int, body: dict[str, object]) -> None:
        self.status_code = status_code
        self.body = body


def test_handle_webhook_sources_only_suggests_live_routed_endpoints() -> None:
    pg = _FakePg()
    request = _FakeRequest(pg)

    canvas_pickers._handle_webhook_sources(request, "/api/canvas/pickers/webhook-sources")

    assert request.status_code == 200
    assert request.body == {
        "sources": [
            {
                "value": "stripe",
                "label": "stripe (stripe)",
                "provider": "stripe",
                "endpoint_id": "endpoint-123",
                "enabled": True,
            }
        ],
        "count": 1,
    }
    assert len(pg.queries) == 1


class _FakePgIntegrationProviders:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def fetch(self, query: str, *args):
        del args
        self.queries.append(query)
        normalized = " ".join(query.split())
        assert "FROM integration_registry" in normalized
        assert "GROUP BY ir.provider" in normalized
        assert "effective_provider_circuit_breaker_state" in normalized
        return list(self.rows)


def test_handle_integration_providers_reads_registry_only() -> None:
    pg = _FakePgIntegrationProviders(
        [
            {"provider": "dag", "name": "DAG Dispatch"},
            {"provider": "http", "name": "Webhook"},
        ]
    )
    request = _FakeRequest(pg)

    canvas_pickers._handle_integration_providers(request, "/api/canvas/pickers/integration-providers")

    assert request.status_code == 200
    assert request.body == {
        "providers": [
            {"value": "dag", "label": "DAG Dispatch (dag)"},
            {"value": "http", "label": "Webhook (http)"},
        ],
        "count": 2,
    }


def test_handle_integration_providers_db_error_returns_500() -> None:
    class _BrokenPg:
        def fetch(self, *_a, **_k):
            raise RuntimeError("db down")

    request = _FakeRequest(_BrokenPg())
    canvas_pickers._handle_integration_providers(request, "/api/canvas/pickers/integration-providers")

    assert request.status_code == 500
    assert request.body is not None
    assert "error" in request.body
