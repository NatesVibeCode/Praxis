from __future__ import annotations

import asyncio
import json
import types

from surfaces.api.handlers import webhook_ingest
from runtime.integrations import webhook_receiver
from storage.postgres.webhook_repository import PostgresWebhookRepository


def test_webhook_conn_uses_shared_surface_database_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_conn = object()

    monkeypatch.setattr(
        webhook_ingest,
        "workflow_database_env_for_repo",
        lambda repo_root: {
            "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
            "PATH": "",
        },
    )

    def _fake_get_workflow_pool(env=None):
        captured["env"] = env
        return "pool"

    def _fake_sync_postgres_connection(pool):
        captured["pool"] = pool
        return fake_conn

    monkeypatch.setattr(webhook_ingest, "get_workflow_pool", _fake_get_workflow_pool)
    monkeypatch.setattr(webhook_ingest, "SyncPostgresConnection", _fake_sync_postgres_connection)

    resolved = webhook_ingest._webhook_conn()

    assert resolved is fake_conn
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "PATH": "",
    }
    assert captured["pool"] == "pool"


def test_create_webhook_endpoint_rejects_mixed_targets(monkeypatch) -> None:
    monkeypatch.setattr(webhook_ingest, "_webhook_conn", lambda: (_ for _ in ()).throw(AssertionError("_webhook_conn should not be called")))

    class _FakeRequest:
        async def json(self) -> dict[str, object]:
            return {
                "slug": "stripe",
                "provider": "stripe",
                "target_workflow_id": "workflow-abc",
                "target_integration_id": "notifications",
                "target_integration_action": "send",
            }

    response = asyncio.run(webhook_ingest.create_webhook_endpoint(_FakeRequest()))
    payload = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 400
    assert "mutually exclusive" in payload["error"]


def test_create_webhook_endpoint_registers_integration_target(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeRepository:
        def __init__(self, conn) -> None:
            self.conn = conn

        def upsert_webhook_endpoint(self, **kwargs) -> dict[str, object]:
            captured["upsert"] = kwargs
            return {"endpoint_id": "endpoint-123", "slug": kwargs["slug"]}

        def ensure_webhook_workflow_trigger(self, **kwargs) -> None:
            captured["workflow_trigger"] = kwargs

        def ensure_webhook_integration_trigger(self, **kwargs) -> None:
            captured["integration_trigger"] = kwargs

    monkeypatch.setattr(webhook_ingest, "_webhook_conn", lambda: object())
    monkeypatch.setattr(webhook_ingest, "PostgresWebhookRepository", _FakeRepository)

    class _FakeRequest:
        async def json(self) -> dict[str, object]:
            return {
                "slug": "stripe",
                "provider": "stripe",
                "target_integration_id": "notifications",
                "target_integration_action": "send",
                "target_integration_args": {"channel": "slack"},
                "enabled": True,
            }

    response = asyncio.run(webhook_ingest.create_webhook_endpoint(_FakeRequest()))
    payload = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert payload == {
        "endpoint_id": "endpoint-123",
        "slug": "stripe",
        "status": "registered",
    }
    assert captured["upsert"]["target_workflow_id"] is None
    assert captured["upsert"]["target_integration_id"] == "notifications"
    assert captured["upsert"]["target_integration_action"] == "send"
    assert captured["upsert"]["target_integration_args"] == {"channel": "slack"}
    assert "workflow_trigger" not in captured
    assert captured["integration_trigger"] == {
        "endpoint_id": "endpoint-123",
        "integration_id": "notifications",
        "integration_action": "send",
        "integration_args": {"channel": "slack"},
    }


def test_webhook_ingest_emits_canonical_system_event(monkeypatch) -> None:
    emitted: dict[str, object] = {}
    inserted: dict[str, object] = {}

    class _FakeRepository:
        def __init__(self, conn) -> None:
            self.conn = conn

        def load_webhook_endpoint(self, slug: str) -> dict[str, object] | None:
            assert slug == "stripe"
            return {
                "endpoint_id": "endpoint-123",
                "provider": "stripe",
                "secret_env_var": None,
                "signature_header": None,
                "signature_algorithm": "hmac-sha256",
                "target_workflow_id": "workflow-abc",
                "target_trigger_id": "trigger-xyz",
                "filter_expression": None,
                "transform_spec": None,
                "enabled": True,
            }

        def insert_webhook_event(
            self,
            *,
            endpoint_id: str,
            payload: dict[str, object],
            headers: dict[str, object],
            signature_valid,
        ) -> dict[str, object]:
            inserted["endpoint_id"] = endpoint_id
            inserted["payload"] = payload
            inserted["headers"] = headers
            inserted["signature_valid"] = signature_valid
            return {"event_id": "event-456"}

    monkeypatch.setattr(webhook_receiver, "PostgresWebhookRepository", _FakeRepository)
    monkeypatch.setattr(
        webhook_receiver,
        "emit_system_event",
        lambda conn, **kwargs: emitted.update(kwargs),
    )

    result = webhook_receiver.ingest_webhook(
        object(),
        "stripe",
        {"type": "payment_intent.succeeded"},
        {"Stripe-Signature": "ignored"},
        b'{"type":"payment_intent.succeeded"}',
    )

    assert result.event_id == "event-456"
    assert result.trigger_action == {
        "workflow_id": "workflow-abc",
        "trigger_id": "trigger-xyz",
        "payload": {"type": "payment_intent.succeeded"},
    }
    assert inserted["endpoint_id"] == "endpoint-123"
    assert emitted["event_type"] == "db.webhook_events.insert"
    assert emitted["source_id"] == "endpoint-123"
    assert emitted["source_type"] == "webhook_events"
    assert emitted["payload"]["webhook_event_id"] == "event-456"
    assert emitted["payload"]["endpoint_slug"] == "stripe"


def test_webhook_trigger_filters_match_canonical_event_shape() -> None:
    conn = type("Conn", (), {"calls": []})()

    def _execute(query: str, *args):
        conn.calls.append((query, args))
        return []

    conn.execute = _execute  # type: ignore[attr-defined]

    repository = PostgresWebhookRepository(conn)  # type: ignore[arg-type]
    repository.ensure_webhook_workflow_trigger(
        endpoint_id="endpoint-123",
        workflow_id="workflow-abc",
    )
    repository.ensure_webhook_integration_trigger(
        endpoint_id="endpoint-123",
        integration_id="notifications",
        integration_action="send",
        integration_args={"channel": "slack"},
    )

    workflow_query, workflow_args = conn.calls[0]
    integration_query, integration_args = conn.calls[1]
    normalized_workflow = " ".join(workflow_query.split())
    normalized_integration = " ".join(integration_query.split())

    assert "jsonb_build_object('source_id', $1, 'source_type', 'webhook_events')" in normalized_workflow
    assert workflow_args == ("endpoint-123", "workflow-abc")
    assert "jsonb_build_object('source_id', $1, 'source_type', 'webhook_events')" in normalized_integration
    assert integration_args[0] == "endpoint-123"
    assert integration_args[1] == "notifications"
    assert integration_args[2] == "send"


def test_receive_webhook_returns_trigger_action(monkeypatch) -> None:
    fake_result = types.SimpleNamespace(
        event_id="event-456",
        trigger_action={"workflow_id": "workflow-abc", "trigger_id": "trigger-xyz"},
        error=None,
    )

    monkeypatch.setattr(webhook_ingest, "_webhook_conn", lambda: object())
    monkeypatch.setattr(webhook_ingest, "ingest_webhook", lambda *args, **kwargs: fake_result)

    class _FakeRequest:
        headers: dict[str, str] = {}

        async def body(self) -> bytes:
            return b"{}"

        async def json(self) -> dict[str, object]:
            return {}

    response = asyncio.run(webhook_ingest.receive_webhook("stripe", _FakeRequest()))
    payload = json.loads(response.body.decode("utf-8"))

    assert payload["event_id"] == "event-456"
    assert payload["trigger_action"] == {
        "workflow_id": "workflow-abc",
        "trigger_id": "trigger-xyz",
    }
