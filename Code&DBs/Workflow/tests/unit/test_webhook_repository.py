from __future__ import annotations

from storage.postgres.webhook_repository import PostgresWebhookRepository


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        return {"endpoint_id": "endpoint-123", "slug": "stripe"}


def test_upsert_webhook_endpoint_persists_full_target_shape() -> None:
    conn = _FakeConn()
    repository = PostgresWebhookRepository(conn)  # type: ignore[arg-type]

    result = repository.upsert_webhook_endpoint(
        slug="stripe",
        provider="stripe",
        secret_env_var="STRIPE_SECRET",
        signature_header="Stripe-Signature",
        signature_algorithm="hmac-sha256",
        target_workflow_id=None,
        target_trigger_id=None,
        target_integration_id="notifications",
        target_integration_action="send",
        target_integration_args={"channel": "slack"},
        filter_expression={"env": "prod"},
        transform_spec={"enabled": True},
        enabled=True,
    )

    assert result == {"endpoint_id": "endpoint-123", "slug": "stripe"}
    query, args = conn.calls[0]
    normalized = " ".join(query.split())
    assert "signature_header = EXCLUDED.signature_header" in normalized
    assert "signature_algorithm = EXCLUDED.signature_algorithm" in normalized
    assert "target_trigger_id = EXCLUDED.target_trigger_id" in normalized
    assert "target_integration_id = EXCLUDED.target_integration_id" in normalized
    assert "target_integration_action = EXCLUDED.target_integration_action" in normalized
    assert "target_integration_args = EXCLUDED.target_integration_args" in normalized
    assert args[0] == "stripe"
    assert args[1] == "stripe"
    assert args[3] == "Stripe-Signature"
    assert args[4] == "hmac-sha256"
    assert args[7] == "notifications"
    assert args[8] == "send"
    assert args[9] == '{"channel":"slack"}'
    assert args[10] == '{"env":"prod"}'
    assert args[11] == '{"enabled":true}'
    assert args[12] is True
