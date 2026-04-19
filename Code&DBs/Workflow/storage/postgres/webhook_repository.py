"""Explicit Postgres repository for webhook endpoint and event persistence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import asyncpg

from .validators import PostgresWriteError, _encode_jsonb, _require_text


def _row_dict(row: object, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "webhook.write_failed",
            f"{operation} returned no row",
        )
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise PostgresWriteError(
            "webhook.write_failed",
            f"{operation} returned an invalid row type",
            details={"operation": operation, "row_type": type(row).__name__},
        ) from exc


def _normalize_enabled(enabled: bool | None) -> bool:
    if enabled is None:
        return True
    if not isinstance(enabled, bool):
        raise PostgresWriteError(
            "webhook.invalid_submission",
            "enabled must be true or false",
            details={"field": "enabled"},
        )
    return enabled


class PostgresWebhookRepository:
    """Owns webhook endpoint and inbound webhook event persistence."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def upsert_webhook_endpoint(
        self,
        *,
        slug: str,
        provider: str | None,
        secret_env_var: str | None,
        signature_header: str | None,
        signature_algorithm: str,
        target_workflow_id: str | None,
        target_trigger_id: str | None,
        filter_expression: Mapping[str, Any] | None,
        transform_spec: Mapping[str, Any] | None,
        enabled: bool | None,
    ) -> dict[str, Any]:
        """Create or update one webhook endpoint and return its identifier."""
        try:
            row = self._conn.fetchrow(
                """
                INSERT INTO webhook_endpoints (
                    slug,
                    provider,
                    secret_env_var,
                    signature_header,
                    signature_algorithm,
                    target_workflow_id,
                    target_trigger_id,
                    filter_expression,
                    transform_spec,
                    enabled
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10
                )
                ON CONFLICT (slug) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    secret_env_var = EXCLUDED.secret_env_var,
                    target_workflow_id = EXCLUDED.target_workflow_id,
                    filter_expression = EXCLUDED.filter_expression,
                    transform_spec = EXCLUDED.transform_spec,
                    enabled = EXCLUDED.enabled,
                    updated_at = now()
                RETURNING endpoint_id, slug
                """,
                _require_text(slug, field_name="slug"),
                provider or "",
                secret_env_var,
                signature_header,
                signature_algorithm or "hmac-sha256",
                target_workflow_id,
                target_trigger_id,
                _encode_jsonb(filter_expression, field_name="filter_expression")
                if filter_expression is not None
                else None,
                _encode_jsonb(transform_spec, field_name="transform_spec")
                if transform_spec is not None
                else None,
                _normalize_enabled(enabled),
            )
        except Exception as exc:
            raise PostgresWriteError(
                "webhook.write_failed",
                "failed to upsert webhook endpoint",
                details={
                    "slug": slug,
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc
        return _row_dict(row, operation="upsert_webhook_endpoint")

    def load_webhook_endpoint(self, slug: str) -> dict[str, Any] | None:
        """Load one webhook endpoint by slug."""
        row = self._conn.fetchrow(
            """
            SELECT
                endpoint_id,
                provider,
                secret_env_var,
                signature_header,
                signature_algorithm,
                target_workflow_id,
                target_trigger_id,
                filter_expression,
                transform_spec,
                enabled
            FROM webhook_endpoints
            WHERE slug = $1
            LIMIT 1
            """,
            _require_text(slug, field_name="slug"),
        )
        if row is None:
            return None
        return _row_dict(row, operation="load_webhook_endpoint")

    def insert_webhook_event(
        self,
        *,
        endpoint_id: str,
        payload: Mapping[str, Any],
        headers: Mapping[str, Any],
        signature_valid: bool | None,
    ) -> dict[str, Any]:
        """Store a webhook event payload and emit its identifier."""
        row = self._conn.fetchrow(
            """
            INSERT INTO webhook_events (endpoint_id, payload, headers, signature_valid, processing_status)
            VALUES ($1, $2::jsonb, $3::jsonb, $4, 'received')
            RETURNING event_id
            """,
            _require_text(endpoint_id, field_name="endpoint_id"),
            _encode_jsonb(payload, field_name="payload"),
            _encode_jsonb(headers, field_name="headers"),
            signature_valid,
        )
        return _row_dict(row, operation="insert_webhook_event")

    def ensure_webhook_workflow_trigger(
        self,
        *,
        endpoint_id: str,
        workflow_id: str,
    ) -> None:
        """Create the workflow-style workflow trigger for this webhook endpoint."""
        self._conn.execute(
            """
            INSERT INTO workflow_triggers (
                event_type, source_type, workflow_id, enabled,
                filter_policy, trigger_type
            ) VALUES (
                'db.webhook_events.insert',
                'webhook_events',
                $2,
                true,
                jsonb_build_object('source_id', $1, 'source_type', 'webhook_events'),
                'workflow'
            )
            ON CONFLICT DO NOTHING
            """,
            _require_text(endpoint_id, field_name="endpoint_id"),
            _require_text(workflow_id, field_name="workflow_id"),
        )

    def ensure_webhook_integration_trigger(
        self,
        *,
        endpoint_id: str,
        integration_id: str,
        integration_action: str,
        integration_args: Mapping[str, Any] | None,
    ) -> None:
        """Create the integration-style workflow trigger for this webhook endpoint."""
        self._conn.execute(
            """
            INSERT INTO workflow_triggers (
                event_type, trigger_type, enabled,
                filter_policy,
                integration_id, integration_action, integration_args
            ) VALUES (
                'db.webhook_events.insert',
                'integration',
                true,
                jsonb_build_object('source_id', $1, 'source_type', 'webhook_events'),
                $2, $3, $4::jsonb
            )
            ON CONFLICT DO NOTHING
            """,
            _require_text(endpoint_id, field_name="endpoint_id"),
            _require_text(integration_id, field_name="integration_id"),
            _require_text(integration_action, field_name="integration_action"),
            _encode_jsonb(integration_args or {}, field_name="integration_args"),
        )


__all__ = ["PostgresWebhookRepository"]
