"""FastAPI router for incoming webhook ingestion."""

from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from storage.postgres import get_workflow_pool
from storage.postgres.connection import SyncPostgresConnection
from runtime.integrations.webhook_receiver import ingest_webhook

webhook_ingest_router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


@webhook_ingest_router.post("/endpoints")
async def create_webhook_endpoint(request: Request) -> JSONResponse:
    """Register a new webhook endpoint. Auto-creates workflow_trigger if target_workflow_id is set."""
    body = await request.json()
    conn = SyncPostgresConnection(get_workflow_pool())

    rows = conn.execute(
        """INSERT INTO webhook_endpoints (slug, provider, secret_env_var, signature_header,
               signature_algorithm, target_workflow_id, target_trigger_id,
               filter_expression, transform_spec, enabled)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)
           ON CONFLICT (slug) DO UPDATE SET
               provider = EXCLUDED.provider,
               secret_env_var = EXCLUDED.secret_env_var,
               target_workflow_id = EXCLUDED.target_workflow_id,
               filter_expression = EXCLUDED.filter_expression,
               transform_spec = EXCLUDED.transform_spec,
               enabled = EXCLUDED.enabled,
               updated_at = now()
           RETURNING endpoint_id, slug""",
        body.get("slug", ""),
        body.get("provider", ""),
        body.get("secret_env_var"),
        body.get("signature_header"),
        body.get("signature_algorithm", "hmac-sha256"),
        body.get("target_workflow_id"),
        body.get("target_trigger_id"),
        json.dumps(body.get("filter_expression")) if body.get("filter_expression") else None,
        json.dumps(body.get("transform_spec")) if body.get("transform_spec") else None,
        body.get("enabled", True),
    )
    endpoint = rows[0] if rows else {}

    # Auto-register a trigger so the trigger evaluator fires when a webhook
    # event for this endpoint appears in system_events.
    if endpoint.get("endpoint_id"):
        if body.get("target_workflow_id"):
            _ensure_webhook_trigger(conn, endpoint["endpoint_id"], body["target_workflow_id"])
        elif body.get("target_integration_id") and body.get("target_integration_action"):
            _ensure_integration_trigger(
                conn,
                endpoint["endpoint_id"],
                body["target_integration_id"],
                body["target_integration_action"],
                body.get("target_integration_args"),
            )

    return JSONResponse({
        "endpoint_id": endpoint.get("endpoint_id"),
        "slug": endpoint.get("slug"),
        "status": "registered",
    })


@webhook_ingest_router.post("/{slug}")
async def receive_webhook(slug: str, request: Request) -> JSONResponse:
    """Receive an incoming webhook, validate signature, store event."""
    raw_body = await request.body()
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    headers = dict(request.headers)

    conn = SyncPostgresConnection(get_workflow_pool())
    result = ingest_webhook(conn, slug, payload, headers, raw_body)

    if result.error:
        if "unknown endpoint" in result.error:
            return JSONResponse({"error": result.error}, status_code=404)
        if "signature" in result.error:
            return JSONResponse({"error": result.error}, status_code=401)
        if "disabled" in result.error:
            return JSONResponse({"error": result.error}, status_code=403)
        return JSONResponse({"error": result.error}, status_code=400)

    return JSONResponse({
        "event_id": result.event_id,
        "status": "received",
    })


def _ensure_webhook_trigger(
    conn: SyncPostgresConnection,
    endpoint_id: str,
    workflow_id: str,
) -> None:
    """Create a workflow_trigger that fires when this endpoint receives a webhook."""
    conn.execute(
        """INSERT INTO workflow_triggers (
               event_type, source_type, workflow_id, enabled,
               filter_policy, trigger_type
           ) VALUES (
               'db.webhook_events.insert',
               'webhook_events',
               $2,
               true,
               jsonb_build_object('endpoint_id', $1),
               'workflow'
           )
           ON CONFLICT DO NOTHING""",
        endpoint_id, workflow_id,
    )


def _ensure_integration_trigger(
    conn: SyncPostgresConnection,
    endpoint_id: str,
    integration_id: str,
    integration_action: str,
    integration_args: dict | None = None,
) -> None:
    """Create a workflow_trigger that calls an integration when this endpoint receives a webhook."""
    conn.execute(
        """INSERT INTO workflow_triggers (
               event_type, trigger_type, enabled,
               filter_policy,
               integration_id, integration_action, integration_args
           ) VALUES (
               'db.webhook_events.insert',
               'integration',
               true,
               jsonb_build_object('endpoint_id', $1),
               $2, $3, $4::jsonb
           )
           ON CONFLICT DO NOTHING""",
        endpoint_id, integration_id, integration_action,
        json.dumps(integration_args or {}),
    )
