"""FastAPI router for incoming webhook ingestion."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from storage.postgres import PostgresWebhookRepository, get_workflow_pool
from storage.postgres.connection import SyncPostgresConnection
from runtime.integrations.webhook_receiver import ingest_webhook

webhook_ingest_router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


@webhook_ingest_router.post("/endpoints")
async def create_webhook_endpoint(request: Request) -> JSONResponse:
    """Register a new webhook endpoint. Auto-creates workflow_trigger if target_workflow_id is set."""
    body = await request.json()
    conn = SyncPostgresConnection(get_workflow_pool())
    repository = PostgresWebhookRepository(conn)

    endpoint = repository.upsert_webhook_endpoint(
        slug=body.get("slug", ""),
        provider=body.get("provider"),
        secret_env_var=body.get("secret_env_var"),
        signature_header=body.get("signature_header"),
        signature_algorithm=body.get("signature_algorithm", "hmac-sha256"),
        target_workflow_id=body.get("target_workflow_id"),
        target_trigger_id=body.get("target_trigger_id"),
        filter_expression=body.get("filter_expression"),
        transform_spec=body.get("transform_spec"),
        enabled=body.get("enabled", True),
    )

    if endpoint.get("endpoint_id"):
        if body.get("target_workflow_id"):
            repository.ensure_webhook_workflow_trigger(
                endpoint_id=endpoint["endpoint_id"],
                workflow_id=body["target_workflow_id"],
            )
        elif body.get("target_integration_id") and body.get("target_integration_action"):
            repository.ensure_webhook_integration_trigger(
                endpoint_id=endpoint["endpoint_id"],
                integration_id=body["target_integration_id"],
                integration_action=body["target_integration_action"],
                integration_args=body.get("target_integration_args"),
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
