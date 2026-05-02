"""FastAPI router for incoming webhook ingestion."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from storage.postgres import PostgresWebhookRepository, get_workflow_pool
from storage.postgres.connection import SyncPostgresConnection
from runtime.integrations.webhook_receiver import ingest_webhook
from surfaces._workflow_database import workflow_database_env_for_repo

from ._shared import REPO_ROOT

webhook_ingest_router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


def _webhook_conn() -> SyncPostgresConnection:
    env = workflow_database_env_for_repo(REPO_ROOT)
    return SyncPostgresConnection(get_workflow_pool(env=env))


@webhook_ingest_router.post("/endpoints")
async def create_webhook_endpoint(request: Request) -> JSONResponse:
    """Register a new webhook endpoint. Auto-creates workflow_trigger if target_workflow_id is set."""
    body = await request.json()

    target_workflow_id = body.get("target_workflow_id")
    target_integration_id = body.get("target_integration_id")
    target_integration_action = body.get("target_integration_action")
    target_integration_args = body.get("target_integration_args")
    target_agent_principal_ref = body.get("target_agent_principal_ref")

    has_workflow_target = bool(target_workflow_id)
    has_integration_target = bool(target_integration_id or target_integration_action)
    has_agent_target = bool(target_agent_principal_ref)
    target_count = sum([has_workflow_target, has_integration_target, has_agent_target])
    if target_count > 1:
        return JSONResponse(
            {
                "error": (
                    "target_workflow_id, target_integration_id/action, and "
                    "target_agent_principal_ref are mutually exclusive — pick one"
                )
            },
            status_code=400,
        )

    conn = _webhook_conn()
    repository = PostgresWebhookRepository(conn)

    endpoint = repository.upsert_webhook_endpoint(
        slug=body.get("slug", ""),
        provider=body.get("provider"),
        secret_env_var=body.get("secret_env_var"),
        signature_header=body.get("signature_header"),
        signature_algorithm=body.get("signature_algorithm", "hmac-sha256"),
        target_workflow_id=target_workflow_id,
        target_trigger_id=body.get("target_trigger_id"),
        target_integration_id=target_integration_id,
        target_integration_action=target_integration_action,
        target_integration_args=target_integration_args,
        filter_expression=body.get("filter_expression"),
        transform_spec=body.get("transform_spec"),
        enabled=body.get("enabled", True),
    )

    endpoint_id = endpoint.get("endpoint_id")
    if endpoint_id:
        if has_workflow_target:
            repository.ensure_webhook_workflow_trigger(
                endpoint_id=endpoint_id,
                workflow_id=target_workflow_id,
            )
        elif has_integration_target:
            repository.ensure_webhook_integration_trigger(
                endpoint_id=endpoint_id,
                integration_id=target_integration_id,
                integration_action=target_integration_action,
                integration_args=target_integration_args,
            )
        elif has_agent_target:
            # Phase A trigger convergence: webhook → agent_wake.
            # Updates the endpoint with the generic target shape so the
            # trigger evaluator's agent_wake branch consumes events from
            # this endpoint. Also creates a workflow_triggers row keyed
            # to the endpoint so the system_events → trigger evaluator
            # path picks up webhook events.
            import json as _json

            target_args = body.get("target_agent_args") or {}
            conn.execute(
                """UPDATE webhook_endpoints
                      SET target_kind = 'agent_wake',
                          target_ref = $2,
                          target_args = $3::jsonb,
                          updated_at = now()
                    WHERE endpoint_id = $1""",
                endpoint_id,
                target_agent_principal_ref,
                _json.dumps(target_args),
            )
            trigger_id = f"webhook_{endpoint_id}_agent"
            conn.execute(
                """INSERT INTO workflow_triggers (
                       id, workflow_id, event_type, enabled,
                       target_kind, target_ref, target_args
                   )
                   VALUES ($1, NULL, 'webhook.received', TRUE,
                           'agent_wake', $2, $3::jsonb)
                   ON CONFLICT (id) DO UPDATE SET
                       enabled = TRUE,
                       target_kind = EXCLUDED.target_kind,
                       target_ref = EXCLUDED.target_ref,
                       target_args = EXCLUDED.target_args""",
                trigger_id,
                target_agent_principal_ref,
                _json.dumps({"trigger_kind": "webhook", **target_args}),
            )

    return JSONResponse({
        "endpoint_id": endpoint.get("endpoint_id"),
        "slug": endpoint.get("slug"),
        "status": "registered",
        "target_kind": (
            "agent_wake" if has_agent_target else (
                "workflow" if has_workflow_target else (
                    "integration" if has_integration_target else None
                )
            )
        ),
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

    conn = _webhook_conn()
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
        "trigger_action": result.trigger_action,
    })
