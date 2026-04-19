"""Incoming webhook receiver — accepts webhooks from external services.

Validates HMAC signatures (Stripe, GitHub), stores events durably,
and routes to workflow triggers.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Mapping

from storage.postgres.webhook_repository import PostgresWebhookRepository
from runtime.system_events import emit_system_event

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


@dataclass
class WebhookIngestionResult:
    event_id: Optional[str]
    signature_valid: Optional[bool]
    trigger_action: Optional[dict[str, Any]]
    error: Optional[str] = None


def verify_signature(
    payload_bytes: bytes,
    signature: str,
    secret: str,
    algorithm: str = "hmac-sha256",
) -> bool:
    """Verify webhook HMAC signature.

    Supports:
    - hmac-sha256 with Stripe format: 't=...,v1=...'
    - hmac-sha256 with plain hex
    - hmac-sha1 with GitHub format: 'sha1=...' or 'sha256=...'
    """
    if not signature or not secret:
        return False

    if algorithm == "hmac-sha256":
        # Stripe: t=timestamp,v1=signature
        if ",v1=" in signature:
            parts = dict(p.split("=", 1) for p in signature.split(",") if "=" in p)
            sig_hex = parts.get("v1", "")
            timestamp = parts.get("t", "")
            signed_payload = f"{timestamp}.".encode() + payload_bytes
            expected = hmac.new(
                secret.encode(), signed_payload, hashlib.sha256
            ).hexdigest()
            return hmac.compare_digest(expected, sig_hex)

        # GitHub sha256: 'sha256=hexdigest'
        if signature.startswith("sha256="):
            sig_hex = signature[7:]
            expected = hmac.new(
                secret.encode(), payload_bytes, hashlib.sha256
            ).hexdigest()
            return hmac.compare_digest(expected, sig_hex)

        # Plain hex
        expected = hmac.new(
            secret.encode(), payload_bytes, hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    if algorithm == "hmac-sha1":
        if signature.startswith("sha1="):
            sig_hex = signature[5:]
        else:
            sig_hex = signature
        expected = hmac.new(
            secret.encode(), payload_bytes, hashlib.sha1
        ).hexdigest()
        return hmac.compare_digest(expected, sig_hex)

    return False


def ingest_webhook(
    conn: "SyncPostgresConnection",
    endpoint_slug: str,
    payload: Mapping[str, Any],
    headers: Mapping[str, Any],
    raw_body: bytes,
) -> WebhookIngestionResult:
    """Ingest an incoming webhook: validate signature, store event, route to trigger."""
    repository = PostgresWebhookRepository(conn)
    endpoint = repository.load_webhook_endpoint(slug=endpoint_slug)
    if endpoint is None:
        return WebhookIngestionResult(
            event_id=None, signature_valid=None, trigger_action=None,
            error=f"unknown endpoint: {endpoint_slug}",
        )

    if not endpoint["enabled"]:
        return WebhookIngestionResult(
            event_id=None, signature_valid=None, trigger_action=None,
            error="endpoint disabled",
        )

    # Signature verification
    signature_valid = None
    if endpoint["secret_env_var"]:
        secret = os.environ.get(endpoint["secret_env_var"], "")
        sig_header = endpoint["signature_header"] or ""
        signature = ""
        for key, value in headers.items():
            if key.lower() == sig_header.lower():
                signature = value
                break
        signature_valid = verify_signature(
            raw_body, signature, secret,
            algorithm=endpoint["signature_algorithm"] or "hmac-sha256",
        )
        if not signature_valid:
            return WebhookIngestionResult(
                event_id=None, signature_valid=False, trigger_action=None,
                error="signature verification failed",
            )

    event_row = repository.insert_webhook_event(
        endpoint_id=endpoint["endpoint_id"],
        payload=payload,
        headers=headers,
        signature_valid=signature_valid,
    )
    event_id = event_row["event_id"] if event_row else None

    # Wake the canonical trigger engine. The worker already owns trigger
    # evaluation; the webhook ingress only emits the authoritative event.
    try:
        emit_system_event(
            conn,
            event_type="db.webhook_events.insert",
            source_id=str(endpoint["endpoint_id"]),
            source_type="webhook_events",
            payload={
                "endpoint_id": str(endpoint["endpoint_id"]),
                "endpoint_slug": endpoint_slug,
                "webhook_event_id": event_id,
                "signature_valid": signature_valid,
                "target_workflow_id": endpoint.get("target_workflow_id"),
                "target_trigger_id": endpoint.get("target_trigger_id"),
            },
        )
    except Exception:
        pass

    # Emit to event log
    try:
        from runtime.event_log import emit
        emit(
            conn, "webhook", "webhook_received",
            entity_id=event_id or "",
            entity_kind="webhook_event",
            payload={"endpoint_slug": endpoint_slug, "provider": endpoint["provider"]},
            emitted_by="webhook_receiver",
        )
    except Exception:
        pass

    # Trigger routing
    trigger_action = None
    if endpoint["target_workflow_id"]:
        # Check filter expression if present
        should_trigger = True
        transformed_payload = payload
        if endpoint.get("filter_expression"):
            from runtime.integrations.trigger_filter import evaluate_trigger
            decision = evaluate_trigger(payload, dict(endpoint))
            should_trigger = decision.should_trigger
            transformed_payload = decision.transformed_payload

        if should_trigger:
            trigger_action = {
                "workflow_id": endpoint["target_workflow_id"],
                "trigger_id": endpoint.get("target_trigger_id"),
                "payload": transformed_payload,
            }

    return WebhookIngestionResult(
        event_id=event_id,
        signature_valid=signature_valid,
        trigger_action=trigger_action,
    )
