"""Gateway-dispatched command for registering an entry in integration_registry.

Closes the leak where every integration_registry write happened through
direct INSERTs in migrations or via the bare ``upsert_integration`` helper —
no gateway, no receipt, no ``authority_events`` row. Per
``feedback_authority_layer_tighten`` the canonical path is: input model
validated by the gateway, handler runs the upsert, gateway writes
``authority_operation_receipts`` and (because ``event_required=TRUE``)
``authority_events`` with ``event_type='integration.registered'``.

Use cases:
  - Bootstrapping a built-in integration that historically lived in a
    migration seed (``dag-dispatch``, ``webhook``, ``notifications``,
    ``workflow`` — and now ``praxis_data``). Operators dispatch this
    command after the migration set lands rather than letting the seed
    INSERT bypass the authority chain.
  - Registering DB-native integrations the same way ``praxis_integration
    action='create'`` would, but with a receipt + event so the trail is
    visible.

Idempotency: ``idempotent``. Re-running with the same ``id`` reuses the
gateway receipt; the underlying ``ON CONFLICT (id) DO UPDATE`` keeps the
upsert safe to repeat.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, field_validator


_VALID_AUTH_STATUSES = {"connected", "pending", "degraded"}
_VALID_MANIFEST_SOURCES = {"static", "manifest", "api", "mcp", "ui", "connector"}


class IntegrationCapabilityInput(BaseModel):
    """One row inside the ``capabilities`` JSONB list on integration_registry."""

    action: str = Field(..., description="Action name. Required.")
    description: str = Field(default="", description="Human-readable summary.")
    method: str | None = Field(default=None, description="HTTP method when applicable.")
    path: str | None = Field(default=None, description="HTTP path template when applicable.")
    body_template: dict[str, Any] | None = Field(default=None)
    response_extract: dict[str, Any] | None = Field(default=None)


class IntegrationRegisterCommand(BaseModel):
    """Input contract for the ``integration_register`` command operation."""

    id: str = Field(..., description="integration_registry primary key.")
    name: str = Field(..., description="Display name.")
    description: str = Field(default="", description="One-paragraph summary.")
    provider: str = Field(default="http", description="Provider label (mcp|http|dag|...).")
    capabilities: list[IntegrationCapabilityInput] = Field(
        default_factory=list,
        description="Action list. At least one entry required.",
    )
    auth_status: str = Field(
        default="connected",
        description="One of connected|pending|degraded.",
    )
    icon: str | None = Field(default=None, description="UI icon hint.")
    mcp_server_id: str | None = Field(
        default=None,
        description="MCP server id when the integration dispatches through MCP.",
    )
    catalog_dispatch: bool = Field(
        default=False,
        description=(
            "When TRUE the integration is executed by routing the (id, action) "
            "tuple through the MCP catalog dispatch path. Set for built-in "
            "Praxis tools whose handler lives in surfaces/mcp/tools."
        ),
    )
    manifest_source: str = Field(
        default="api",
        description="static|manifest|api|mcp|ui|connector — origin label.",
    )
    auth_shape: dict[str, Any] = Field(
        default_factory=dict,
        description="Auth credential shape (kind, env_var, scopes, ...).",
    )
    connector_slug: str | None = Field(default=None)
    decision_ref: str | None = Field(
        default=None,
        description="Optional operator decision anchoring the registration.",
    )

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        cleaned = (value or "").strip()
        if not cleaned:
            raise ValueError("id is required")
        if len(cleaned) > 128 or len(cleaned) < 2:
            raise ValueError("id must be 2-128 chars")
        return cleaned

    @field_validator("auth_status")
    @classmethod
    def _validate_auth_status(cls, value: str) -> str:
        cleaned = (value or "").strip().lower()
        if cleaned not in _VALID_AUTH_STATUSES:
            raise ValueError(
                f"auth_status must be one of {sorted(_VALID_AUTH_STATUSES)}; got {value!r}"
            )
        return cleaned

    @field_validator("manifest_source")
    @classmethod
    def _validate_manifest_source(cls, value: str) -> str:
        cleaned = (value or "").strip().lower() or "api"
        if cleaned not in _VALID_MANIFEST_SOURCES:
            raise ValueError(
                f"manifest_source must be one of {sorted(_VALID_MANIFEST_SOURCES)}; got {value!r}"
            )
        return cleaned

    @field_validator("capabilities")
    @classmethod
    def _validate_capabilities(
        cls, value: list[IntegrationCapabilityInput]
    ) -> list[IntegrationCapabilityInput]:
        if not value:
            raise ValueError("capabilities must include at least one action")
        seen: set[str] = set()
        for cap in value:
            action = cap.action.strip()
            if not action:
                raise ValueError("every capability needs a non-empty action")
            if action in seen:
                raise ValueError(f"duplicate capability action: {action!r}")
            seen.add(action)
        return value


def _capabilities_for_upsert(
    capabilities: list[IntegrationCapabilityInput],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for cap in capabilities:
        entry: dict[str, Any] = {
            "action": cap.action.strip(),
            "description": (cap.description or "").strip(),
        }
        if cap.method:
            entry["method"] = cap.method
        if cap.path:
            entry["path"] = cap.path
        if cap.body_template is not None:
            entry["body_template"] = cap.body_template
        if cap.response_extract is not None:
            entry["response_extract"] = cap.response_extract
        out.append(entry)
    return out


def _icon_or_default(icon: str | None) -> str:
    cleaned = (icon or "").strip()
    return cleaned or "plug"


def handle_integration_register(
    command: IntegrationRegisterCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Upsert one integration_registry row and emit ``integration.registered``.

    The gateway wraps this call: it has already validated the input model,
    will write the operation receipt on return, and (because the operation
    is registered with ``event_required=TRUE`` / ``event_type=
    'integration.registered'``) emit one authority_events row hoisted from
    ``event_payload`` below.
    """
    pg_conn = subsystems.get_pg_conn()
    capabilities_json = _capabilities_for_upsert(command.capabilities)
    auth_shape_json = dict(command.auth_shape or {})

    # Single upsert. The gateway makes the surrounding receipt and event;
    # we don't need to write either ourselves. ``ON CONFLICT (id) DO UPDATE``
    # keeps re-runs idempotent in lock-step with the gateway's idempotency_policy.
    pg_conn.execute(
        """INSERT INTO integration_registry
               (id, name, description, provider, capabilities, auth_status,
                icon, mcp_server_id, catalog_dispatch, manifest_source,
                connector_slug, auth_shape)
           VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12::jsonb)
           ON CONFLICT (id) DO UPDATE SET
               name             = EXCLUDED.name,
               description      = EXCLUDED.description,
               provider         = EXCLUDED.provider,
               capabilities     = EXCLUDED.capabilities,
               auth_status      = EXCLUDED.auth_status,
               icon             = EXCLUDED.icon,
               mcp_server_id    = EXCLUDED.mcp_server_id,
               catalog_dispatch = EXCLUDED.catalog_dispatch,
               manifest_source  = EXCLUDED.manifest_source,
               connector_slug   = EXCLUDED.connector_slug,
               auth_shape       = EXCLUDED.auth_shape,
               updated_at       = now()""",
        command.id,
        command.name,
        command.description or "",
        command.provider or "http",
        json.dumps(capabilities_json),
        command.auth_status,
        _icon_or_default(command.icon),
        command.mcp_server_id,
        bool(command.catalog_dispatch),
        command.manifest_source,
        command.connector_slug,
        json.dumps(auth_shape_json),
    )

    action_names = [cap["action"] for cap in capabilities_json]

    return {
        "ok": True,
        "integration_id": command.id,
        "auth_status": command.auth_status,
        "catalog_dispatch": bool(command.catalog_dispatch),
        "manifest_source": command.manifest_source,
        "actions": action_names,
        "event_payload": {
            "integration_id": command.id,
            "name": command.name,
            "provider": command.provider or "http",
            "auth_status": command.auth_status,
            "catalog_dispatch": bool(command.catalog_dispatch),
            "manifest_source": command.manifest_source,
            "mcp_server_id": command.mcp_server_id,
            "actions": action_names,
            "action_count": len(action_names),
            "decision_ref": command.decision_ref,
        },
    }


__all__ = [
    "IntegrationCapabilityInput",
    "IntegrationRegisterCommand",
    "handle_integration_register",
]
