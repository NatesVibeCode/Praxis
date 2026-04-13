"""Bounded MCP protocol endpoint runtime over endpoint authority.

This module adopts the W29 endpoint-authority seam on one explicit MCP-facing
protocol path only:

- outbound MCP `tools/call`
- transport `streamable_http`
- binding scope `protocol_mcp`
- endpoint kind `mcp_tools_call`

It does not broaden MCP cutover, infer endpoints from wrapper defaults, or fall
back to pre-authority transport guesses.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

import asyncpg

from adapters.protocol_events import ProtocolMessage
from registry.endpoint_failover import (
    ProviderEndpointAuthoritySelector,
    ProviderEndpointBindingAuthorityRecord,
    ProviderFailoverAndEndpointAuthorityRepositoryError,
    load_provider_failover_and_endpoint_authority,
)
from runtime._helpers import _fail as _shared_fail

_MCP_PROTOCOL_KIND = "mcp"
_MCP_DIRECTION = "egress"
_MCP_TOOL_CALL_METHOD = "tools/call"
_MCP_TOOL_CALL_TRANSPORT_KIND = "streamable_http"
_MCP_TOOL_CALL_BINDING_SCOPE = "protocol_mcp"
_MCP_TOOL_CALL_ENDPOINT_KIND = "mcp_tools_call"


class ProtocolEndpointRuntimeError(RuntimeError):
    """Raised when bounded protocol endpoint authority cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


_fail = partial(_shared_fail, error_type=ProtocolEndpointRuntimeError)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "protocol_endpoint_runtime.invalid_request",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _fail(
            "protocol_endpoint_runtime.invalid_request",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "protocol_endpoint_runtime.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "protocol_endpoint_runtime.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class MCPProtocolEndpointRequest:
    """Explicit request envelope for one bounded MCP protocol endpoint path."""

    provider_policy_id: str
    candidate_ref: str
    message: ProtocolMessage
    binding_scope: str = _MCP_TOOL_CALL_BINDING_SCOPE
    endpoint_kind: str = _MCP_TOOL_CALL_ENDPOINT_KIND

    def normalized(self) -> "MCPProtocolEndpointRequest":
        if not isinstance(self.message, ProtocolMessage):
            raise _fail(
                "protocol_endpoint_runtime.invalid_request",
                "message must be a ProtocolMessage",
                details={"value_type": type(self.message).__name__},
            )
        return MCPProtocolEndpointRequest(
            provider_policy_id=_require_text(
                self.provider_policy_id,
                field_name="provider_policy_id",
            ),
            candidate_ref=_require_text(
                self.candidate_ref,
                field_name="candidate_ref",
            ),
            message=self.message,
            binding_scope=_require_text(self.binding_scope, field_name="binding_scope"),
            endpoint_kind=_require_text(self.endpoint_kind, field_name="endpoint_kind"),
        )


@dataclass(frozen=True, slots=True)
class MCPProtocolEndpointResolution:
    """Authority-backed runtime resolution for one MCP `tools/call` path."""

    request: MCPProtocolEndpointRequest
    provider_endpoint_binding: ProviderEndpointBindingAuthorityRecord
    as_of: datetime
    authority: str = "registry.endpoint_failover"
    protocol_path: str = _MCP_TOOL_CALL_METHOD

    @property
    def provider_endpoint_binding_id(self) -> str:
        return self.provider_endpoint_binding.provider_endpoint_binding_id

    @property
    def endpoint_ref(self) -> str:
        return self.provider_endpoint_binding.endpoint_ref

    @property
    def endpoint_uri(self) -> str:
        return self.provider_endpoint_binding.endpoint_uri

    @property
    def auth_ref(self) -> str:
        return self.provider_endpoint_binding.auth_ref

    @property
    def transport_kind(self) -> str:
        return self.provider_endpoint_binding.transport_kind

    @property
    def tool_name(self) -> str:
        params = _require_mapping(self.request.message.body.get("params"), field_name="message.body.params")
        return _require_text(params.get("name"), field_name="message.body.params.name")


def _message_details(
    *,
    request: MCPProtocolEndpointRequest,
    as_of: datetime,
) -> dict[str, str]:
    return {
        "provider_policy_id": request.provider_policy_id,
        "candidate_ref": request.candidate_ref,
        "binding_scope": request.binding_scope,
        "endpoint_kind": request.endpoint_kind,
        "protocol_kind": request.message.metadata.protocol_kind,
        "transport_kind": request.message.metadata.transport_kind,
        "as_of": as_of.isoformat(),
    }


def _validate_mcp_tools_call_request(
    request: MCPProtocolEndpointRequest,
) -> tuple[str, str]:
    if request.message.direction != _MCP_DIRECTION:
        raise _fail(
            "protocol_endpoint_runtime.unsupported_direction",
            "protocol endpoint runtime resolves only outbound MCP messages",
            details={"direction": request.message.direction},
        )
    if request.message.metadata.protocol_kind != _MCP_PROTOCOL_KIND:
        raise _fail(
            "protocol_endpoint_runtime.unsupported_protocol",
            "protocol endpoint runtime resolves only MCP protocol messages",
            details={"protocol_kind": request.message.metadata.protocol_kind},
        )
    transport_kind = _require_text(
        request.message.metadata.transport_kind,
        field_name="message.metadata.transport_kind",
    )
    if transport_kind != _MCP_TOOL_CALL_TRANSPORT_KIND:
        raise _fail(
            "protocol_endpoint_runtime.unsupported_transport",
            "bounded MCP endpoint adoption is limited to streamable_http transport",
            details={"transport_kind": transport_kind},
        )

    body = _require_mapping(request.message.body, field_name="message.body")
    method = _require_text(body.get("method"), field_name="message.body.method")
    if method != _MCP_TOOL_CALL_METHOD:
        raise _fail(
            "protocol_endpoint_runtime.unsupported_method",
            "bounded MCP endpoint adoption is limited to tools/call",
            details={"method": method},
        )
    params = _require_mapping(body.get("params"), field_name="message.body.params")
    tool_name = _require_text(params.get("name"), field_name="message.body.params.name")
    return method, tool_name


def _authority_selector(
    *,
    request: MCPProtocolEndpointRequest,
    as_of: datetime,
) -> ProviderEndpointAuthoritySelector:
    return ProviderEndpointAuthoritySelector(
        provider_policy_id=request.provider_policy_id,
        candidate_ref=request.candidate_ref,
        binding_scope=request.binding_scope,
        endpoint_kind=request.endpoint_kind,
        as_of=as_of,
    )


def _translate_authority_failure(
    *,
    request: MCPProtocolEndpointRequest,
    as_of: datetime,
    error: ProviderFailoverAndEndpointAuthorityRepositoryError,
) -> ProtocolEndpointRuntimeError:
    details = {
        **_message_details(request=request, as_of=as_of),
        "authority_reason_code": error.reason_code,
    }
    if error.reason_code == "endpoint_failover.endpoint_missing":
        return _fail(
            "protocol_endpoint_runtime.endpoint_missing",
            "missing active MCP endpoint binding for the requested protocol path",
            details=details,
        )
    if error.reason_code == "endpoint_failover.ambiguous_endpoint_slice":
        return _fail(
            "protocol_endpoint_runtime.endpoint_ambiguous",
            "multiple active MCP endpoint bindings matched the requested protocol path",
            details=details,
        )
    if error.reason_code == "endpoint_failover.read_failed":
        return _fail(
            "protocol_endpoint_runtime.authority_read_failed",
            "failed to read MCP endpoint authority from Postgres",
            details=details,
        )
    return _fail(
        "protocol_endpoint_runtime.authority_invalid",
        "MCP endpoint authority could not be resolved safely",
        details=details,
    )


def _resolve_binding_record(
    *,
    authority: object,
    request: MCPProtocolEndpointRequest,
    as_of: datetime,
) -> ProviderEndpointBindingAuthorityRecord:
    selector = _authority_selector(request=request, as_of=as_of)
    record = authority.resolve_endpoint_binding(selector=selector)
    if record.transport_kind != _MCP_TOOL_CALL_TRANSPORT_KIND:
        raise _fail(
            "protocol_endpoint_runtime.transport_mismatch",
            "authoritative MCP endpoint binding transport does not match the bounded protocol path",
            details={
                **_message_details(request=request, as_of=as_of),
                "binding_transport_kind": record.transport_kind,
                "endpoint_ref": record.endpoint_ref,
            },
        )
    return record


async def resolve_mcp_protocol_endpoint(
    conn: asyncpg.Connection,
    *,
    request: MCPProtocolEndpointRequest,
    as_of: datetime,
) -> MCPProtocolEndpointResolution:
    """Resolve one bounded MCP `tools/call` endpoint through Postgres authority."""

    normalized_request = request.normalized()
    normalized_as_of = _normalize_as_of(as_of)
    _validate_mcp_tools_call_request(normalized_request)

    async with conn.transaction():
        try:
            authority = await load_provider_failover_and_endpoint_authority(
                conn,
                endpoint_selectors=(
                    _authority_selector(
                        request=normalized_request,
                        as_of=normalized_as_of,
                    ),
                ),
            )
        except ProviderFailoverAndEndpointAuthorityRepositoryError as exc:
            raise _translate_authority_failure(
                request=normalized_request,
                as_of=normalized_as_of,
                error=exc,
            ) from exc
        provider_endpoint_binding = _resolve_binding_record(
            authority=authority,
            request=normalized_request,
            as_of=normalized_as_of,
        )

    return MCPProtocolEndpointResolution(
        request=normalized_request,
        provider_endpoint_binding=provider_endpoint_binding,
        as_of=normalized_as_of,
    )


__all__ = [
    "MCPProtocolEndpointRequest",
    "MCPProtocolEndpointResolution",
    "ProtocolEndpointRuntimeError",
    "resolve_mcp_protocol_endpoint",
]
