"""CQRS authority contracts for MCP and catalog-backed CLI tools.

The MCP catalog is the shared front door for JSON-RPC, ``workflow tools``,
and most convenience CLI aliases. This module makes the authority path
inspectable before a tool mutates durable state.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog


MUTATING_RISKS = frozenset({"write", "launch", "dispatch", "session"})


class FrontdoorAuthorityError(RuntimeError):
    """Raised when a front-door tool lacks an authority contract."""

    def __init__(self, message: str, *, drift: dict[str, Any]) -> None:
        super().__init__(message)
        self.drift = drift


@dataclass(frozen=True, slots=True)
class McpToolAuthorityContract:
    tool_name: str
    selector_field: str | None
    selector_value: str
    risk: str
    surface: str
    tier: str
    boundary_kind: str
    authority_domain_ref: str
    cqrs_entrypoint: str
    receipt_policy: str
    event_policy: str
    workspace_authority_ref: str
    operation_name: str
    legacy_status: str = "active"
    migration_target: str | None = None
    decision_ref: str = "decision.cqrs_authority_unification.20260422"

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _SurfaceAuthority:
    authority_domain_ref: str
    boundary_kind: str
    cqrs_entrypoint: str
    receipt_policy: str
    event_policy: str
    migration_target: str | None = None


_READ_AUTHORITY = _SurfaceAuthority(
    authority_domain_ref="authority.read_models",
    boundary_kind="read_projection",
    cqrs_entrypoint="projection_or_query_handler",
    receipt_policy="surface_usage_event",
    event_policy="not_required",
)

_MCP_SURFACE_AUTHORITY: dict[str, _SurfaceAuthority] = {
    "code": _SurfaceAuthority(
        "authority.compile_index",
        "domain_authority",
        "compile_index_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "data": _SurfaceAuthority(
        "authority.data_pipeline",
        "domain_authority",
        "operation_catalog_or_domain_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "evidence": _SurfaceAuthority(
        "authority.evidence",
        "domain_authority",
        "operation_catalog_or_domain_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "integration": _SurfaceAuthority(
        "authority.integrations",
        "domain_authority",
        "operation_catalog_or_integration_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "knowledge": _SurfaceAuthority(
        "authority.knowledge",
        "domain_authority",
        "operation_catalog_or_domain_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "operations": _SurfaceAuthority(
        "authority.operations",
        "domain_authority",
        "operation_catalog_or_domain_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "operator": _SurfaceAuthority(
        "authority.operator_control",
        "domain_authority",
        "operator_control_frontdoor_or_operation_catalog",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "planning": _SurfaceAuthority(
        "authority.manifest",
        "domain_authority",
        "operation_catalog_or_manifest_authority",
        "authority_operation_receipt",
        "authority_event",
        "operation_catalog_gateway",
    ),
    "research": _SurfaceAuthority(
        "authority.workflow_runs",
        "control_command_bus",
        "workflow_control_command_bus",
        "control_command_receipt",
        "workflow_event",
        "operation_catalog_gateway",
    ),
    "session": _SurfaceAuthority(
        "authority.workflow_mcp_session",
        "workflow_mcp_session_authority",
        "workflow_mcp_session_authority",
        "workflow_mcp_session_receipt",
        "workflow_mcp_session_event",
        None,
    ),
    "submissions": _SurfaceAuthority(
        "authority.workflow_submissions",
        "sealed_submission_authority",
        "workflow_submission_authority",
        "workflow_submission_receipt",
        "workflow_submission_event",
        None,
    ),
    "workflow": _SurfaceAuthority(
        "authority.workflow_runs",
        "control_command_bus",
        "workflow_control_command_bus",
        "control_command_receipt",
        "workflow_event",
        "operation_catalog_gateway",
    ),
}


def _selector_values(definition: McpToolDefinition) -> list[str]:
    values = list(definition.selector_enum)
    default = definition.selector_default or definition.default_action
    if default and default not in values:
        values.insert(0, default)
    return values or [default or "call"]


def _surface_authority(definition: McpToolDefinition, risk: str) -> _SurfaceAuthority | None:
    surface = definition.cli_surface
    if risk not in MUTATING_RISKS:
        return _READ_AUTHORITY
    return _MCP_SURFACE_AUTHORITY.get(surface)


def _contract_for_selector(
    definition: McpToolDefinition,
    selector_value: object,
) -> McpToolAuthorityContract:
    selector = str(selector_value or definition.default_action or "call").strip()
    risk = definition.risk_for_selector(selector)
    authority = _surface_authority(definition, risk)
    if authority is None:
        authority = _SurfaceAuthority(
            authority_domain_ref="authority.unclassified",
            boundary_kind="unclassified",
            cqrs_entrypoint="unclassified",
            receipt_policy="missing",
            event_policy="missing",
        )
    return McpToolAuthorityContract(
        tool_name=definition.name,
        selector_field=definition.selector_field,
        selector_value=selector,
        risk=risk,
        surface=definition.cli_surface,
        tier=definition.cli_tier,
        boundary_kind=authority.boundary_kind,
        authority_domain_ref=authority.authority_domain_ref,
        cqrs_entrypoint=authority.cqrs_entrypoint,
        receipt_policy=authority.receipt_policy,
        event_policy=authority.event_policy,
        workspace_authority_ref="registry_workspace_base_path_authority",
        operation_name=f"mcp.{definition.name}.{selector}",
        legacy_status="active" if risk not in MUTATING_RISKS else "legacy_visible",
        migration_target=authority.migration_target,
    )


def tool_authority_contracts(definition: McpToolDefinition) -> list[dict[str, Any]]:
    """Return one authority contract per selector value for a tool."""

    return [
        _contract_for_selector(definition, selector).to_payload()
        for selector in _selector_values(definition)
    ]


def tool_authority_contract_for_params(
    definition: McpToolDefinition,
    params: Mapping[str, Any] | None,
) -> McpToolAuthorityContract:
    source = dict(params or {})
    selector_field = definition.selector_field
    selector_value = (
        source.get(selector_field, definition.selector_default or definition.default_action)
        if selector_field is not None
        else definition.default_action
    )
    return _contract_for_selector(definition, selector_value)


def classify_mcp_tool_catalog(
    catalog: Mapping[str, McpToolDefinition] | None = None,
) -> dict[str, Any]:
    resolved_catalog = dict(catalog or get_tool_catalog())
    rows: list[dict[str, Any]] = []
    unknown_mutating_contracts: list[dict[str, Any]] = []

    for definition in sorted(resolved_catalog.values(), key=lambda item: item.name):
        for contract in tool_authority_contracts(definition):
            rows.append(contract)
            if (
                str(contract.get("risk") or "") in MUTATING_RISKS
                and contract.get("authority_domain_ref") == "authority.unclassified"
            ):
                unknown_mutating_contracts.append(contract)

    mutating_count = sum(1 for row in rows if str(row.get("risk") or "") in MUTATING_RISKS)
    return {
        "contract_version": 1,
        "tool_count": len(resolved_catalog),
        "authority_contract_count": len(rows),
        "mutating_contract_count": mutating_count,
        "contracts": rows,
        "drift": {
            "unknown_mutating_contracts": unknown_mutating_contracts,
        },
    }


def assert_mcp_tool_catalog_classified(
    catalog: Mapping[str, McpToolDefinition] | None = None,
) -> None:
    payload = classify_mcp_tool_catalog(catalog)
    drift = payload["drift"]
    if drift["unknown_mutating_contracts"]:
        raise FrontdoorAuthorityError(
            "mutating MCP tools must declare an authority boundary",
            drift=drift,
        )


def assert_mcp_tool_authority_contract(
    definition: McpToolDefinition,
    params: Mapping[str, Any] | None,
) -> McpToolAuthorityContract:
    contract = tool_authority_contract_for_params(definition, params)
    if (
        contract.risk in MUTATING_RISKS
        and contract.authority_domain_ref == "authority.unclassified"
    ):
        raise FrontdoorAuthorityError(
            "mutating MCP tool call must declare an authority boundary",
            drift={"unknown_mutating_contracts": [contract.to_payload()]},
        )
    return contract


def build_mcp_tool_authority_payload(
    catalog: Mapping[str, McpToolDefinition] | None = None,
) -> dict[str, Any]:
    payload = classify_mcp_tool_catalog(catalog)
    drift = payload["drift"]
    payload["ok"] = not drift["unknown_mutating_contracts"]
    payload["routed_to"] = "mcp_frontdoor_authority"
    return payload


__all__ = [
    "MUTATING_RISKS",
    "FrontdoorAuthorityError",
    "McpToolAuthorityContract",
    "assert_mcp_tool_authority_contract",
    "assert_mcp_tool_catalog_classified",
    "build_mcp_tool_authority_payload",
    "classify_mcp_tool_catalog",
    "tool_authority_contract_for_params",
    "tool_authority_contracts",
]
