"""Authority contracts for Praxis CLI front doors.

CLI commands are renderers and adapters. Durable state belongs to the same
CQRS/domain authorities used by API and MCP, so every exposed command gets an
explicit boundary classification before dispatch.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


class CliFrontdoorAuthorityError(RuntimeError):
    """Raised when a CLI command lacks an authority declaration."""

    def __init__(self, message: str, *, command_ref: str) -> None:
        super().__init__(message)
        self.command_ref = command_ref


@dataclass(frozen=True, slots=True)
class CliFrontdoorAuthority:
    command_ref: str
    risk: str
    boundary_kind: str
    authority_domain_ref: str
    cqrs_entrypoint: str
    receipt_policy: str
    event_policy: str
    workspace_authority_ref: str = "registry_workspace_base_path_authority"
    legacy_status: str = "active"
    migration_target: str | None = None
    decision_ref: str = "decision.cqrs_authority_unification.20260422"

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _row(
    command_ref: str,
    *,
    risk: str,
    boundary_kind: str,
    authority_domain_ref: str,
    cqrs_entrypoint: str,
    receipt_policy: str = "surface_usage_event",
    event_policy: str = "not_required",
    legacy_status: str = "active",
    migration_target: str | None = None,
) -> CliFrontdoorAuthority:
    return CliFrontdoorAuthority(
        command_ref=command_ref,
        risk=risk,
        boundary_kind=boundary_kind,
        authority_domain_ref=authority_domain_ref,
        cqrs_entrypoint=cqrs_entrypoint,
        receipt_policy=receipt_policy,
        event_policy=event_policy,
        legacy_status=legacy_status,
        migration_target=migration_target,
    )


def _read(command: str, domain: str = "authority.read_models") -> CliFrontdoorAuthority:
    return _row(
        f"workflow {command}",
        risk="read",
        boundary_kind="read_projection",
        authority_domain_ref=domain,
        cqrs_entrypoint="projection_or_query_handler",
    )


def _write(
    command: str,
    domain: str,
    *,
    cqrs_entrypoint: str = "operation_catalog_or_domain_authority",
) -> CliFrontdoorAuthority:
    return _row(
        f"workflow {command}",
        risk="write",
        boundary_kind="domain_authority",
        authority_domain_ref=domain,
        cqrs_entrypoint=cqrs_entrypoint,
        receipt_policy="authority_operation_receipt",
        event_policy="authority_event",
        legacy_status="legacy_visible",
        migration_target="operation_catalog_gateway",
    )


def _launch(command: str, domain: str = "authority.workflow_runs") -> CliFrontdoorAuthority:
    return _row(
        f"workflow {command}",
        risk="launch",
        boundary_kind="control_command_bus",
        authority_domain_ref=domain,
        cqrs_entrypoint="workflow_control_command_bus",
        receipt_policy="control_command_receipt",
        event_policy="workflow_event",
    )


def _delegated(command: str) -> CliFrontdoorAuthority:
    return _row(
        f"workflow {command}",
        risk="delegated",
        boundary_kind="mcp_tool_catalog",
        authority_domain_ref="authority.mcp_tool_catalog",
        cqrs_entrypoint="mcp_frontdoor_authority",
        receipt_policy="tool_contract_specific",
        event_policy="tool_contract_specific",
    )


_WORKFLOW_CLI_AUTHORITY_ROWS: tuple[CliFrontdoorAuthority, ...] = (
    *(_read(command) for command in (
        "active",
        "api",
        "architecture",
        "artifacts",
        "capabilities",
        "chain-status",
        "commands",
        "config",
        "costs",
        "dashboard",
        "decompose",
        "diagnose",
        "dry-run",
        "events",
        "fitness",
        "graph-lineage",
        "graph-topology",
        "handoff",
        "health",
        "health-map",
        "inspect",
        "inspect-job",
        "leaderboard",
        "lineage",
        "metrics",
        "notifications",
        "params",
        "preview",
        "query",
        "recall",
        "receipts",
        "replay",
        "reviews",
        "risk",
        "routes",
        "run-status",
        "runs",
        "scheduler",
        "scope",
        "slots",
        "status",
        "stream",
        "supervisor",
        "topology",
        "trust",
        "validate",
        "workflows",
    )),
    _read("authority-index", "authority.authority_objects"),
    _read("cache", "authority.cache"),
    _read("discover", "authority.compile_index"),
    _read("integrations", "authority.integrations"),
    _read("trends", "authority.observability"),
    *(_write(command, domain) for command, domain in (
        ("authority-memory", "authority.semantic_memory"),
        ("bugs", "authority.bugs"),
        ("catalog", "authority.catalog"),
        ("circuits", "authority.operations"),
        ("compile", "authority.compile_index"),
        ("data", "authority.data_pipeline"),
        ("dictionary", "authority.data_dictionary"),
        ("files", "authority.files"),
        ("generate", "authority.workflow_definitions"),
        ("github", "authority.github"),
        ("heartbeat", "authority.heartbeat"),
        ("integration", "authority.integrations"),
        ("maintenance", "authority.maintenance"),
        ("manifest", "authority.manifest"),
        ("object", "authority.object_schema"),
        ("object-field", "authority.object_schema"),
        ("object-type", "authority.object_schema"),
        ("records", "authority.workflow_definitions"),
        ("registry", "authority.registry"),
        ("reload", "authority.runtime_reload"),
        ("reconcile", "authority.reconcile"),
        ("roadmap", "authority.operator_control"),
        ("schema", "authority.schema"),
        ("triggers", "authority.workflow_triggers"),
        ("work", "authority.workflow_worker"),
    )),
    *(_launch(command) for command in (
        "cancel",
        "chain",
        "debate",
        "heal",
        "loop",
        "pipeline",
        "proof",
        "queue",
        "repair",
        "research",
        "retry",
        "run",
        "spawn",
        "verify",
        "verify-platform",
    )),
    _delegated("mcp"),
    _delegated("tools"),
    _row(
        "workflow native-operator",
        risk="write",
        boundary_kind="operator_control_frontdoor",
        authority_domain_ref="authority.operator_control",
        cqrs_entrypoint="operator_control_frontdoor_or_operation_catalog",
        receipt_policy="authority_operation_receipt",
        event_policy="authority_event",
        legacy_status="legacy_visible",
        migration_target="operation_catalog_gateway",
    ),
)


def _root(
    namespace: str,
    *,
    risk: str,
    boundary_kind: str,
    authority_domain_ref: str,
    cqrs_entrypoint: str,
    receipt_policy: str = "authority_operation_receipt",
    event_policy: str = "authority_event",
    legacy_status: str = "active",
    migration_target: str | None = None,
) -> CliFrontdoorAuthority:
    return _row(
        f"praxis {namespace}",
        risk=risk,
        boundary_kind=boundary_kind,
        authority_domain_ref=authority_domain_ref,
        cqrs_entrypoint=cqrs_entrypoint,
        receipt_policy=receipt_policy,
        event_policy=event_policy,
        legacy_status=legacy_status,
        migration_target=migration_target,
    )


_ROOT_CLI_AUTHORITY_ROWS: tuple[CliFrontdoorAuthority, ...] = (
    _root(
        "workflow",
        risk="delegated",
        boundary_kind="workflow_cli_frontdoor",
        authority_domain_ref="authority.workflow_frontdoor",
        cqrs_entrypoint="workflow_cli_authority",
        receipt_policy="command_specific",
        event_policy="command_specific",
    ),
    _root(
        "launcher",
        risk="write",
        boundary_kind="launcher_authority",
        authority_domain_ref="authority.launcher",
        cqrs_entrypoint="launcher_authority",
        legacy_status="active",
        migration_target=None,
    ),
    *(
        _root(
            namespace,
            risk="write",
            boundary_kind="domain_authority",
            authority_domain_ref=domain,
            cqrs_entrypoint="operation_catalog_or_domain_authority",
            legacy_status="legacy_visible",
            migration_target="operation_catalog_gateway",
        )
        for namespace, domain in (
            ("catalog", "authority.catalog"),
            ("data", "authority.data_pipeline"),
            ("dataset", "authority.dataset"),
            ("db", "authority.schema"),
            ("hierarchy", "authority.object_schema"),
            ("object", "authority.object_schema"),
            ("object-type", "authority.object_schema"),
            ("objects", "authority.object_schema"),
            ("page", "authority.manifest"),
            ("reconcile", "authority.reconcile"),
            ("registry", "authority.registry"),
            ("reload", "authority.runtime_reload"),
        )
    ),
)


CLI_FRONTDOOR_AUTHORITY: dict[str, CliFrontdoorAuthority] = {
    row.command_ref: row
    for row in (*_WORKFLOW_CLI_AUTHORITY_ROWS, *_ROOT_CLI_AUTHORITY_ROWS)
}


def workflow_command_ref(command_name: str) -> str:
    return f"workflow {str(command_name or '').strip()}"


def root_namespace_ref(namespace: str) -> str:
    return f"praxis {str(namespace or '').strip()}"


def classify_cli_frontdoors(command_refs: Iterable[str] | None = None) -> dict[str, Any]:
    refs = sorted(set(command_refs or CLI_FRONTDOOR_AUTHORITY))
    rows: list[dict[str, Any]] = []
    unknown_refs: list[str] = []
    for ref in refs:
        contract = CLI_FRONTDOOR_AUTHORITY.get(ref)
        if contract is None:
            unknown_refs.append(ref)
        else:
            rows.append(contract.to_payload())
    return {
        "contract_version": 1,
        "classified_count": len(rows),
        "contracts": rows,
        "drift": {"unknown_command_refs": unknown_refs},
    }


def command_authority(command_ref: str) -> CliFrontdoorAuthority:
    normalized = " ".join(str(command_ref or "").split())
    contract = CLI_FRONTDOOR_AUTHORITY.get(normalized)
    if contract is None:
        raise CliFrontdoorAuthorityError(
            f"CLI command lacks authority classification: {normalized}",
            command_ref=normalized,
        )
    return contract


def assert_workflow_command_classified(command_name: str) -> CliFrontdoorAuthority:
    return command_authority(workflow_command_ref(command_name))


def assert_root_namespace_classified(namespace: str) -> CliFrontdoorAuthority:
    return command_authority(root_namespace_ref(namespace))


def build_cli_frontdoor_authority_payload(
    command_refs: Iterable[str] | None = None,
) -> dict[str, Any]:
    payload = classify_cli_frontdoors(command_refs)
    payload["ok"] = not payload["drift"]["unknown_command_refs"]
    payload["routed_to"] = "cli_frontdoor_authority"
    return payload


__all__ = [
    "CLI_FRONTDOOR_AUTHORITY",
    "CliFrontdoorAuthority",
    "CliFrontdoorAuthorityError",
    "assert_root_namespace_classified",
    "assert_workflow_command_classified",
    "build_cli_frontdoor_authority_payload",
    "classify_cli_frontdoors",
    "command_authority",
    "root_namespace_ref",
    "workflow_command_ref",
]
