"""Machine-readable primitive contracts projected by /orient.

These helpers do not own runtime truth. They name the current authority for
each primitive so cold-start agents can consume one packet without inventing
sidecar context.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

from contracts.operation_catalog import OPERATION_IDEMPOTENCY_POLICIES, OPERATION_POSTURES

_POLICY_DECISION_REF = (
    "operator_decision.architecture_policy.primitive_contracts."
    "orient_projects_operation_runtime_state_contracts"
)
_POLICY_DECISION_KEY = (
    "architecture-policy::primitive-contracts::"
    "orient-projects-operation-runtime-state-contracts"
)

_API_BASE_URL_ENV = "PRAXIS_API_BASE_URL"
_WORKFLOW_API_BASE_URL_ENV = "PRAXIS_WORKFLOW_API_BASE_URL"
_API_PORT_ENV = "PRAXIS_API_PORT"
_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
_DATABASE_AUTHORITY_SOURCE_ENV = "WORKFLOW_DATABASE_AUTHORITY_SOURCE"
_DEFAULT_API_PORT = "8420"

# Tolerated legacy status aliases produced by older tooling. Consumers must
# accept them on read paths but never emit them as canonical values.
_BUG_STATUS_LEGACY_RESOLVED_ALIASES: tuple[str, ...] = ("RESOLVED", "DONE", "CLOSED")

_BUG_STATUS_SEMANTICS: dict[str, dict[str, bool]] = {
    "OPEN": {
        "is_open": True,
        "is_active": True,
        "is_resolved": False,
        "is_terminal": False,
    },
    "IN_PROGRESS": {
        "is_open": True,
        "is_active": True,
        "is_resolved": False,
        "is_terminal": False,
    },
    "FIXED": {
        "is_open": False,
        "is_active": False,
        "is_resolved": True,
        "is_terminal": True,
    },
    "WONT_FIX": {
        "is_open": False,
        "is_active": False,
        "is_resolved": True,
        "is_terminal": True,
    },
    "DEFERRED": {
        "is_open": False,
        "is_active": False,
        "is_resolved": True,
        "is_terminal": True,
    },
}


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _join_url(base_url: str, suffix: str) -> str:
    return f"{base_url.rstrip('/')}/{suffix.lstrip('/')}"


def redact_url(value: object) -> str | None:
    """Return a non-secret URL representation suitable for orient packets."""

    raw = _clean_text(value)
    if not raw:
        return None
    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return "<configured>"

    hostname = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port is not None else ""
    if parsed.username:
        netloc = f"{parsed.username}:***@{hostname}{port}"
    else:
        netloc = f"{hostname}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


def bug_open_status_values() -> tuple[str, ...]:
    """Canonical status values that count as open bug work."""

    return tuple(
        status
        for status, predicates in _BUG_STATUS_SEMANTICS.items()
        if predicates["is_open"]
    )


def bug_resolved_status_values() -> tuple[str, ...]:
    """Canonical status values that resolve bug work."""

    return tuple(
        status
        for status, predicates in _BUG_STATUS_SEMANTICS.items()
        if predicates["is_resolved"]
    )


def bug_resolved_status_values_with_legacy() -> tuple[str, ...]:
    """Resolved status values plus tolerated legacy aliases for read paths."""

    return bug_resolved_status_values() + _BUG_STATUS_LEGACY_RESOLVED_ALIASES


def bug_status_legacy_resolved_aliases() -> tuple[str, ...]:
    """Tolerated legacy resolved-status aliases (uppercase)."""

    return _BUG_STATUS_LEGACY_RESOLVED_ALIASES


def bug_status_sql_in_literal(
    kind: Literal["open", "resolved", "resolved_with_legacy"],
    *,
    column: str = "status",
) -> str:
    """Return a ``UPPER(<column>) IN (...)`` SQL fragment from the state contract.

    Callers should embed the returned fragment directly; no query parameters
    are needed because every value is a fixed, contract-owned identifier.
    """

    if kind == "open":
        values = bug_open_status_values()
    elif kind == "resolved":
        values = bug_resolved_status_values()
    elif kind == "resolved_with_legacy":
        values = bug_resolved_status_values_with_legacy()
    else:  # pragma: no cover — exhaustive Literal guard
        raise ValueError(f"unknown bug status predicate kind: {kind}")
    column_text = str(column or "").strip() or "status"
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"UPPER({column_text}) IN ({rendered})"


def bug_status_sql_equals_literal(status: str, *, column: str = "status") -> str:
    """Return a ``UPPER(<column>) = '<STATUS>'`` SQL fragment.

    Raises ``ValueError`` if *status* is not a canonical status in the
    state-semantics contract. This prevents consumers from drifting onto
    non-authority status values.
    """

    status_text = str(status or "").strip().upper()
    if status_text not in _BUG_STATUS_SEMANTICS:
        raise ValueError(
            f"{status_text!r} is not a canonical bug status; "
            f"known: {sorted(_BUG_STATUS_SEMANTICS)}"
        )
    column_text = str(column or "").strip() or "status"
    return f"UPPER({column_text}) = '{status_text}'"


def bug_query_default_open_only_list() -> bool:
    """Default ``open_only`` for generic bug list queries.

    Machine-facing surfaces (API ``GET /bugs``, API ``POST /bugs``/``search``,
    MCP ``praxis_bugs`` generic tool) expose the full bug set by default.
    Callers narrow with ``open_only=True`` or ``status=<canonical>`` when they
    need the open-work view.
    """

    return False


def bug_query_default_open_only_backlog() -> bool:
    """Default ``open_only`` for operator-backlog bug queries.

    Operator-facing surfaces (CLI ``praxis workflow bugs list``,
    ``praxis_issue_backlog``, ``praxis_bug_replay_provenance_backfill``,
    ``praxis_replay_ready_bugs``) show the actionable open-work slice by
    default. An explicit ``--all`` / ``open_only=False`` flag widens the
    view to include resolved history.
    """

    return True


def failure_identity_fields() -> tuple[str, ...]:
    """Return the canonical failure-identity field order from the contract."""

    contract = build_failure_identity_contract()
    return tuple(contract["identity_fields"])


def build_operation_posture_contract() -> dict[str, Any]:
    """Project the operation-posture primitive from current catalog authority."""

    return {
        "kind": "operation_posture_contract",
        "authority": "runtime.posture.PostureEnforcer + runtime.operation_catalog",
        "policy_decision_ref": _POLICY_DECISION_REF,
        "policy_decision_key": _POLICY_DECISION_KEY,
        "catalog_postures": sorted(OPERATION_POSTURES),
        "idempotency_policies": sorted(OPERATION_IDEMPOTENCY_POLICIES),
        "classifications": ["read", "mutate", "telemetry"],
        "posture_rules": {
            "observe": {
                "allows": ["read", "telemetry"],
                "forbids": ["mutate"],
                "mutation_contract": "no state writes; read paths must not backfill or repair",
            },
            "operate": {
                "allows": ["read", "mutate", "telemetry"],
                "requires": ["operation_receipt", "catalog_binding"],
                "mutation_contract": "writes must route through catalog-backed authority",
            },
            "build": {
                "allows": ["read", "mutate", "telemetry"],
                "requires": ["declared_scope", "validation_receipt"],
                "mutation_contract": "repo writes must stay inside execution scope",
            },
        },
        "semantic_operations": {
            "propose": {
                "catalog_posture": "observe",
                "idempotency_policy": "read_only",
                "contract": "preview or plan only; must not persist state",
            },
            "mutate": {
                "catalog_posture": "operate",
                "contract": "state change with operation receipt",
            },
            "repair": {
                "catalog_posture": "operate",
                "requires": ["proof_ref", "before_state_ref", "after_state_ref"],
                "contract": "explicit repair path; never hidden read-path backfill",
            },
        },
        "fail_closed": True,
    }


def build_runtime_binding_contract(
    *,
    workflow_env: Mapping[str, str] | None,
    native_instance: Mapping[str, Any] | None,
    workflow_env_error: str | None = None,
) -> dict[str, Any]:
    """Project DB and local HTTP endpoint binding without exposing secrets."""

    env = dict(workflow_env or {})
    api_base = _clean_text(env.get(_API_BASE_URL_ENV))
    api_source = f"env:{_API_BASE_URL_ENV}"
    if not api_base:
        port = _clean_text(env.get(_API_PORT_ENV)) or _DEFAULT_API_PORT
        api_base = f"http://127.0.0.1:{port}"
        api_source = f"env:{_API_PORT_ENV}" if _clean_text(env.get(_API_PORT_ENV)) else "default:api_port"

    workflow_api_base = _clean_text(env.get(_WORKFLOW_API_BASE_URL_ENV)) or api_base
    workflow_api_source = (
        f"env:{_WORKFLOW_API_BASE_URL_ENV}"
        if _clean_text(env.get(_WORKFLOW_API_BASE_URL_ENV))
        else api_source
    )

    database_url = _clean_text(env.get(_DATABASE_URL_ENV))
    database_source = _clean_text(env.get(_DATABASE_AUTHORITY_SOURCE_ENV)) or "unknown"
    native_contract = dict(native_instance or {})

    binding = {
        "kind": "runtime_binding_contract",
        "authority": "runtime._workflow_database + runtime.instance + surfaces.api.server",
        "policy_decision_ref": _POLICY_DECISION_REF,
        "policy_decision_key": _POLICY_DECISION_KEY,
        "database": {
            "env_ref": _DATABASE_URL_ENV,
            "configured": bool(database_url),
            "authority_source": database_source,
            "redacted_url": redact_url(database_url),
            "secret_policy": "never emit raw DSN in orient; consumers resolve by env_ref",
        },
        "http_endpoints": {
            "api_base_url": api_base.rstrip("/"),
            "workflow_api_base_url": workflow_api_base.rstrip("/"),
            "launch_url": _join_url(api_base, "app"),
            "dashboard_url": _join_url(api_base, "app"),
            "api_docs_url": _join_url(api_base, "docs"),
            "authority_source": api_source,
            "workflow_api_authority_source": workflow_api_source,
        },
        "native_instance_ref": "/orient#authority_envelope.native_instance",
        "workspace": {
            "repo_root": native_contract.get("repo_root"),
            "workdir": native_contract.get("workdir"),
            "runtime_profile": native_contract.get("praxis_runtime_profile"),
        },
    }
    if workflow_env_error:
        binding["database"]["error"] = workflow_env_error
    return binding


def resolve_runtime_http_endpoints(
    *,
    workflow_env: Mapping[str, str] | None = None,
    native_instance: Mapping[str, Any] | None = None,
    workflow_env_error: str | None = None,
) -> dict[str, str]:
    """Return launcher-facing HTTP endpoints from runtime binding authority."""

    binding = build_runtime_binding_contract(
        workflow_env=workflow_env,
        native_instance=native_instance,
        workflow_env_error=workflow_env_error,
    )
    endpoints = binding["http_endpoints"]
    assert isinstance(endpoints, Mapping)
    return {
        "api_base_url": _clean_text(endpoints.get("api_base_url")),
        "workflow_api_base_url": _clean_text(endpoints.get("workflow_api_base_url")),
        "launch_url": _clean_text(endpoints.get("launch_url")),
        "dashboard_url": _clean_text(endpoints.get("dashboard_url")),
        "api_docs_url": _clean_text(endpoints.get("api_docs_url")),
        "authority_source": _clean_text(endpoints.get("authority_source")),
        "workflow_api_authority_source": _clean_text(
            endpoints.get("workflow_api_authority_source")
        ),
    }


def build_state_semantics_contract() -> dict[str, Any]:
    """Project canonical status predicates used by operator read paths."""

    return {
        "kind": "state_semantics_contract",
        "authority": "runtime.primitive_contracts",
        "policy_decision_ref": _POLICY_DECISION_REF,
        "policy_decision_key": _POLICY_DECISION_KEY,
        "bug": {
            "status_predicates": {
                status: dict(predicates)
                for status, predicates in sorted(_BUG_STATUS_SEMANTICS.items())
            },
            "open_statuses": list(bug_open_status_values()),
            "resolved_statuses": list(bug_resolved_status_values()),
            "legacy_resolved_aliases": list(_BUG_STATUS_LEGACY_RESOLVED_ALIASES),
            "resolved_statuses_with_legacy": list(
                bug_resolved_status_values_with_legacy()
            ),
            "sql_predicate_helper": (
                "runtime.primitive_contracts.bug_status_sql_in_literal"
            ),
            "normalization": "strip, uppercase, replace '-' with '_'",
            "query_defaults": {
                "list": {
                    "open_only": bug_query_default_open_only_list(),
                    "consumer": "machine-facing: API /bugs, MCP praxis_bugs",
                    "helper": (
                        "runtime.primitive_contracts."
                        "bug_query_default_open_only_list"
                    ),
                },
                "backlog": {
                    "open_only": bug_query_default_open_only_backlog(),
                    "consumer": (
                        "operator-facing: CLI workflow bugs list, "
                        "praxis_issue_backlog, praxis_bug_replay_provenance_backfill, "
                        "praxis_replay_ready_bugs"
                    ),
                    "helper": (
                        "runtime.primitive_contracts."
                        "bug_query_default_open_only_backlog"
                    ),
                },
            },
        },
    }


def build_proof_ref_contract() -> dict[str, Any]:
    """Project the proof-ref primitive shape used by repair and closeout paths."""

    from runtime.bug_evidence import ALLOWED_EVIDENCE_KINDS, ALLOWED_EVIDENCE_ROLES

    return {
        "kind": "proof_ref_contract",
        "authority": "receipts + bug evidence + operator decisions",
        "policy_decision_ref": _POLICY_DECISION_REF,
        "policy_decision_key": _POLICY_DECISION_KEY,
        "allowed_ref_kinds": [
            "receipt",
            "run",
            "decision",
            "artifact",
            "bug",
            "roadmap_item",
            "verification_run",
            "healing_run",
        ],
        "allowed_evidence_kinds": sorted(ALLOWED_EVIDENCE_KINDS),
        "allowed_evidence_roles": sorted(ALLOWED_EVIDENCE_ROLES),
        "required_fields": ["ref_kind", "ref_id", "evidence_role"],
        "repair_extensions": ["before_state_ref", "after_state_ref", "reason_code"],
        "replay_ref": {
            "required_when_available": ["run_id", "receipt_id"],
            "blocked_reason_field": "replay_reason_code",
        },
    }


def build_failure_identity_contract() -> dict[str, Any]:
    """Project canonical failure identity over the existing bug evidence helper."""

    return {
        "kind": "failure_identity_contract",
        "authority": "runtime.bug_evidence.build_failure_signature",
        "policy_decision_ref": _POLICY_DECISION_REF,
        "policy_decision_key": _POLICY_DECISION_KEY,
        "identity_fields": [
            "failure_code",
            "job_label",
            "node_id",
            "failure_category",
            "agent",
            "provider_slug",
            "model_slug",
        ],
        "fingerprint_field": "fingerprint",
        "single_bug_authority_rule": (
            "Auto-filing consumers must share this identity or delegate filing to one authority."
        ),
    }


def build_orient_primitive_contracts(
    *,
    workflow_env: Mapping[str, str] | None,
    native_instance: Mapping[str, Any] | None,
    workflow_env_error: str | None = None,
) -> dict[str, Any]:
    """Return the complete primitive contract projection for /orient."""

    return {
        "kind": "orient_primitive_contracts",
        "policy_decision_ref": _POLICY_DECISION_REF,
        "policy_decision_key": _POLICY_DECISION_KEY,
        "operation_posture": build_operation_posture_contract(),
        "runtime_binding": build_runtime_binding_contract(
            workflow_env=workflow_env,
            native_instance=native_instance,
            workflow_env_error=workflow_env_error,
        ),
        "state_semantics": build_state_semantics_contract(),
        "proof_ref": build_proof_ref_contract(),
        "failure_identity": build_failure_identity_contract(),
    }


__all__ = [
    "bug_open_status_values",
    "bug_query_default_open_only_backlog",
    "bug_query_default_open_only_list",
    "bug_resolved_status_values",
    "bug_resolved_status_values_with_legacy",
    "bug_status_legacy_resolved_aliases",
    "bug_status_sql_equals_literal",
    "bug_status_sql_in_literal",
    "build_failure_identity_contract",
    "build_operation_posture_contract",
    "build_orient_primitive_contracts",
    "build_proof_ref_contract",
    "build_runtime_binding_contract",
    "build_state_semantics_contract",
    "failure_identity_fields",
    "resolve_runtime_http_endpoints",
    "redact_url",
]
