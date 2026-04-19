"""Anti-bypass tests for primitive contract consumer boundaries.

These tests enforce the policy row
``architecture-policy::primitive-contracts::orient-projects-operation-runtime-state-contracts``:
consumers of the orient primitive contracts must not hand-roll raw literals
for DSN strings, HTTP endpoints, bug status predicates, or failure identity
fields. Instead they must route through
``runtime.primitive_contracts`` or the named underlying authorities.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from runtime import bug_evidence
from runtime.primitive_contracts import (
    _BUG_STATUS_SEMANTICS,
    bug_open_status_values,
    bug_resolved_status_values,
    bug_resolved_status_values_with_legacy,
    bug_status_legacy_resolved_aliases,
    bug_status_sql_equals_literal,
    bug_status_sql_in_literal,
    build_failure_identity_contract,
    build_runtime_binding_contract,
    build_state_semantics_contract,
    failure_identity_fields,
    resolve_runtime_http_endpoints,
)

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


def _read(path_from_workflow: str) -> str:
    return (_WORKFLOW_ROOT / path_from_workflow).read_text()


# ---------------------------------------------------------------------------
# Status literal bypass: targeted consumer surfaces must route through helpers.
# ---------------------------------------------------------------------------


_STATUS_CONSUMER_SURFACES = (
    "surfaces/api/handlers/workflow_admin.py",
    "runtime/bug_tracker.py",
    "runtime/dataset_staleness.py",
)


@pytest.mark.parametrize("relative_path", _STATUS_CONSUMER_SURFACES)
def test_targeted_consumers_do_not_hand_roll_open_status_sql(relative_path: str) -> None:
    source = _read(relative_path)
    # The exact legacy literal must not appear in targeted consumer surfaces —
    # they must consume the state-semantics contract helper instead.
    assert "UPPER(status) IN ('OPEN', 'IN_PROGRESS')" not in source, (
        f"{relative_path} bypasses primitive_contracts.bug_status_sql_in_literal"
    )
    assert "UPPER(b.status) IN ('OPEN', 'IN_PROGRESS')" not in source, (
        f"{relative_path} bypasses primitive_contracts.bug_status_sql_in_literal"
    )


def test_bug_tracker_resolved_legacy_aliases_come_from_contract() -> None:
    source = _read("runtime/bug_tracker.py")
    assert "bug_status_legacy_resolved_aliases" in source
    # The legacy resolved alias tuple must not be inlined as a quoted list.
    assert "'RESOLVED'" not in source
    assert "'DONE'" not in source
    assert "'CLOSED'" not in source
    # The long 'FIXED','RESOLVED','DONE','CLOSED' form must be gone from
    # historical_fix queries — they must ask the contract for the combined list.
    assert "'FIXED', 'RESOLVED', 'DONE', 'CLOSED'" not in source


def test_dataset_staleness_wontfix_uses_contract_helper() -> None:
    source = _read("runtime/dataset_staleness.py")
    assert "bug_status_sql_equals_literal" in source
    # Raw literal predicate must not be present — only the helper call.
    assert "UPPER(status) = 'WONT_FIX'" not in source


# ---------------------------------------------------------------------------
# SQL helper semantics: helpers must emit values drawn from the contract.
# ---------------------------------------------------------------------------


def test_bug_status_sql_in_literal_matches_contract_values() -> None:
    open_rendered = bug_status_sql_in_literal("open")
    resolved_rendered = bug_status_sql_in_literal("resolved")
    legacy_rendered = bug_status_sql_in_literal("resolved_with_legacy")

    for status in bug_open_status_values():
        assert f"'{status}'" in open_rendered
    for status in bug_resolved_status_values():
        assert f"'{status}'" in resolved_rendered
    for status in bug_resolved_status_values_with_legacy():
        assert f"'{status}'" in legacy_rendered

    assert open_rendered.startswith("UPPER(status) IN (")
    assert bug_status_sql_in_literal("open", column="b.status").startswith(
        "UPPER(b.status) IN ("
    )


def test_bug_status_sql_equals_literal_rejects_unknown_status() -> None:
    with pytest.raises(ValueError):
        bug_status_sql_equals_literal("MADE_UP")


def test_bug_status_sql_equals_literal_accepts_canonical_status() -> None:
    for status in _BUG_STATUS_SEMANTICS:
        assert bug_status_sql_equals_literal(status).endswith(f"= '{status}'")


def test_bug_resolved_aliases_are_public_through_contract() -> None:
    aliases = bug_status_legacy_resolved_aliases()
    assert aliases == ("RESOLVED", "DONE", "CLOSED")


# ---------------------------------------------------------------------------
# Runtime binding: targeted consumers do not embed raw DSN env reads.
# ---------------------------------------------------------------------------


def test_runtime_binding_contract_redacts_dsn() -> None:
    binding = build_runtime_binding_contract(
        workflow_env={
            "WORKFLOW_DATABASE_URL": "postgresql://user:password@host:5432/db",
            "PRAXIS_API_PORT": "9001",
        },
        native_instance={"repo_root": "/tmp/repo"},
    )
    database = binding["database"]
    assert database["redacted_url"] is not None
    # The raw password must never appear in the projected contract.
    assert "password" not in database["redacted_url"]
    # The configured flag and env_ref must be named authorities, not constants.
    assert database["env_ref"] == "WORKFLOW_DATABASE_URL"
    assert binding["http_endpoints"]["api_base_url"] == "http://127.0.0.1:9001"


def test_resolve_runtime_http_endpoints_projects_from_contract() -> None:
    endpoints = resolve_runtime_http_endpoints(
        workflow_env={"PRAXIS_API_BASE_URL": "http://api.test:8001"},
        native_instance={},
    )
    assert endpoints["api_base_url"] == "http://api.test:8001"
    assert endpoints["launch_url"] == "http://api.test:8001/app"
    assert endpoints["api_docs_url"] == "http://api.test:8001/docs"


# ---------------------------------------------------------------------------
# Failure identity: bug_evidence consumes contract fields, not a local list.
# ---------------------------------------------------------------------------


def test_bug_evidence_signature_anchors_come_from_contract() -> None:
    assert bug_evidence._SIGNATURE_ANCHOR_FIELDS == failure_identity_fields()
    contract = build_failure_identity_contract()
    assert tuple(contract["identity_fields"]) == failure_identity_fields()


def test_bug_evidence_source_does_not_inline_identity_tuple() -> None:
    source = _read("runtime/bug_evidence.py")
    # Hand-rolled anchor-field tuples must be gone from bug_evidence.
    assert 'failure_code",\n    "job_label",\n    "node_id"' not in source
    assert "failure_identity_fields" in source


# ---------------------------------------------------------------------------
# State semantics projection carries legacy alias authority.
# ---------------------------------------------------------------------------


def test_state_semantics_contract_projects_legacy_aliases() -> None:
    contract = build_state_semantics_contract()
    bug_block = contract["bug"]
    assert bug_block["legacy_resolved_aliases"] == list(
        bug_status_legacy_resolved_aliases()
    )
    assert bug_block["resolved_statuses_with_legacy"] == list(
        bug_resolved_status_values_with_legacy()
    )
    assert (
        bug_block["sql_predicate_helper"]
        == "runtime.primitive_contracts.bug_status_sql_in_literal"
    )
