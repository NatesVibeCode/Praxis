from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime.task_contracts import (
    AllowedTool,
    ContractEvaluationContext,
    ContractPolicyBounds,
    HierarchyNode,
    ModelPolicy,
    RevisionRecord,
    ScopeGrant,
    SopGap,
    SopReference,
    StalenessPolicy,
    StalenessSignal,
    TaskEnvironmentContract,
    VerifierReference,
    sha256_json,
    validate_append_only_revision_chain,
    validate_next_revision,
    validate_task_environment_contract,
)


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)


def _node(
    node_id: str,
    node_type: str,
    parent_node_id: str | None,
    *,
    status: str = "active",
    owner_ref: str = "owner.ops",
    steward_ref: str = "steward.ops",
) -> HierarchyNode:
    return HierarchyNode(
        node_id=node_id,
        node_type=node_type,
        node_name=node_id.rsplit(".", 1)[-1],
        parent_node_id=parent_node_id,
        status=status,
        effective_from=NOW - timedelta(days=30),
        effective_to=None,
        owner_ref=owner_ref,
        steward_ref=steward_ref,
        default_sop_refs=("sop.account_sync",),
        default_tool_policy_ref="tool_policy.account_sync",
        default_scope_policy_ref="scope_policy.account_sync",
        default_model_policy_ref="model_policy.object_truth_contracting",
        default_verifier_refs=("verifier.contract.behavior",),
        revision_id=f"rev.{node_id}.1",
    )


def _hierarchy() -> tuple[HierarchyNode, ...]:
    return (
        _node("enterprise.acme", "enterprise", None),
        _node("program.gtm", "program", "enterprise.acme"),
        _node("workflow.account_sync", "workflow", "program.gtm"),
        _node("task.account_sync", "task", "workflow.account_sync"),
    )


def _sop(*, status: str = "active") -> SopReference:
    return SopReference(
        sop_ref="sop.account_sync",
        sop_title="Account Sync SOP",
        sop_version="1.0",
        sop_status=status,
        sop_owner_ref="owner.ops",
        effective_from=NOW - timedelta(days=10),
        source_uri="repo://docs/account-sync.md",
        primary=True,
    )


def _tool(*, tool_ref: str = "tool.repo.read", tool_class: str = "read_only_repo") -> AllowedTool:
    return AllowedTool(
        tool_ref=tool_ref,
        tool_name=tool_ref,
        tool_class=tool_class,
        capabilities=("read",),
        data_domains=("account",),
        approval_level="standard",
        logging_requirements=("receipt",),
        allowed_operations=("read",),
        prohibited_operations=(),
        approval_ref="approval.deploy.1" if tool_class in {"deployment", "admin_override"} else None,
    )


def _read_scope(locator: str = "/clients/acme/accounts") -> ScopeGrant:
    return ScopeGrant(
        scope_ref="scope.read.accounts",
        resource_type="repo_path",
        resource_locator=locator,
        access_mode="read",
        environment="dev",
        classification="confidential",
        tenant_boundary="tenant.acme",
    )


def _write_scope(locator: str = "/clients/acme/accounts/outbox") -> ScopeGrant:
    return ScopeGrant(
        scope_ref="scope.write.accounts",
        resource_type="repo_path",
        resource_locator=locator,
        access_mode="append",
        environment="dev",
        classification="confidential",
        tenant_boundary="tenant.acme",
        change_constraints=("append_only",),
        rollback_requirement="quarantine",
        append_only=True,
    )


def _model_policy(*, human_review: bool = True) -> ModelPolicy:
    return ModelPolicy(
        model_policy_ref="model_policy.object_truth_contracting",
        approved_model_classes=("frontier",),
        approved_model_ids=("openai/gpt-5.4",),
        approved_aliases=("contract-author",),
        reasoning_limit="high",
        tool_use_limit="read_only",
        data_handling_constraints=("redacted_hashes",),
        retention_constraints=("no_training",),
        human_review_requirement=human_review,
        disallowed_use_cases=("unsupported_authority",),
        permitted_input_classifications=("confidential",),
        permitted_output_classifications=("confidential",),
        approved_for_high_impact=True,
    )


def _verifier(
    *,
    verifier_type: str = "policy_compliance_check",
    independence: str = "independent",
) -> VerifierReference:
    return VerifierReference(
        verifier_ref="verifier.contract.behavior",
        verifier_type=verifier_type,
        applicability_rule="always",
        pass_criteria="all required evidence refs are present",
        failure_severity="blocker",
        independence_requirement=independence,
        evidence_output_ref="evidence.contract.behavior",
    )


def _staleness_policy(*, block_on_stale: bool = True) -> StalenessPolicy:
    return StalenessPolicy(
        staleness_policy_ref="staleness.task_environment.standard",
        review_interval_days=90,
        trigger_types=("sop_superseded", "tool_policy_changed", "hierarchy_retired"),
        block_on_stale=block_on_stale,
        grace_period_days=0,
        revalidation_requirements=("new_revision",),
    )


def _contract(**overrides) -> TaskEnvironmentContract:
    values = {
        "contract_id": "task_contract.account_sync.1",
        "task_ref": "task.account_sync",
        "hierarchy_node_id": "task.account_sync",
        "owner_ref": "owner.ops",
        "steward_ref": "steward.ops",
        "sop_refs": (_sop(),),
        "allowed_tools": (_tool(),),
        "read_scope": (_read_scope(),),
        "write_scope": (_write_scope(),),
        "model_policy": _model_policy(),
        "verifier_refs": (_verifier(),),
        "input_classification": "confidential",
        "output_classification": "confidential",
        "data_retention_ref": "retention.standard",
        "staleness_policy": _staleness_policy(),
        "revision_id": "rev.contract.1",
        "status": "active",
        "effective_from": NOW - timedelta(days=1),
        "requested_model_ref": "openai/gpt-5.4",
        "risk_level": "high",
    }
    values.update(overrides)
    contract = TaskEnvironmentContract(**values)
    return TaskEnvironmentContract(**{**values, "contract_hash": sha256_json(contract.to_json())})


def _context(**overrides) -> ContractEvaluationContext:
    values = {"hierarchy_nodes": _hierarchy(), "as_of": NOW}
    values.update(overrides)
    return ContractEvaluationContext(**values)


def _reason_codes(result) -> set[str]:
    return {state.reason_code for state in result.invalid_states}


def test_valid_contract_resolves_path_responsibility_and_policies() -> None:
    result = validate_task_environment_contract(_contract(), _context())

    assert result.ok is True
    assert result.hierarchy_path is not None
    assert result.hierarchy_path.canonical_path == (
        "enterprise.acme/program.gtm/workflow.account_sync/task.account_sync"
    )
    assert result.responsibility is not None
    assert result.responsibility.owner_ref == "owner.ops"
    assert result.staleness_decision.status == "fresh"


def test_missing_owner_and_steward_are_typed_invalid_states() -> None:
    hierarchy = (
        _node("enterprise.acme", "enterprise", None),
        _node(
            "task.account_sync",
            "task",
            "enterprise.acme",
            owner_ref="",
            steward_ref="",
        ),
    )
    result = validate_task_environment_contract(
        _contract(owner_ref=None, steward_ref=None),
        _context(hierarchy_nodes=hierarchy),
    )

    assert result.ok is False
    assert {
        "task_contract.hierarchy_owner_missing",
        "task_contract.hierarchy_steward_missing",
        "task_contract.contract_owner_missing",
        "task_contract.contract_steward_missing",
    } <= _reason_codes(result)


def test_retired_hierarchy_node_blocks_execution() -> None:
    hierarchy = (
        _node("enterprise.acme", "enterprise", None),
        _node(
            "task.account_sync",
            "task",
            "enterprise.acme",
            status="retired",
        ),
    )

    result = validate_task_environment_contract(_contract(), _context(hierarchy_nodes=hierarchy))

    assert result.ok is False
    assert "task_contract.hierarchy_node_not_active" in _reason_codes(result)


def test_contract_accepts_explicit_sop_gap_when_no_active_sop_exists() -> None:
    result = validate_task_environment_contract(
        _contract(
            sop_refs=(),
            sop_gap=SopGap(
                gap_ref="gap.sop.account_sync",
                owner_approval_ref="approval.owner.1",
                review_expires_at=NOW + timedelta(days=7),
            ),
        ),
        _context(),
    )

    assert result.ok is True


def test_deprecated_sop_is_not_assignable() -> None:
    result = validate_task_environment_contract(_contract(sop_refs=(_sop(status="deprecated"),)), _context())

    assert result.ok is False
    assert "task_contract.sop_not_assignable" in _reason_codes(result)


def test_write_scope_broader_than_inherited_policy_is_denied() -> None:
    bounds = ContractPolicyBounds(
        allowed_tool_refs=("tool.repo.read",),
        read_scope=(_read_scope("/clients/acme/accounts"),),
        write_scope=(_write_scope("/clients/acme/accounts/outbox"),),
        model_policy_refs=("model_policy.object_truth_contracting",),
        verifier_refs=("verifier.contract.behavior",),
    )
    result = validate_task_environment_contract(
        _contract(write_scope=(_write_scope("/clients/acme"),)),
        _context(policy_bounds=bounds),
    )

    assert result.ok is False
    assert "task_contract.write_scope_policy_broadened" in _reason_codes(result)


def test_unlisted_tool_operation_is_denied_by_default() -> None:
    decision = _contract().is_tool_operation_allowed("tool.never.registered", "write")

    assert decision.allowed is False
    assert decision.reason_code == "task_contract.tool_unlisted"


def test_stale_contract_blocks_when_policy_requires_revalidation() -> None:
    result = validate_task_environment_contract(
        _contract(),
        _context(
            staleness_signals=(
                StalenessSignal(
                    trigger_type="sop_superseded",
                    source_ref="sop.account_sync",
                    observed_at=NOW,
                    detail="SOP v2 is active",
                ),
            )
        ),
    )

    assert result.ok is False
    assert "task_contract.contract_stale" in _reason_codes(result)
    assert result.staleness_decision.blocks_execution is True


def test_high_risk_contract_requires_independent_verifier() -> None:
    result = validate_task_environment_contract(
        _contract(verifier_refs=(_verifier(independence="self"),)),
        _context(),
    )

    assert result.ok is False
    assert "task_contract.independent_verifier_missing" in _reason_codes(result)


def test_append_only_revision_chain_accepts_linear_history() -> None:
    first = RevisionRecord(
        revision_id="rev.contract.1",
        entity_type="task_environment_contract",
        entity_id="task_contract.account_sync",
        prior_revision_id=None,
        change_summary="initial",
        changed_by="operator:nate",
        changed_at=NOW,
        change_reason="phase_04",
        supersedes_effective_from=NOW,
        payload_hash="hash.1",
    )
    second = RevisionRecord(
        revision_id="rev.contract.2",
        entity_type="task_environment_contract",
        entity_id="task_contract.account_sync",
        prior_revision_id="rev.contract.1",
        change_summary="narrow scope",
        changed_by="operator:nate",
        changed_at=NOW + timedelta(minutes=1),
        change_reason="least_privilege",
        supersedes_effective_from=NOW + timedelta(minutes=1),
        payload_hash="hash.2",
    )

    chain = validate_append_only_revision_chain((first, second))
    next_check = validate_next_revision(first, second)

    assert chain.ok is True
    assert chain.head_revision_id == "rev.contract.2"
    assert next_check.ok is True


def test_revision_without_exact_predecessor_is_rejected() -> None:
    first = RevisionRecord(
        revision_id="rev.contract.1",
        entity_type="task_environment_contract",
        entity_id="task_contract.account_sync",
        prior_revision_id=None,
        change_summary="initial",
        changed_by="operator:nate",
        changed_at=NOW,
        change_reason="phase_04",
        supersedes_effective_from=NOW,
    )
    bad_second = RevisionRecord(
        revision_id="rev.contract.2",
        entity_type="task_environment_contract",
        entity_id="task_contract.account_sync",
        prior_revision_id=None,
        change_summary="overwrite",
        changed_by="operator:nate",
        changed_at=NOW + timedelta(minutes=1),
        change_reason="bad_update",
        supersedes_effective_from=NOW + timedelta(minutes=1),
    )

    check = validate_next_revision(first, bad_second)

    assert check.ok is False
    assert {state.reason_code for state in check.invalid_states} == {
        "task_contract.revision_prior_mismatch"
    }

