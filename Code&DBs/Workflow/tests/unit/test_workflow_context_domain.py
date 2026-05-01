from __future__ import annotations

import pytest

from runtime.workflow_context import (
    WorkflowContextError,
    build_binding,
    build_review_packet,
    compute_confidence,
    compile_workflow_context,
    guardrail_check,
    scenario_pack_registry,
    transition_context_pack,
)
from runtime.virtual_lab.simulation import run_simulation_scenario, simulation_scenario_from_dict
from runtime.virtual_lab.state import environment_revision_from_dict, object_state_record_from_dict


def test_compile_renewal_risk_synthetic_context_is_deterministic() -> None:
    first = compile_workflow_context(
        intent="When an account is a renewal risk, check CRM, billing, support, and Slack.",
        workflow_ref="workflow.renewal_risk.demo",
        context_mode="synthetic",
        seed="demo-seed",
    )
    second = compile_workflow_context(
        intent="When an account is a renewal risk, check CRM, billing, support, and Slack.",
        workflow_ref="workflow.renewal_risk.demo",
        context_mode="synthetic",
        seed="demo-seed",
    )

    assert first["context_ref"] == second["context_ref"]
    assert first["synthetic_world"] == second["synthetic_world"]
    assert "renewal_risk" in first["scenario_pack_refs"]
    assert {entity["label"] for entity in first["entities"]} >= {
        "CRM",
        "Billing",
        "Support",
        "Slack",
        "Account",
        "Subscription",
        "Ticket",
    }
    assert first["synthetic_world"]["permissions"] == {
        "live_writes_allowed": False,
        "customer_data_allowed": False,
        "promotion_evidence_allowed": False,
    }
    assert first["synthetic_world"]["virtual_lab"] == second["synthetic_world"]["virtual_lab"]


def test_synthetic_context_projects_to_virtual_lab_simulation() -> None:
    context = compile_workflow_context(
        intent="When an account is a renewal risk, check CRM, billing, support, and Slack.",
        workflow_ref="workflow.renewal_risk.virtual_lab",
        context_mode="synthetic",
        scenario_pack_refs=["renewal_risk"],
        seed="virtual-lab-seed",
    )

    virtual_lab = context["synthetic_world"]["virtual_lab"]
    state_payload = virtual_lab["state_record_payload"]
    revision = environment_revision_from_dict(state_payload["environment_revision"])
    object_states = [object_state_record_from_dict(item) for item in state_payload["object_states"]]
    scenario = simulation_scenario_from_dict(virtual_lab["simulation_scenario"])
    result = run_simulation_scenario(scenario)

    assert revision.environment_id.startswith("virtual_lab.env.workflow_context.")
    assert len(object_states) == len(context["synthetic_world"]["records"])
    assert result.status == "passed"
    assert result.stop_reason == "success"
    assert result.trace.transitions
    assert all(transition.pre_state_digest != transition.post_state_digest for transition in result.trace.transitions)
    assert {verifier.status for verifier in result.verifier_results} == {"passed"}
    assert not result.blockers


def test_all_scenario_packs_compile_into_context() -> None:
    registry = scenario_pack_registry()

    for pack_ref in registry:
        context = compile_workflow_context(
            intent=f"Build a standalone workflow for {pack_ref}",
            context_mode="synthetic",
            scenario_pack_refs=[pack_ref],
            seed="scenario-seed",
        )
        assert context["scenario_pack_refs"] == [pack_ref]
        assert context["entities"]
        assert context["verifier_expectations"]
        assert context["synthetic_world"]["synthetic"] is True


def test_synthetic_context_cannot_promote_without_verified_evidence() -> None:
    context = compile_workflow_context(
        intent="Renewal risk automation",
        context_mode="synthetic",
        seed="promotion-seed",
    )

    guardrail = guardrail_check(context, target_truth_state="promoted")

    assert guardrail["allowed"] is False
    assert {
        item["reason_code"]
        for item in guardrail["no_go_conditions"]
    } >= {
        "workflow_context.synthetic_or_inferred_cannot_promote",
        "workflow_context.verified_evidence_required",
    }
    with pytest.raises(WorkflowContextError):
        transition_context_pack(
            context,
            to_truth_state="promoted",
            transition_reason="try live promotion from fake world",
        )


def test_verified_context_can_promote_when_evidence_and_confidence_are_real() -> None:
    context = compile_workflow_context(
        intent="CRM sync",
        context_mode="inferred",
        evidence=[{"evidence_ref": "schema", "evidence_tier": "verified"}],
    )
    verified, _transition = transition_context_pack(
        context,
        to_truth_state="verified",
        transition_reason="verifier passed against observed Object Truth",
        evidence=[{"evidence_ref": "verifier.run.1", "evidence_tier": "verified"}],
    )

    guardrail = guardrail_check(verified, target_truth_state="promoted")

    assert guardrail["allowed"] is True
    assert guardrail["no_go_conditions"] == []


def test_confidence_tracks_evidence_tiers_and_unknown_mutators() -> None:
    documented = compute_confidence(
        truth_state="documented",
        evidence=[{"evidence_ref": "sop", "evidence_tier": "documented"}],
    )
    schema_bound = compute_confidence(
        truth_state="documented",
        evidence=[{"evidence_ref": "schema.account", "evidence_tier": "schema_bound"}],
    )
    observed = compute_confidence(
        truth_state="documented",
        evidence=[{"evidence_ref": "object_truth.account", "evidence_tier": "observed"}],
    )
    verified = compute_confidence(
        truth_state="documented",
        evidence=[{"evidence_ref": "verifier.account", "evidence_tier": "verified"}],
    )
    risky = compute_confidence(
        truth_state="observed",
        evidence=[{"evidence_ref": "warehouse.unknown_mutator", "evidence_tier": "observed"}],
    )
    stale = compute_confidence(
        truth_state="observed",
        evidence=[{"evidence_ref": "object_truth.account", "evidence_tier": "observed", "freshness_state": "stale"}],
    )

    assert documented["score"] < schema_bound["score"] < observed["score"] < verified["score"]
    assert risky["score"] < observed["score"]
    assert risky["inputs"]["unknown_mutator_risk"] is True
    assert stale["score"] < observed["score"]
    assert stale["inputs"]["freshness_state"] == "stale"


def test_anonymized_verified_evidence_cannot_prove_live_promotion() -> None:
    context = compile_workflow_context(
        intent="Support escalation based on anonymized ticket samples",
        context_mode="inferred",
        evidence=[
            {
                "evidence_ref": "sample.anonymized.support_tickets",
                "evidence_tier": "verified",
                "anonymized": True,
            }
        ],
    )
    verified, _transition = transition_context_pack(
        context,
        to_truth_state="verified",
        transition_reason="anonymized operational sample verified by parser",
        evidence=[{"evidence_ref": "verifier.sample.parser", "evidence_tier": "verified", "anonymized": True}],
    )

    guardrail = guardrail_check(verified, target_truth_state="promoted")

    assert guardrail["allowed"] is False
    assert {
        item["reason_code"]
        for item in guardrail["no_go_conditions"]
    } >= {"workflow_context.live_evidence_required"}
    with pytest.raises(WorkflowContextError):
        transition_context_pack(
            verified,
            to_truth_state="promoted",
            transition_reason="try live promotion from anonymized samples",
        )


def test_evidence_declared_unknown_mutator_blocks_promotion() -> None:
    context = compile_workflow_context(
        intent="CRM renewal risk with warehouse writers",
        context_mode="inferred",
        evidence=[
            {
                "evidence_ref": "object_truth.account.observed",
                "evidence_tier": "observed",
                "unknown_mutator_risk": True,
            }
        ],
    )

    assert context["unknown_mutator_risk"] is True
    guardrail = guardrail_check(
        {
            **context,
            "truth_state": "verified",
            "confidence": {"score": 0.93, "state": "verified"},
            "evidence_refs": [
                {"evidence_ref": "verifier.object_truth.account", "evidence_tier": "verified"},
            ],
        },
        target_truth_state="promoted",
    )

    assert guardrail["allowed"] is False
    assert {
        item["reason_code"]
        for item in guardrail["no_go_conditions"]
    } >= {"workflow_context.unknown_mutator_risk"}


def test_review_packet_blocks_trust_boundary_without_freezing_autopilot() -> None:
    context = compile_workflow_context(
        intent="Renewal risk automation",
        context_mode="synthetic",
        seed="review-packet-seed",
        unknown_mutator_risk=True,
    )
    guardrail = guardrail_check(context, target_truth_state="promoted")

    packet = build_review_packet(
        context_ref=context["context_ref"],
        truth_state=context["truth_state"],
        confidence=context["confidence"],
        blockers=context["blockers"],
        guardrail=guardrail,
        binding=None,
    )

    assert packet["autopilot_allowed"] is True
    assert packet["human_review_blocks_all_work"] is False
    assert "continue_building" in packet["safe_next_llm_actions"]
    assert "promote_context" in packet["autopilot_scope"]["blocked_until_review"]
    assert {
        item["decision_type"]
        for item in packet["queue_items"]
    } >= {
        "promotion_or_live_trust_boundary",
        "accepted_risk_or_blocker_resolution",
    }


def test_review_packet_queues_sensitive_binding_and_source_authority_decision() -> None:
    context = compile_workflow_context(
        intent="Bind synthetic Account to a non Object Truth authority",
        context_mode="synthetic",
        seed="source-authority-review",
    )
    account = next(entity for entity in context["entities"] if entity["label"] == "Account")
    binding = build_binding(
        pack=context,
        entity=account,
        target_ref="external_authority.salesforce.Account",
        target_authority_domain="authority.salesforce",
        risk_level="high",
    )

    packet = build_review_packet(
        context_ref=context["context_ref"],
        truth_state=context["truth_state"],
        confidence=context["confidence"],
        blockers=context["blockers"],
        guardrail=binding["guardrail"],
        binding=binding,
    )

    assert packet["autopilot_allowed"] is True
    assert {
        item["decision_type"]
        for item in packet["queue_items"]
    } >= {"sensitive_binding_review", "source_authority_decision"}
    assert "accept_binding" in packet["autopilot_scope"]["blocked_until_review"]
    assert packet["human_review_blocks_all_work"] is False


def test_high_risk_binding_requires_review_before_acceptance() -> None:
    context = compile_workflow_context(
        intent="Bind synthetic Account to Salesforce Account",
        context_mode="synthetic",
        seed="binding-seed",
    )
    account = next(entity for entity in context["entities"] if entity["label"] == "Account")

    proposed = build_binding(
        pack=context,
        entity=account,
        target_ref="object_truth_object_version:abc123",
        risk_level="high",
    )

    assert proposed["requires_review"] is True
    assert proposed["binding_state"] == "proposed"
    with pytest.raises(WorkflowContextError):
        build_binding(
            pack=context,
            entity=account,
            target_ref="object_truth_object_version:abc123",
            risk_level="high",
            binding_state="accepted",
        )
