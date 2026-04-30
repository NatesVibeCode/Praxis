from __future__ import annotations

from runtime.virtual_lab.simulation import (
    AutomationPredicate,
    AutomationRule,
    SimulationAction,
    SimulationAssertion,
    SimulationConfig,
    SimulationInitialState,
    SimulationScenario,
    SimulationVerifier,
    run_simulation_scenario,
    simulation_scenario_from_dict,
)
from runtime.virtual_lab.state import (
    ActorIdentity,
    build_environment_revision,
    build_seed_manifest,
    object_states_from_seed_manifest,
)


def _actor() -> ActorIdentity:
    return ActorIdentity(actor_id="agent.phase_07", actor_type="agent")


def _initial_state() -> SimulationInitialState:
    manifest = build_seed_manifest(
        [
            {
                "object_id": "account:001",
                "object_truth_ref": "object_truth.account.acme",
                "object_truth_version": "version.7",
                "projection_version": "projection.account.v1",
                "seed_parameters": {"include_contacts": True},
                "base_state": {
                    "name": "Acme",
                    "status": "prospect",
                    "lifecycle": {"owner": "marketing"},
                },
            }
        ]
    )
    revision = build_environment_revision(
        environment_id="virtual_lab.env.phase_07",
        revision_reason="simulation_seed",
        seed_manifest=manifest,
        config={"simulation_engine": "virtual_lab.simulation", "version": "1"},
        policy={"promotion": "verifier_required"},
        created_at="2026-04-30T16:00:00Z",
        created_by="agent.phase_07",
    )
    return SimulationInitialState(
        revision=revision,
        object_states=object_states_from_seed_manifest(revision),
    )


def _config(**updates):
    payload = {
        "seed": "seed.phase_07",
        "clock_start": "2026-04-30T17:00:00Z",
        "clock_step_seconds": 1,
        "max_actions": 20,
        "max_automation_firings": 10,
        "max_recursion_depth": 5,
    }
    payload.update(updates)
    return SimulationConfig(**payload)


def test_simulation_happy_path_runs_action_automation_assertion_and_verifier() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.happy_path",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=_actor(),
            ),
        ),
        automation_rules=(
            AutomationRule(
                rule_id="rule.assign_sales_owner",
                name="Assign sales owner after qualification",
                predicate=AutomationPredicate(
                    predicate_kind="payload_field_equals",
                    field_path=("overlay_patch", "status"),
                    expected="qualified",
                ),
                effects=(
                    SimulationAction(
                        action_id="automation.assign_sales_owner",
                        action_kind="patch_object",
                        object_id="account:001",
                        payload={"patch": {"lifecycle": {"owner": "sales"}}},
                        actor=_actor(),
                    ),
                ),
            ),
        ),
        assertions=(
            SimulationAssertion(
                assertion_id="assert.owner_is_sales",
                assertion_kind="final_object_field_equals",
                object_id="account:001",
                field_path=("lifecycle", "owner"),
                expected="sales",
            ),
        ),
        verifiers=(
            SimulationVerifier(
                verifier_id="verifier.two_patch_events",
                verifier_kind="trace_contains_event_type",
                event_type="object.patched",
                min_count=2,
            ),
        ),
    )

    result = run_simulation_scenario(scenario)

    assert result.status == "passed"
    assert result.stop_reason == "success"
    assert result.final_state[0].effective_state["status"] == "qualified"
    assert result.final_state[0].effective_state["lifecycle"]["owner"] == "sales"
    assert [item.rule_id for item in result.trace.automation_firings] == ["rule.assign_sales_owner"]
    assert [item.status for item in result.action_results] == ["succeeded", "succeeded"]
    assert result.assertion_results[0].passed is True
    assert result.verifier_results[0].status == "passed"


def test_unsupported_action_surfaces_typed_gap_and_blocks_promotion() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.unsupported_action",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.send_live_email",
                action_kind="send_live_email",
                object_id="account:001",
                payload={"subject": "not in the lab"},
                actor=_actor(),
            ),
        ),
    )

    result = run_simulation_scenario(scenario)

    assert result.status == "blocked"
    assert result.stop_reason == "unsupported_capability"
    assert result.gaps[0].code == "simulation.unsupported_action"
    assert result.blockers[0].code == "simulation.unsupported_action"
    assert result.final_state[0].effective_state["status"] == "prospect"


def test_deterministic_ordering_and_replay_are_stable_for_equal_priority_rules() -> None:
    rules = (
        AutomationRule(
            rule_id="rule.z_followup",
            name="Second by id",
            predicate=AutomationPredicate(
                predicate_kind="payload_field_equals",
                field_path=("overlay_patch", "status"),
                expected="qualified",
            ),
            priority=10,
            effects=(
                SimulationAction(
                    action_id="automation.z_followup",
                    action_kind="patch_object",
                    object_id="account:001",
                    payload={"patch": {"z_followup": True}},
                    actor=_actor(),
                ),
            ),
        ),
        AutomationRule(
            rule_id="rule.a_followup",
            name="First by id",
            predicate=AutomationPredicate(
                predicate_kind="payload_field_equals",
                field_path=("overlay_patch", "status"),
                expected="qualified",
            ),
            priority=10,
            effects=(
                SimulationAction(
                    action_id="automation.a_followup",
                    action_kind="patch_object",
                    object_id="account:001",
                    payload={"patch": {"a_followup": True}},
                    actor=_actor(),
                ),
            ),
        ),
    )
    scenario = SimulationScenario(
        scenario_id="scenario.deterministic_order",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=_actor(),
            ),
        ),
        automation_rules=rules,
    )

    first = run_simulation_scenario(scenario)
    second = run_simulation_scenario(scenario)

    assert first.to_json() == second.to_json()
    assert [item.rule_id for item in first.trace.automation_firings] == [
        "rule.a_followup",
        "rule.z_followup",
    ]
    assert [event.command_id for event in first.trace.state_events] == [
        "action.qualify_account",
        "automation.a_followup.automation_firing_1",
        "automation.z_followup.automation_firing_2",
    ]


def test_automation_loop_guard_blocks_unbounded_recursive_firing() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.loop_guard",
        initial_state=_initial_state(),
        config=_config(max_automation_firings=2, max_recursion_depth=10),
        actions=(
            SimulationAction(
                action_id="action.start_loop",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"loop": {"count": 1}}},
                actor=_actor(),
            ),
        ),
        automation_rules=(
            AutomationRule(
                rule_id="rule.always_patch",
                name="Always patch after any object event",
                predicate=AutomationPredicate(predicate_kind="always"),
                effects=(
                    SimulationAction(
                        action_id="automation.loop_patch",
                        action_kind="patch_object",
                        object_id="account:001",
                        payload={"patch": {"loop": {"guard_probe": True}}},
                        actor=_actor(),
                    ),
                ),
            ),
        ),
    )

    result = run_simulation_scenario(scenario)

    assert result.status == "blocked"
    assert result.stop_reason == "guardrail_exceeded"
    assert result.trace.automation_firings[-1].firing_id
    assert result.blockers[0].code == "simulation.automation_loop_guard.max_firings"


def test_assertion_failure_is_machine_readable_without_promotion_blocker() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.assertion_failure",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=_actor(),
            ),
        ),
        assertions=(
            SimulationAssertion(
                assertion_id="assert.status_active",
                assertion_kind="final_object_field_equals",
                object_id="account:001",
                field_path=("status",),
                expected="active",
            ),
        ),
    )

    result = run_simulation_scenario(scenario)

    assert result.status == "failed"
    assert result.stop_reason == "assertion_failed"
    assert result.assertion_results[0].to_json()["actual"] == "qualified"
    assert result.assertion_results[0].to_json()["expected"] == "active"
    assert result.blockers == ()


def test_verifier_output_reports_structured_findings() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.verifier_output",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=_actor(),
            ),
        ),
        verifiers=(
            SimulationVerifier(
                verifier_id="verifier.needs_tombstone",
                verifier_kind="trace_contains_event_type",
                event_type="object.tombstoned",
                min_count=1,
            ),
        ),
    )

    result = run_simulation_scenario(scenario)

    assert result.status == "failed"
    assert result.stop_reason == "verifier_failed"
    assert result.verifier_results[0].status == "failed"
    assert result.verifier_results[0].findings[0] == {
        "code": "simulation.verifier.event_count",
        "event_type": "object.tombstoned",
        "expected_min_count": 1,
        "actual_count": 0,
    }


def test_green_status_requires_at_least_one_verifier() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.no_verifier",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=_actor(),
            ),
        ),
        assertions=(
            SimulationAssertion(
                assertion_id="assert.status_qualified",
                assertion_kind="final_object_field_equals",
                object_id="account:001",
                field_path=("status",),
                expected="qualified",
            ),
        ),
    )

    result = run_simulation_scenario(scenario)

    assert result.status == "blocked"
    assert result.stop_reason == "verifier_failed"
    assert result.blockers[0].code == "simulation.verifier_required"


def test_simulation_scenario_from_dict_round_trips_domain_json() -> None:
    scenario = SimulationScenario(
        scenario_id="scenario.round_trip",
        initial_state=_initial_state(),
        config=_config(),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=_actor(),
            ),
        ),
        verifiers=(
            SimulationVerifier(
                verifier_id="verifier.no_blockers",
                verifier_kind="no_blockers",
            ),
        ),
    )

    parsed = simulation_scenario_from_dict(scenario.to_json())

    assert parsed.to_json() == scenario.to_json()
