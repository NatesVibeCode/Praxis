from __future__ import annotations

from storage.postgres import virtual_lab_simulation_repository as repo
from runtime.virtual_lab.simulation import (
    SimulationAction,
    SimulationConfig,
    SimulationInitialState,
    SimulationScenario,
    SimulationVerifier,
    run_simulation_scenario,
)
from runtime.virtual_lab.state import (
    ActorIdentity,
    build_environment_revision,
    build_seed_manifest,
    object_states_from_seed_manifest,
)


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO virtual_lab_simulation_runs" in sql:
            return {
                "run_id": args[0],
                "scenario_id": args[1],
                "scenario_digest": args[2],
                "config_digest": args[3],
                "environment_id": args[4],
                "revision_id": args[5],
                "status": args[7],
                "runtime_event_count": args[13],
                "state_event_count": args[14],
                "transition_count": args[15],
                "action_count": args[12],
            }
        return {}

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def _scenario() -> SimulationScenario:
    manifest = build_seed_manifest(
        [
            {
                "object_id": "account:001",
                "object_truth_ref": "object_truth.account.acme",
                "object_truth_version": "version.7",
                "projection_version": "projection.account.v1",
                "seed_parameters": {"include_contacts": True},
                "base_state": {"name": "Acme", "status": "prospect"},
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
    return SimulationScenario(
        scenario_id="scenario.repository",
        initial_state=SimulationInitialState(
            revision=revision,
            object_states=object_states_from_seed_manifest(revision),
        ),
        config=SimulationConfig(
            seed="seed.phase_07",
            clock_start="2026-04-30T17:00:00Z",
        ),
        actions=(
            SimulationAction(
                action_id="action.qualify_account",
                action_kind="patch_object",
                object_id="account:001",
                payload={"patch": {"status": "qualified"}},
                actor=ActorIdentity(actor_id="agent.phase_07", actor_type="agent"),
            ),
        ),
        verifiers=(
            SimulationVerifier(
                verifier_id="verifier.no_blockers",
                verifier_kind="no_blockers",
            ),
        ),
    )


def test_persist_virtual_lab_simulation_run_writes_run_and_child_records() -> None:
    conn = _RecordingConn()
    scenario = _scenario()
    result = run_simulation_scenario(scenario)

    persisted = repo.persist_virtual_lab_simulation_run(
        conn,
        scenario=scenario.to_json(),
        result=result.to_json(),
        task_contract_ref="task_environment_contract.account_sync",
        integration_action_contract_refs=["integration_action_contract.crm.patch_account"],
        automation_snapshot_refs=["snapshot.integration_automation_rule.assign_owner"],
        observed_by_ref="operator:nate",
        source_ref="phase_07_test",
    )

    assert "INSERT INTO virtual_lab_simulation_runs" in conn.fetchrow_calls[0][0]
    assert persisted["run_id"] == result.run_id
    assert persisted["status"] == "passed"
    assert persisted["runtime_event_count"] == len(result.trace.events)
    assert persisted["state_event_count"] == len(result.trace.state_events)
    assert persisted["transition_count"] == len(result.trace.transitions)
    assert persisted["action_count"] == len(result.action_results)
    assert any("DELETE FROM virtual_lab_simulation_runtime_events" in call[0] for call in conn.execute_calls)
    assert any("INSERT INTO virtual_lab_simulation_runtime_events" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_simulation_state_events" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_simulation_transitions" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_simulation_action_results" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_simulation_verifier_results" in call[0] for call in conn.batch_calls)
