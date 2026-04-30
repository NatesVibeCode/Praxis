from __future__ import annotations

from types import SimpleNamespace

from runtime.operations.commands import virtual_lab_simulation as commands
from runtime.operations.queries import virtual_lab_simulation as queries
from runtime.virtual_lab.simulation import (
    SimulationAction,
    SimulationConfig,
    SimulationInitialState,
    SimulationScenario,
    SimulationVerifier,
)
from runtime.virtual_lab.state import (
    ActorIdentity,
    build_environment_revision,
    build_seed_manifest,
    object_states_from_seed_manifest,
)


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


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
        scenario_id="scenario.operations",
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


def test_virtual_lab_simulation_run_persists_result_and_event_payload(monkeypatch) -> None:
    persist_calls: list[dict[str, object]] = []
    scenario = _scenario()

    def _persist(conn, **kwargs):
        persist_calls.append(kwargs)
        return {
            "run_id": kwargs["result"]["run_id"],
            "scenario_id": kwargs["result"]["scenario_id"],
            "status": kwargs["result"]["status"],
        }

    monkeypatch.setattr(commands, "persist_virtual_lab_simulation_run", _persist)

    result = commands.handle_virtual_lab_simulation_run(
        commands.RunVirtualLabSimulationCommand(
            scenario=scenario.to_json(),
            task_contract_ref="task_environment_contract.account_sync",
            integration_action_contract_refs=["integration_action_contract.crm.patch_account"],
            automation_snapshot_refs=["snapshot.integration_automation_rule.assign_owner"],
            observed_by_ref="operator:nate",
            source_ref="phase_07_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "virtual_lab_simulation_run"
    assert result["status"] == "passed"
    assert result["event_payload"]["environment_id"] == "virtual_lab.env.phase_07"
    assert result["event_payload"]["verifier_count"] == 1
    assert result["event_payload"]["blocker_count"] == 0
    assert persist_calls[0]["task_contract_ref"] == "task_environment_contract.account_sync"
    assert persist_calls[0]["integration_action_contract_refs"] == [
        "integration_action_contract.crm.patch_account"
    ]


def test_virtual_lab_simulation_read_lists_and_describes(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_simulation_runs",
        lambda conn, status=None, scenario_id=None, environment_id=None, revision_id=None, limit=50: [
            {"run_id": "virtual_lab_simulation_run.demo", "status": status}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_virtual_lab_simulation_run",
        lambda conn, run_id, **kwargs: {
            "run_id": run_id,
            "runtime_events": [{}] if kwargs["include_events"] else [],
        },
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_simulation_events",
        lambda conn, run_id, event_type=None, source_area=None, limit=50: [
            {"run_id": run_id, "event_type": event_type, "source_area": source_area}
        ],
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_simulation_verifiers",
        lambda conn, run_id, status=None, limit=50: [
            {"run_id": run_id, "status": status}
        ],
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_simulation_blockers",
        lambda conn, run_id, code=None, limit=50: [
            {"run_id": run_id, "code": code}
        ],
    )

    listed = queries.handle_virtual_lab_simulation_read(
        queries.QueryVirtualLabSimulationRead(action="list_runs", status="blocked"),
        _subsystems(),
    )
    described = queries.handle_virtual_lab_simulation_read(
        queries.QueryVirtualLabSimulationRead(
            action="describe_run",
            run_id="virtual_lab_simulation_run.demo",
            include_events=True,
        ),
        _subsystems(),
    )
    events = queries.handle_virtual_lab_simulation_read(
        queries.QueryVirtualLabSimulationRead(
            action="list_events",
            run_id="virtual_lab_simulation_run.demo",
            event_type="action.result",
            source_area="action",
        ),
        _subsystems(),
    )
    verifiers = queries.handle_virtual_lab_simulation_read(
        queries.QueryVirtualLabSimulationRead(
            action="list_verifiers",
            run_id="virtual_lab_simulation_run.demo",
            status="passed",
        ),
        _subsystems(),
    )
    blockers = queries.handle_virtual_lab_simulation_read(
        queries.QueryVirtualLabSimulationRead(
            action="list_blockers",
            run_id="virtual_lab_simulation_run.demo",
            blocker_code="simulation.verifier_required",
        ),
        _subsystems(),
    )

    assert listed["items"][0]["status"] == "blocked"
    assert described["run"]["run_id"] == "virtual_lab_simulation_run.demo"
    assert events["items"][0]["event_type"] == "action.result"
    assert verifiers["items"][0]["status"] == "passed"
    assert blockers["items"][0]["code"] == "simulation.verifier_required"
