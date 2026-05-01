from __future__ import annotations

from runtime.operations.commands import synthetic_environment as commands
from runtime.operations.queries import synthetic_environment as queries
from runtime.synthetic_data import generate_synthetic_dataset
from runtime.synthetic_environment import create_synthetic_environment_from_dataset


class _Subsystems:
    def get_pg_conn(self):
        return object()


def _dataset() -> dict:
    return generate_synthetic_dataset(
        intent="Support escalation synthetic data.",
        namespace="ops-demo",
        scenario_pack_refs=["support_escalation"],
        object_counts={"Ticket": 2, "Account": 2},
        seed="ops-seed",
    )


def test_create_command_loads_dataset_persists_environment_and_event(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        commands,
        "load_synthetic_dataset",
        lambda conn, dataset_ref, include_records=True, limit=100000: _dataset(),
    )

    def _persist(conn, *, environment, effect=None):
        captured["environment"] = environment
        captured["effect"] = effect
        return environment

    monkeypatch.setattr(commands, "persist_synthetic_environment", _persist)

    result = commands.handle_synthetic_environment_create(
        commands.CreateSyntheticEnvironmentCommand(
            dataset_ref="synthetic_dataset.demo",
            namespace="ops-demo",
            seed="environment-seed",
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "synthetic_environment_create"
    assert result["environment_ref"].startswith("synthetic_environment:ops_demo:")
    assert result["event_payload"]["effect_type"] == "environment.created"
    assert captured["effect"]["sequence_number"] == 1


def test_mutation_commands_return_effect_payloads(monkeypatch) -> None:
    environment, _ = create_synthetic_environment_from_dataset(dataset=_dataset(), namespace="ops-demo")
    target_ref = environment["current_state"]["record_order"][0]
    stored = {"environment": environment}

    monkeypatch.setattr(
        commands,
        "load_synthetic_environment",
        lambda conn, environment_ref: stored["environment"],
    )
    monkeypatch.setattr(
        commands,
        "next_synthetic_environment_effect_sequence",
        lambda conn, environment_ref: 2,
    )

    def _persist(conn, *, environment, effect=None):
        stored["environment"] = environment
        stored["effect"] = effect
        return environment

    monkeypatch.setattr(commands, "persist_synthetic_environment", _persist)

    injected = commands.handle_synthetic_environment_event_inject(
        commands.InjectSyntheticEnvironmentEventCommand(
            environment_ref=environment["environment_ref"],
            event_type="payment.failed",
            event_payload={"failure_reason": "card_declined"},
            target_refs=[target_ref],
        ),
        _Subsystems(),
    )
    cleared = commands.handle_synthetic_environment_clear(
        commands.ClearSyntheticEnvironmentCommand(environment_ref=environment["environment_ref"]),
        _Subsystems(),
    )
    reset = commands.handle_synthetic_environment_reset(
        commands.ResetSyntheticEnvironmentCommand(environment_ref=environment["environment_ref"]),
        _Subsystems(),
    )
    clock = commands.handle_synthetic_environment_clock_advance(
        commands.AdvanceSyntheticEnvironmentClockCommand(
            environment_ref=environment["environment_ref"],
            seconds=60,
        ),
        _Subsystems(),
    )

    assert injected["effect"]["effect_type"] == "environment.event_injected"
    assert injected["event_payload"]["changed_record_count"] == 1
    assert cleared["environment"]["lifecycle_state"] == "cleared"
    assert reset["environment"]["lifecycle_state"] == "active"
    assert clock["effect"]["effect_type"] == "environment.clock_advanced"


def test_read_query_lists_describes_effects_and_diff(monkeypatch) -> None:
    environment, effect = create_synthetic_environment_from_dataset(dataset=_dataset(), namespace="ops-demo")

    monkeypatch.setattr(
        queries,
        "list_synthetic_environments",
        lambda conn, namespace=None, source_dataset_ref=None, lifecycle_state=None, limit=50: [environment],
    )
    monkeypatch.setattr(
        queries,
        "load_synthetic_environment",
        lambda conn, environment_ref: environment,
    )
    monkeypatch.setattr(
        queries,
        "list_synthetic_environment_effects",
        lambda conn, environment_ref, effect_type=None, limit=100: [effect],
    )

    listed = queries.handle_synthetic_environment_read(
        queries.QuerySyntheticEnvironmentRead(namespace="ops-demo", include_state=False),
        _Subsystems(),
    )
    described = queries.handle_synthetic_environment_read(
        queries.QuerySyntheticEnvironmentRead(
            action="describe_environment",
            environment_ref=environment["environment_ref"],
            include_effects=True,
        ),
        _Subsystems(),
    )
    effects = queries.handle_synthetic_environment_read(
        queries.QuerySyntheticEnvironmentRead(
            action="list_effects",
            environment_ref=environment["environment_ref"],
        ),
        _Subsystems(),
    )
    diff = queries.handle_synthetic_environment_read(
        queries.QuerySyntheticEnvironmentRead(action="diff", environment_ref=environment["environment_ref"]),
        _Subsystems(),
    )

    assert listed["count"] == 1
    assert "current_state" not in listed["environments"][0]
    assert described["effects"] == [effect]
    assert effects["count"] == 1
    assert diff["diff"]["dirty_record_count"] == 0
