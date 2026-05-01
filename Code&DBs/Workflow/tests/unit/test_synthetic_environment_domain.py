from __future__ import annotations

from decimal import Decimal

import pytest

from runtime.synthetic_data import generate_synthetic_dataset
from runtime.synthetic_environment import (
    SyntheticEnvironmentError,
    advance_synthetic_environment_clock,
    clear_synthetic_environment,
    create_synthetic_environment_from_dataset,
    diff_synthetic_environment,
    inject_synthetic_environment_event,
    reset_synthetic_environment,
)


def _dataset() -> dict:
    return generate_synthetic_dataset(
        intent="Renewal risk CRM and support demo.",
        namespace="environment-demo",
        scenario_pack_refs=["renewal_risk"],
        object_counts={"Account": 3, "Ticket": 3},
        seed="environment-seed",
    )


def test_environment_create_seeds_current_state_and_creation_effect() -> None:
    dataset = _dataset()
    dataset["quality_score"] = Decimal("1.0000")

    environment, effect = create_synthetic_environment_from_dataset(
        dataset=dataset,
        namespace="mutable-demo",
        seed="environment-v1",
        clock_time="2026-05-01T00:00:00Z",
    )

    assert environment["environment_ref"].startswith("synthetic_environment:mutable_demo:")
    assert environment["lifecycle_state"] == "active"
    assert environment["record_count"] == 6
    assert environment["current_record_count"] == 6
    assert environment["dirty_record_count"] == 0
    assert environment["seed_state_digest"] == environment["current_state_digest"]
    assert effect["effect_type"] == "environment.created"
    assert effect["changed_record_count"] == 0
    assert environment["permissions"]["outside_event_injection_allowed"] is True
    assert environment["metadata"]["source_quality_score"] == 1.0


def test_event_injection_records_effect_and_diff() -> None:
    environment, _ = create_synthetic_environment_from_dataset(
        dataset=_dataset(),
        namespace="mutable-demo",
        seed="environment-v1",
        clock_time="2026-05-01T00:00:00Z",
    )
    target_ref = environment["current_state"]["record_order"][0]

    mutated, effect = inject_synthetic_environment_event(
        environment,
        event_type="crm.owner_changed",
        event_payload={"owner_ref": "synthetic_owner:nate", "reason": "territory_rebalance"},
        target_refs=[target_ref],
        occurred_at="2026-05-01T01:00:00Z",
        sequence_number=2,
    )

    target = mutated["current_state"]["records"][target_ref]
    diff = diff_synthetic_environment(mutated)

    assert effect["effect_type"] == "environment.event_injected"
    assert effect["event_ref"].startswith("synthetic_environment_event:")
    assert effect["after_state_digest"] == mutated["current_state_digest"]
    assert target["fields"]["owner_ref"] == "synthetic_owner:nate"
    assert target["lineage"]["last_synthetic_environment_effect_sequence"] == 2
    assert diff["dirty_record_count"] == 1
    assert diff["records_changed_preview"][0]["record_ref"] == target_ref
    assert "owner_ref" in diff["records_changed_preview"][0]["changed_fields"]


def test_clear_and_reset_make_effects_instead_of_deleting_history() -> None:
    environment, _ = create_synthetic_environment_from_dataset(
        dataset=_dataset(),
        namespace="mutable-demo",
        seed="environment-v1",
    )

    cleared, clear_effect = clear_synthetic_environment(
        environment,
        reason="demo_restart",
        sequence_number=2,
    )
    reset, reset_effect = reset_synthetic_environment(
        cleared,
        reason="demo_restart_complete",
        sequence_number=3,
    )

    assert cleared["lifecycle_state"] == "cleared"
    assert cleared["current_record_count"] == 0
    assert cleared["dirty_record_count"] == cleared["record_count"]
    assert clear_effect["effect_type"] == "environment.cleared"
    assert clear_effect["changed_record_count"] == environment["record_count"]
    assert reset["lifecycle_state"] == "active"
    assert reset["current_state_digest"] == reset["seed_state_digest"]
    assert reset["dirty_record_count"] == 0
    assert reset_effect["effect_type"] == "environment.reset"


def test_clock_advance_records_time_without_state_drift() -> None:
    environment, _ = create_synthetic_environment_from_dataset(
        dataset=_dataset(),
        namespace="mutable-demo",
        seed="environment-v1",
        clock_time="2026-05-01T00:00:00Z",
    )

    advanced, effect = advance_synthetic_environment_clock(
        environment,
        seconds=3600,
        sequence_number=2,
    )

    assert advanced["clock_time"] == "2026-05-01T01:00:00Z"
    assert advanced["current_state_digest"] == environment["current_state_digest"]
    assert effect["effect_type"] == "environment.clock_advanced"
    assert effect["changed_record_count"] == 0


def test_event_injection_rejects_missing_targets() -> None:
    environment, _ = create_synthetic_environment_from_dataset(dataset=_dataset())

    with pytest.raises(SyntheticEnvironmentError) as exc:
        inject_synthetic_environment_event(
            environment,
            event_type="payment.failed",
            target_refs=["synthetic_record:missing"],
            sequence_number=2,
        )

    assert exc.value.reason_code == "synthetic_environment.target_not_found"
