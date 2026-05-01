from __future__ import annotations

import pytest

from runtime.synthetic_data import SyntheticDataError, generate_synthetic_dataset


def test_synthetic_dataset_generates_quality_names_across_thousands() -> None:
    dataset = generate_synthetic_dataset(
        intent="Renewal risk data for CRM, billing, support, and Slack.",
        namespace="renewal-risk-demo",
        scenario_pack_refs=["renewal_risk"],
        object_counts={"Account": 1000, "Contact": 1000, "Ticket": 1000},
        seed="naming-scale-seed",
        reserved_terms=["Acme", "Praxis"],
    )

    names = [record["display_name"] for record in dataset["records"]]

    assert dataset["quality_state"] == "accepted"
    assert dataset["quality_report"]["collision_count"] == 0
    assert dataset["quality_report"]["reserved_term_hits"] == []
    assert dataset["quality_report"]["placeholder_name_hits"] == []
    assert len(names) == len(set(names)) == 3000
    assert all(not name.startswith("Synthetic ") for name in names)
    assert all(record["name_ref"].startswith("synthetic_name:renewal_risk_demo:") for record in dataset["records"])
    assert dataset["permissions"]["promotion_evidence_allowed"] is False


def test_synthetic_dataset_is_seed_deterministic() -> None:
    first = generate_synthetic_dataset(
        intent="Support escalation synthetic data.",
        namespace="support-demo",
        scenario_pack_refs=["support_escalation"],
        object_counts={"Ticket": 50, "Account": 50},
        seed="same-seed",
    )
    second = generate_synthetic_dataset(
        intent="Support escalation synthetic data.",
        namespace="support-demo",
        scenario_pack_refs=["support_escalation"],
        object_counts={"Ticket": 50, "Account": 50},
        seed="same-seed",
    )

    assert first["dataset_ref"] == second["dataset_ref"]
    assert first["name_plan"] == second["name_plan"]
    assert first["records"] == second["records"]


def test_synthetic_name_plan_fails_when_capacity_is_exceeded() -> None:
    with pytest.raises(SyntheticDataError) as exc:
        generate_synthetic_dataset(
            intent="Too many contacts.",
            namespace="capacity-demo",
            scenario_pack_refs=["crm_sync"],
            object_counts={"Contact": 90_000},
            seed="capacity-seed",
        )

    assert exc.value.reason_code == "synthetic_data.name_capacity_exceeded"
