from __future__ import annotations

from typing import Any

import pytest

from storage.postgres.transport_eligibility_repository import (
    PostgresTransportEligibilityRepository,
)
from storage.postgres.validators import PostgresWriteError
from runtime.workflow._admission import _enforce_effective_provider_job_catalog


class FakeConn:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM effective_private_provider_job_catalog" not in normalized:
            raise AssertionError(f"unexpected query: {query}")
        return self.rows


def test_effective_provider_job_catalog_reads_effective_view_only() -> None:
    conn = FakeConn(
        rows=[
            {
                "runtime_profile_ref": "nate-private",
                "job_type": "build",
                "transport_type": "CLI",
                "adapter_type": "cli_llm",
                "provider_slug": "anthropic",
                "model_slug": "claude-opus-4-7",
                "model_version": "claude-opus-4-7",
                "cost_structure": "subscription_included",
                "cost_metadata": {"billing_mode": "subscription_included"},
                "reason_code": "catalog.available",
                "candidate_ref": "candidate.anthropic.cli.claude-opus-4-7",
                "provider_ref": "provider.anthropic",
                "source_refs": ["table.task_type_routing"],
                "projected_at": "2026-04-26T00:00:00Z",
                "projection_ref": "projection.private_provider_control_plane_snapshot",
            }
        ]
    )

    rows = PostgresTransportEligibilityRepository(
        conn
    ).list_effective_provider_job_catalog(
        runtime_profile_ref="nate-private",
        job_type="build",
        transport_type="cli",
        provider_slug="anthropic",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.runtime_profile_ref == "nate-private"
    assert row.job_type == "build"
    assert row.transport_type == "CLI"
    assert row.adapter_type == "cli_llm"
    assert row.provider_slug == "anthropic"
    assert row.model_slug == "claude-opus-4-7"
    assert row.cost_structure == "subscription_included"
    assert row.source_refs == ("table.task_type_routing",)
    assert conn.calls[0][1] == (
        "nate-private",
        "build",
        "CLI",
        "anthropic",
        None,
    )


def test_effective_provider_job_catalog_requires_runtime_profile_ref() -> None:
    with pytest.raises(PostgresWriteError) as exc_info:
        PostgresTransportEligibilityRepository(
            FakeConn()
        ).list_effective_provider_job_catalog(runtime_profile_ref="")

    assert exc_info.value.reason_code == "postgres.invalid_submission"
    assert "runtime_profile_ref" in str(exc_info.value)


def test_submission_gate_rejects_provider_model_absent_from_effective_catalog() -> None:
    with pytest.raises(RuntimeError) as exc_info:
        _enforce_effective_provider_job_catalog(
            FakeConn(rows=[]),
            runtime_profile_ref="nate-private",
            route_task_type="build",
            failover_chain=["anthropic/claude-sonnet-4-6"],
            job_label="phase_build",
        )

    assert "no effective provider job catalog candidate" in str(exc_info.value)
    assert "phase_build" in str(exc_info.value)


def test_submission_gate_allows_provider_model_present_in_effective_catalog() -> None:
    _enforce_effective_provider_job_catalog(
        FakeConn(
            rows=[
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": "build",
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-opus-4-7",
                    "model_version": "claude-opus-4-7",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {},
                    "reason_code": "catalog.available",
                    "candidate_ref": "candidate.anthropic.cli.claude-opus-4-7",
                    "provider_ref": "provider.anthropic",
                    "source_refs": [],
                    "projected_at": None,
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                }
            ]
        ),
        runtime_profile_ref="nate-private",
        route_task_type="build",
        failover_chain=["anthropic/claude-opus-4-7"],
        job_label="phase_build",
    )
