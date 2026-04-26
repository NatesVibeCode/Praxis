from __future__ import annotations

from typing import Any

from storage.postgres.provider_control_plane_repository import (
    PostgresProviderControlPlaneRepository,
)


class FakeConn:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.execute_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM private_provider_control_plane_snapshot" in normalized:
            return [
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
                    "credential_availability_state": "missing",
                    "credential_sources": ["ANTHROPIC_API_KEY"],
                    "credential_observations": [
                        {
                            "credential_ref": "ANTHROPIC_API_KEY",
                            "status": "failed",
                            "source_kind": "env",
                        }
                    ],
                    "capability_state": "removed",
                    "is_runnable": False,
                    "breaker_state": "OPEN",
                    "manual_override_state": "OPEN",
                    "primary_removal_reason_code": "circuit_breaker.open",
                    "removal_reasons": [
                        {
                            "reason_code": "circuit_breaker.open",
                            "source_ref": "projection.circuit_breakers",
                            "details": {"breaker_state": "OPEN"},
                        }
                    ],
                    "candidate_ref": "candidate.anthropic.cli.claude-opus-4-7",
                    "provider_ref": "provider.anthropic",
                    "source_refs": ["table.provider_circuit_breaker_state"],
                    "projected_at": "2026-04-26T00:00:00Z",
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                }
            ]
        if "FROM effective_provider_circuit_breaker_state" in normalized:
            return [
                {
                    "provider_slug": "anthropic",
                    "runtime_state": "OPEN",
                    "effective_state": "OPEN",
                    "manual_override_state": "OPEN",
                    "manual_override_reason": "maintenance",
                    "failure_count": 4,
                    "success_count": 2,
                    "failure_threshold": 3,
                    "recovery_timeout_s": 60,
                    "half_open_max_calls": 1,
                    "last_failure_at": None,
                    "opened_at": None,
                    "half_open_after": None,
                    "half_open_calls": 0,
                    "updated_at": "2026-04-26T00:00:00Z",
                    "projected_at": "2026-04-26T00:00:00Z",
                    "projection_ref": "projection.circuit_breakers",
                }
            ]
        raise AssertionError(f"unexpected query: {query}")

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM authority_projection_state" in normalized:
            return {
                "projection_ref": str(args[0]),
                "freshness_status": "fresh",
                "last_refreshed_at": "2026-04-26T00:00:01Z",
                "error_code": None,
                "error_detail": None,
            }
        raise AssertionError(f"unexpected query: {query}")


def test_repository_reads_control_plane_snapshot_rows() -> None:
    repo = PostgresProviderControlPlaneRepository(FakeConn())

    rows = repo.list_provider_control_plane_rows(runtime_profile_ref="nate-private")

    assert len(rows) == 1
    row = rows[0]
    assert row.provider_slug == "anthropic"
    assert row.breaker_state == "OPEN"
    assert row.credential_availability_state == "missing"
    assert row.credential_sources == ("ANTHROPIC_API_KEY",)
    assert row.credential_observations[0]["credential_ref"] == "ANTHROPIC_API_KEY"
    assert row.primary_removal_reason_code == "circuit_breaker.open"
    assert row.removal_reasons[0]["reason_code"] == "circuit_breaker.open"


def test_repository_reads_projection_freshness_and_circuit_state() -> None:
    repo = PostgresProviderControlPlaneRepository(FakeConn())

    freshness = repo.get_projection_freshness("projection.circuit_breakers")
    rows = repo.list_provider_circuit_states(provider_slug="anthropic")

    assert freshness.projection_ref == "projection.circuit_breakers"
    assert freshness.freshness_status == "fresh"
    assert rows[0].provider_slug == "anthropic"
    assert rows[0].manual_override_state == "OPEN"
