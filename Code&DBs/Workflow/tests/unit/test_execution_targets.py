"""Unit tests for Execution Target Authority helpers and CQRS handlers."""

from __future__ import annotations

from typing import Any

from runtime.execution_targets import (
    CONTROL_PLANE_API_TARGET,
    DOCKER_THIN_CLI_TARGET,
    PROCESS_SANDBOX_TARGET,
    candidate_set_hash,
    enrich_dispatch_candidate,
    resolve_target_for_transport,
)
from runtime.operations.commands.execution_dispatch_choice import (
    CommitDispatchChoiceCommand,
    handle_commit_dispatch_choice,
)
from runtime.operations.queries.execution_targets import (
    QueryExecutionTargetsResolve,
    handle_query_execution_targets_resolve,
)
from runtime.operations.queries.operator_synthesis import (
    QueryExecutionProof,
    handle_query_execution_proof,
)


class _StubConn:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows
        self.inserts: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        if "INSERT INTO execution_dispatch_choices" in sql:
            self.inserts.append((sql, args))
            return []
        if "WHERE task_type" in sql and args:
            return [dict(row) for row in self._rows if row.get("task_type") == args[0]]
        return [dict(row) for row in self._rows]


class _StubSubsystems:
    def __init__(self, rows: list[dict[str, Any]]):
        self.conn = _StubConn(rows)

    def get_pg_conn(self) -> _StubConn:
        return self.conn


def _route(
    *,
    provider: str = "openrouter",
    model: str = "deepseek/deepseek-v4-pro",
    transport: str = "API",
    permitted: bool = True,
    rank: int = 1,
) -> dict[str, Any]:
    return {
        "task_type": "chat",
        "sub_task_type": "*",
        "provider_slug": provider,
        "model_slug": model,
        "transport_type": transport,
        "rank": rank,
        "permitted": permitted,
        "route_health_score": 0.9,
        "benchmark_score": None,
        "route_tier": "high",
        "latency_class": "interactive",
        "cost_per_m_tokens": None,
    }


def test_api_transport_resolves_to_control_plane_api() -> None:
    resolution = resolve_target_for_transport(transport_type="API")

    assert resolution.execution_target_ref == CONTROL_PLANE_API_TARGET
    assert resolution.execution_target_kind == "control_plane_api"
    assert resolution.packaging_kind == "none"
    assert resolution.sandbox_provider == "control_plane"


def test_cli_transport_resolves_to_docker_thin_cli() -> None:
    resolution = resolve_target_for_transport(transport_type="CLI")

    assert resolution.execution_target_ref == DOCKER_THIN_CLI_TARGET
    assert resolution.execution_target_kind == "docker_thin_cli"
    assert resolution.sandbox_provider == "docker_local"


def test_explicit_unavailable_target_fails_closed() -> None:
    result = handle_query_execution_targets_resolve(
        QueryExecutionTargetsResolve(
            transport_type="PROCESS",
            explicit_target_ref=PROCESS_SANDBOX_TARGET,
        ),
        _StubSubsystems([]),
    )

    assert result["ok"] is False
    assert result["error_code"] == "execution_target_resolution.rejected"


def test_fallback_only_happens_when_profile_declares_it() -> None:
    rejected = handle_query_execution_targets_resolve(
        QueryExecutionTargetsResolve(
            transport_type="API",
            explicit_profile_ref="execution_profile.unknown",
            fallback_allowed=False,
        ),
        _StubSubsystems([]),
    )
    fallback = handle_query_execution_targets_resolve(
        QueryExecutionTargetsResolve(
            transport_type="API",
            explicit_profile_ref="execution_profile.unknown",
            fallback_allowed=True,
        ),
        _StubSubsystems([]),
    )

    assert rejected["ok"] is False
    assert fallback["ok"] is True
    assert fallback["resolution"]["execution_target_ref"] == CONTROL_PLANE_API_TARGET
    assert fallback["resolution"]["target_resolution_reason"] == "fallback_after_explicit_rejected"


def test_openrouter_candidate_is_api_only_control_plane() -> None:
    candidate = enrich_dispatch_candidate(_route())

    assert candidate["provider_slug"] == "openrouter"
    assert candidate["transport_type"] == "API"
    assert candidate["execution_target_ref"] == CONTROL_PLANE_API_TARGET
    assert candidate["sandbox_provider"] == "control_plane"


def test_dispatch_choice_hash_must_match_candidate_set() -> None:
    result = handle_commit_dispatch_choice(
        CommitDispatchChoiceCommand(
            candidate_set_hash="wrong",
            selected_provider_slug="openrouter",
            selected_model_slug="deepseek/deepseek-v4-pro",
            selected_transport_type="API",
        ),
        _StubSubsystems([_route()]),
    )

    assert result["ok"] is False
    assert result["error_code"] == "dispatch_choice.candidate_set_hash_mismatch"


def test_dispatch_choice_rejects_disabled_candidate() -> None:
    rows = [_route(permitted=False)]
    candidate = enrich_dispatch_candidate(rows[0])
    digest = candidate_set_hash([candidate])

    result = handle_commit_dispatch_choice(
        CommitDispatchChoiceCommand(
            candidate_set_hash=digest,
            selected_candidate_ref=candidate["candidate_ref"],
        ),
        _StubSubsystems(rows),
    )

    assert result["ok"] is False
    assert result["error_code"] == "dispatch_choice.selected_candidate_disabled"


def test_dispatch_choice_records_selected_candidate() -> None:
    rows = [_route()]
    candidate = enrich_dispatch_candidate(rows[0])
    digest = candidate_set_hash([candidate])
    subsystems = _StubSubsystems(rows)

    result = handle_commit_dispatch_choice(
        CommitDispatchChoiceCommand(
            candidate_set_hash=digest,
            selected_candidate_ref=candidate["candidate_ref"],
            selected_by="operator",
            surface="unit_test",
        ),
        subsystems,
    )

    assert result["ok"] is True
    assert result["selected_candidate_ref"] == candidate["candidate_ref"]
    assert result["selected_target_ref"] == CONTROL_PLANE_API_TARGET
    assert len(subsystems.conn.inserts) == 1


def test_execution_proof_can_prove_dispatch_choice_governed_chat_message() -> None:
    class _ProofConn:
        def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM execution_dispatch_choices" in sql:
                return [
                    {
                        "dispatch_choice_ref": args[0],
                        "candidate_set_hash": "hash-1",
                        "selected_candidate_ref": "dispatch_option.chat.api.openrouter.deepseek",
                        "selected_target_ref": CONTROL_PLANE_API_TARGET,
                        "selected_profile_ref": "execution_profile.praxis.control_plane_api",
                        "selected_provider_slug": "openrouter",
                        "selected_model_slug": "deepseek/deepseek-v4-pro",
                        "selected_transport_type": "API",
                        "selection_kind": "explicit_click",
                        "selected_by": "operator",
                        "surface": "unit_test",
                        "conversation_id": "conv-1",
                        "selected_candidate_json": {},
                        "ask_all_candidates_json": [],
                        "selected_at": "2026-05-01T00:00:00+00:00",
                    }
                ]
            if "FROM conversation_messages" in sql:
                return [
                    {
                        "id": "msg-1",
                        "conversation_id": args[0],
                        "role": "assistant",
                        "model_used": "openrouter/deepseek/deepseek-v4-pro",
                        "latency_ms": 42,
                        "cost_usd": 0,
                        "created_at": "2026-05-01T00:00:01+00:00",
                    }
                ]
            return []

    class _ProofSubsystems:
        def get_pg_conn(self) -> _ProofConn:
            return _ProofConn()

    result = handle_query_execution_proof(
        QueryExecutionProof(dispatch_choice_ref="dispatch_choice.test", include_trace=False),
        _ProofSubsystems(),
    )

    assert result["verdict"] == "dispatch_choice_governed_run"
    assert result["selected_target_ref"] == CONTROL_PLANE_API_TARGET
    assert result["missing_evidence"] == []
