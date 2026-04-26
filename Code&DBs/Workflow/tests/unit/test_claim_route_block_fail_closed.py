"""Regression pin for BUG-32194458.

``_select_claim_route`` must fail closed when every candidate in a failover
chain is explicitly blocked (``permitted=false``) by ``task_type_routing`` —
previously it silently degraded to the original chain and returned a forbidden
route, which made the policy block advisory. Now it raises
``ClaimRouteBlockedError`` and the claim path converts that into a failed ready
job via ``_fail_unclaimable_ready_job``.
"""
from __future__ import annotations

import pytest

from runtime.workflow._routing import (
    ClaimRouteBlockedError,
    _select_claim_route,
)
from runtime.workflow import _claiming as _claiming_mod


class _ClaimConn:
    """FakeConn that returns a two-model chain with both rows marked blocked."""

    def __init__(
        self,
        *,
        blocked_slugs: tuple[str, ...],
        route_task_type: str = "build",
    ) -> None:
        self._blocked_slugs = set(blocked_slugs)
        self._route_task_type = route_task_type

    def execute(self, query: str, *args):
        if "FROM provider_model_candidates" in query:
            return [
                {
                    "candidate_ref": "cand-openai",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "priority": 1,
                },
                {
                    "candidate_ref": "cand-anthropic",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "priority": 2,
                },
            ]
        if "FROM workflow_runs" in query:
            return [{"runtime_profile_ref": "nate-private"}]
        if "FROM effective_private_provider_job_catalog" in query:
            return [
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": self._route_task_type,
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "model_version": "gpt-5.4",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {},
                    "reason_code": "catalog.available",
                    "candidate_ref": "cand-openai",
                    "provider_ref": "provider.openai",
                    "source_refs": [],
                    "projected_at": None,
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": self._route_task_type,
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "model_version": "claude-sonnet-4-6",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {},
                    "reason_code": "catalog.available",
                    "candidate_ref": "cand-anthropic",
                    "provider_ref": "provider.anthropic",
                    "source_refs": [],
                    "projected_at": None,
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
            ]
        if "FROM provider_transport_gate_denials" in query:
            return []
        if "FROM registry_runtime_profile_authority" in query:
            return []
        if "GROUP BY 1" in query:
            return []
        if "FROM task_type_routing" in query:
            # The route_meta query (permitted=true) returns nothing.
            if "permitted = true" in query:
                return []
            # The _blocked_candidates_for_task query (permitted=false) returns
            # our blocked set — assembled by provider/model split.
            if "permitted = false" in query:
                rows = []
                for slug in self._blocked_slugs:
                    provider_slug, model_slug = slug.split("/", 1)
                    rows.append({"slug": f"{provider_slug}/{model_slug}"})
                return rows
        raise AssertionError(f"unexpected query: {query!r}")


def _job() -> dict:
    return {
        "run_id": "run-1",
        "agent_slug": "openai/gpt-5.4",
        "failover_chain": ["openai/gpt-5.4", "anthropic/claude-sonnet-4-6"],
        "route_task_type": "build",
    }


def test_select_claim_route_raises_when_all_candidates_blocked() -> None:
    """Both candidates blocked → must raise, not return either slug."""
    conn = _ClaimConn(
        blocked_slugs=("openai/gpt-5.4", "anthropic/claude-sonnet-4-6"),
    )

    with pytest.raises(ClaimRouteBlockedError) as excinfo:
        _select_claim_route(conn, _job())

    err = excinfo.value
    assert err.reason_code == "routing.all_candidates_blocked"
    assert err.task_type == "build"
    # Both candidates must appear in the exception's blocked list so the
    # upstream failure record can name them.
    assert set(err.blocked_candidates) == {
        "openai/gpt-5.4",
        "anthropic/claude-sonnet-4-6",
    }
    # The policy-closure reason must be in the message so the failure
    # reason that surfaces upstream names the bug it closes.
    assert "BUG-32194458" in str(err)


def test_select_claim_route_uses_unblocked_candidate_when_one_remains() -> None:
    """One blocked, one clear → must pick the clear candidate (legacy path)."""
    conn = _ClaimConn(blocked_slugs=("openai/gpt-5.4",))

    selected = _select_claim_route(conn, _job())

    assert selected == "anthropic/claude-sonnet-4-6"


def test_select_claim_route_surfaces_control_panel_transport_denial() -> None:
    """When the catalog blocks a transport at the control panel, the upstream
    error must carry the operator message instead of a stale model-specific
    explanation.
    """

    class _ControlPanelConn(_ClaimConn):
        def execute(self, query: str, *args):
            if "FROM effective_private_provider_job_catalog" in query:
                return []
            if "FROM provider_transport_gate_denials" in query:
                return [
                    {
                        "runtime_profile_ref": "nate-private",
                        "job_type": "build",
                        "transport_type": "API",
                        "adapter_type": "llm_task",
                        "provider_slug": "together",
                        "model_slug": "deepseek-ai/DeepSeek-V4-Pro",
                        "reason_code": "control_panel.transport_turned_off",
                        "source_refs": [
                            "table.private_provider_transport_control_policy"
                        ],
                        "default_posture": "deny_unless_allowlisted",
                        "operator_message": (
                            "this Model Access method has been turned off on purpose "
                            "at the control panel either for this specific task type, "
                            "or more broadly, consult the control panel and do not "
                            "turn it on without confirming with the user even if you "
                            "think that will help you complete your task."
                        ),
                        "decision_ref": "decision.private-api-control-panel",
                    }
                ]
            return super().execute(query, *args)

    conn = _ControlPanelConn(blocked_slugs=())

    with pytest.raises(ClaimRouteBlockedError) as excinfo:
        _select_claim_route(conn, _job())

    err = excinfo.value
    assert err.reason_code == "control_panel.transport_turned_off"
    assert str(err) == (
        "this Model Access method has been turned off on purpose at the control "
        "panel either for this specific task type, or more broadly, consult the "
        "control panel and do not turn it on without confirming with the user "
        "even if you think that will help you complete your task."
    )
    assert err.details["transport_type"] == "API"
    assert err.details["decision_ref"] == "decision.private-api-control-panel"


def test_claim_one_fails_closed_on_all_blocked(monkeypatch) -> None:
    """``claim_one`` must call _fail_unclaimable_ready_job for an all-blocked
    chain, tag the failure with ``routing_blocked`` (not the authority-missing
    category reserved for missing runtime-profile authority), and block
    descendants under the same stable reason code.
    """

    class _QuarantineConn:
        def execute(self, query: str, *args):
            if "FROM workflow_jobs j" in query and "r.requested_at DESC" in query:
                return [
                    {
                        "id": 42,
                        "run_id": "run-quarantine",
                        "label": "phase_blocked",
                        "status": "ready",
                        "agent_slug": "openai/gpt-5.4",
                        "failover_chain": [
                            "openai/gpt-5.4",
                            "anthropic/claude-sonnet-4-6",
                        ],
                        "route_task_type": "build",
                    }
                ]
            if "SET status = 'failed'" in query and "WHERE id = $1" in query:
                # Capture the update parameters for assertion.
                _QuarantineConn.last_update_args = args
                return [
                    {
                        "id": 42,
                        "run_id": "run-quarantine",
                        "route_task_type": "build",
                        "effective_agent": "openai/gpt-5.4",
                    }
                ]
            raise AssertionError(f"unexpected query: {query!r}")

    _QuarantineConn.last_update_args = ()

    blocked_descendants: list[tuple[int, str]] = []
    recomputed: list[str] = []
    outcomes: list[dict] = []

    monkeypatch.setattr(_claiming_mod, "_job_has_touch_conflict", lambda _conn, _job: False)
    monkeypatch.setattr(
        _claiming_mod,
        "_select_claim_route",
        lambda _conn, _job: (_ for _ in ()).throw(
            ClaimRouteBlockedError(
                "routing.all_candidates_blocked",
                "all candidates are blocked for task_type='build'",
                blocked_candidates=("openai/gpt-5.4", "anthropic/claude-sonnet-4-6"),
                task_type="build",
            )
        ),
    )
    monkeypatch.setattr(
        _claiming_mod,
        "_block_descendants",
        lambda _conn, job_id, code: blocked_descendants.append((job_id, code)),
    )
    monkeypatch.setattr(
        _claiming_mod,
        "_recompute_workflow_run_state",
        lambda _conn, run_id: recomputed.append(run_id),
    )
    monkeypatch.setattr(
        _claiming_mod,
        "_record_task_route_outcome",
        lambda _conn, **kwargs: outcomes.append(kwargs),
    )

    claimed = _claiming_mod.claim_one(_QuarantineConn(), "worker-1")

    assert claimed is None
    # Descendants blocked under the stable all-blocked reason code.
    assert blocked_descendants == [(42, "routing.all_candidates_blocked")]
    # Workflow run state recomputed so the failure is surfaced.
    assert recomputed == ["run-quarantine"]
    # Route outcome reporting fired with the stable reason code so task-type
    # routing feedback tracks the denial instead of losing it.
    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome["succeeded"] is False
    assert outcome["failure_code"] == "routing.all_candidates_blocked"
    assert outcome["failure_category"] == "routing_blocked"
    # The UPDATE args are (job_id, error_code, stdout_preview, failure_category, failure_zone).
    update_args = _QuarantineConn.last_update_args
    assert update_args[0] == 42
    assert update_args[1] == "routing.all_candidates_blocked"
    assert update_args[3] == "routing_blocked"
    assert update_args[4] == "routing"


def test_claim_route_blocked_error_carries_reason_code_shape() -> None:
    """Contract: error matches RuntimeProfileAdmissionError so one except clause
    in claim_one handles both — a sibling permit-denial and an authority gap.
    """
    err = ClaimRouteBlockedError(
        "routing.all_candidates_blocked",
        "msg",
        blocked_candidates=("a/b",),
        task_type="build",
    )
    assert isinstance(err, RuntimeError)
    assert err.reason_code == "routing.all_candidates_blocked"
    assert err.blocked_candidates == ("a/b",)
    assert err.task_type == "build"
