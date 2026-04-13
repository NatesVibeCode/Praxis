from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from registry.provider_routing import ProviderRouteAuthority
from runtime.work_item_workflow_bindings import WorkItemWorkflowBindingRecord
from surfaces.api import native_operator_surface


async def _fake_load_persona_activation(self, *, env, run_id, as_of):
    del self, env
    return {
        "kind": "native_operator_persona_activation",
        "authority": "test.stub",
        "selector_authority": "test.stub",
        "selector": {
            "run_id": run_id,
            "binding_scope": "native_runtime",
            "workspace_ref": "workspace.test",
            "runtime_profile_ref": "runtime_profile.test",
            "as_of": as_of.isoformat(),
        },
        "persona_profile": None,
        "persona_context_bindings": [],
    }


def test_native_operator_surface_route_loader_uses_explicit_as_of_bound_snapshot(monkeypatch) -> None:
    as_of = datetime(2026, 4, 2, 21, 20, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    connect_calls: list[dict[str, object]] = []
    snapshot_calls: list[dict[str, object]] = []
    close_calls: list[str] = []

    class _FakeConnection:
        async def close(self) -> None:
            close_calls.append("closed")

    fake_connection = _FakeConnection()

    async def _fake_connect_database(source_env):
        connect_calls.append({"env": dict(source_env or {})})
        return fake_connection

    async def _fake_load_provider_route_authority_snapshot(
        conn,
        *,
        as_of,
        model_profile_ids=None,
        provider_policy_ids=None,
        candidate_refs=None,
    ):
        snapshot_calls.append(
            {
                "conn": conn,
                "as_of": as_of,
                "model_profile_ids": model_profile_ids,
                "provider_policy_ids": provider_policy_ids,
                "candidate_refs": candidate_refs,
            }
        )
        return {"route_rows": 1, "snapshot": True}

    frontdoor = native_operator_surface.NativeOperatorSurfaceFrontdoor(
        connect_database=_fake_connect_database
    )
    monkeypatch.setattr(
        native_operator_surface,
        "load_provider_route_authority_snapshot",
        _fake_load_provider_route_authority_snapshot,
    )

    route_authority = asyncio.run(frontdoor._load_route_authority(env=env, as_of=as_of))

    assert route_authority == {"route_rows": 1, "snapshot": True}
    assert connect_calls == [{"env": env}]
    assert snapshot_calls == [
        {
            "conn": fake_connection,
            "as_of": as_of,
            "model_profile_ids": None,
            "provider_policy_ids": None,
            "candidate_refs": None,
        }
    ]
    assert close_calls == ["closed"]


def test_native_operator_surface_refuses_ambiguous_dispatch_selection(monkeypatch) -> None:
    as_of = datetime(2026, 4, 2, 21, 30, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    route_calls: list[dict[str, object]] = []
    cutover_calls: list[dict[str, object]] = []
    cockpit_calls: list[dict[str, object]] = []

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    async def _fake_load_route_authority(self, *, env, as_of):
        route_calls.append({"env": dict(env or {}), "as_of": as_of})
        return {"route_rows": 1}

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        cutover_calls.append(
            {
                "env": dict(env or {}),
                "run_id": run_id,
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return {"cutover_rows": 1}

    def _fake_operator_cockpit_run(
        *,
        run_id,
        as_of,
        route_authority,
        dispatch_resolution,
        cutover_status,
    ):
        cockpit_calls.append(
            {
                "run_id": run_id,
                "as_of": as_of,
                "route_authority": route_authority,
                "dispatch_resolution": dispatch_resolution,
                "cutover_status": cutover_status,
            }
        )
        return {"kind": "operator_cockpit"}

    def _fake_query_operator_surface(
        *,
        env=None,
        as_of=None,
        bug_ids=None,
        roadmap_item_ids=None,
        cutover_gate_ids=None,
        work_item_workflow_binding_ids=None,
        workflow_run_ids=None,
    ) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": list(work_item_workflow_binding_ids or []),
                "workflow_run_ids": list(workflow_run_ids or []),
            },
            "counts": {
                "bugs": 0,
                "roadmap_items": 0,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 2,
            },
            "bugs": [],
            "roadmap_items": [],
            "cutover_gates": [],
            "work_item_workflow_bindings": [
                {
                    "work_item_workflow_binding_id": "binding.1",
                    "binding_kind": "governed_by",
                    "binding_status": "active",
                    "source": {"kind": "cutover_gate", "id": "gate.1", "cutover_gate_id": "gate.1"},
                    "targets": {
                        "workflow_class_id": "dispatch.1",
                        "workflow_run_id": "run.1",
                    },
                    "bound_by_decision_id": "decision.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                },
                {
                    "work_item_workflow_binding_id": "binding.2",
                    "binding_kind": "governed_by",
                    "binding_status": "active",
                    "source": {"kind": "cutover_gate", "id": "gate.2", "cutover_gate_id": "gate.2"},
                    "targets": {
                        "workflow_class_id": "dispatch.2",
                        "workflow_run_id": "run.1",
                    },
                    "bound_by_decision_id": "decision.2",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                },
            ],
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return (
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.1",
                binding_kind="governed_by",
                binding_status="active",
                roadmap_item_id=None,
                bug_id=None,
                cutover_gate_id="gate.1",
                workflow_class_id="dispatch.1",
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision.1",
                created_at=as_of,
                updated_at=as_of,
            ),
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.2",
                binding_kind="governed_by",
                binding_status="active",
                roadmap_item_id=None,
                bug_id=None,
                cutover_gate_id="gate.2",
                workflow_class_id="dispatch.2",
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision.2",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_route_authority",
        _fake_load_route_authority,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_cutover_status",
        _fake_load_cutover_status,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "operator_cockpit_run",
        _fake_operator_cockpit_run,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "resolve_native_instance",
        lambda env=None: _FakeInstance(),
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )

    with pytest.raises(native_operator_surface.NativeOperatorSurfaceError) as exc_info:
        native_operator_surface.query_native_operator_surface(
            run_id="run.1",
            env=env,
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_operator_surface.workflow_class_ambiguous"
    assert route_calls == [{"env": env, "as_of": as_of}]
    assert cutover_calls[0]["env"] == env
    assert cutover_calls[0]["run_id"] == "run.1"
    assert cutover_calls[0]["as_of"] == as_of
    assert len(cutover_calls[0]["work_bindings"]) == 2
    assert all(binding.workflow_run_id == "run.1" for binding in cutover_calls[0]["work_bindings"])
    assert cockpit_calls == []


def test_native_operator_surface_fails_closed_when_route_authority_is_missing(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 2, 21, 35, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    route_calls: list[dict[str, object]] = []
    dispatch_calls: list[dict[str, object]] = []
    cutover_calls: list[dict[str, object]] = []

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    async def _fake_load_route_authority(self, *, env, as_of):
        route_calls.append({"env": dict(env or {}), "as_of": as_of})
        return None

    async def _fake_load_dispatch_resolution(self, *, env, as_of, work_bindings):
        dispatch_calls.append(
            {
                "env": dict(env or {}),
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return {"dispatch_rows": 1}

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        cutover_calls.append(
            {
                "env": dict(env or {}),
                "run_id": run_id,
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return {"cutover_rows": 1}

    def _fake_query_operator_surface(
        *,
        env=None,
        as_of=None,
        bug_ids=None,
        roadmap_item_ids=None,
        cutover_gate_ids=None,
        work_item_workflow_binding_ids=None,
        workflow_run_ids=None,
    ) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": bug_ids,
                "roadmap_item_ids": roadmap_item_ids,
                "cutover_gate_ids": cutover_gate_ids,
                "work_item_workflow_binding_ids": list(work_item_workflow_binding_ids or []),
                "workflow_run_ids": list(workflow_run_ids or []),
            },
            "counts": {
                "bugs": 0,
                "roadmap_items": 0,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 1,
            },
            "bugs": [],
            "roadmap_items": [],
            "cutover_gates": [],
            "work_item_workflow_bindings": [
                {
                    "work_item_workflow_binding_id": "binding.1",
                    "binding_kind": "governed_by",
                    "binding_status": "active",
                    "source": {"kind": "cutover_gate", "id": "gate.1"},
                    "targets": {
                        "workflow_class_id": "dispatch.1",
                        "workflow_run_id": "run.1",
                    },
                    "bound_by_decision_id": "decision.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return (
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.1",
                binding_kind="governed_by",
                binding_status="active",
                roadmap_item_id=None,
                bug_id=None,
                cutover_gate_id="gate.1",
                workflow_class_id="dispatch.1",
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision.1",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_route_authority",
        _fake_load_route_authority,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_dispatch_resolution",
        _fake_load_dispatch_resolution,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_cutover_status",
        _fake_load_cutover_status,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "resolve_native_instance",
        lambda env=None: _FakeInstance(),
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )

    with pytest.raises(native_operator_surface.NativeOperatorSurfaceError) as exc_info:
        native_operator_surface.query_native_operator_surface(
            run_id="run.1",
            env=env,
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_operator_surface.route_authority_missing"
    assert exc_info.value.details["cockpit_reason_code"] == (
        "operator_cockpit.route_authority_missing"
    )
    assert route_calls == [{"env": env, "as_of": as_of}]
    assert dispatch_calls == [
        {
            "env": env,
            "as_of": as_of,
            "work_bindings": (
                WorkItemWorkflowBindingRecord(
                    work_item_workflow_binding_id="binding.1",
                    binding_kind="governed_by",
                    binding_status="active",
                    roadmap_item_id=None,
                    bug_id=None,
                    cutover_gate_id="gate.1",
                    workflow_class_id="dispatch.1",
                    schedule_definition_id=None,
                    workflow_run_id="run.1",
                    bound_by_decision_id="decision.1",
                    created_at=as_of,
                    updated_at=as_of,
                ),
            ),
        }
    ]
    assert cutover_calls == [
        {
            "env": env,
            "run_id": "run.1",
            "as_of": as_of,
            "work_bindings": (
                WorkItemWorkflowBindingRecord(
                    work_item_workflow_binding_id="binding.1",
                    binding_kind="governed_by",
                    binding_status="active",
                    roadmap_item_id=None,
                    bug_id=None,
                    cutover_gate_id="gate.1",
                    workflow_class_id="dispatch.1",
                    schedule_definition_id=None,
                    workflow_run_id="run.1",
                    bound_by_decision_id="decision.1",
                    created_at=as_of,
                    updated_at=as_of,
                ),
            ),
        }
    ]


def test_native_operator_surface_fails_closed_when_cutover_truth_is_missing(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 2, 21, 40, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    route_calls: list[dict[str, object]] = []
    dispatch_calls: list[dict[str, object]] = []
    cutover_calls: list[dict[str, object]] = []

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    async def _fake_load_route_authority(self, *, env, as_of):
        route_calls.append({"env": dict(env or {}), "as_of": as_of})
        return ProviderRouteAuthority(
            provider_route_health_windows={},
            provider_budget_windows={},
            route_eligibility_states={},
        )

    async def _fake_load_dispatch_resolution(self, *, env, as_of, work_bindings):
        dispatch_calls.append(
            {
                "env": dict(env or {}),
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return {"dispatch_rows": 1}

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        cutover_calls.append(
            {
                "env": dict(env or {}),
                "run_id": run_id,
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return None

    def _fake_query_operator_surface(
        *,
        env=None,
        as_of=None,
        bug_ids=None,
        roadmap_item_ids=None,
        cutover_gate_ids=None,
        work_item_workflow_binding_ids=None,
        workflow_run_ids=None,
    ) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": bug_ids,
                "roadmap_item_ids": roadmap_item_ids,
                "cutover_gate_ids": cutover_gate_ids,
                "work_item_workflow_binding_ids": list(work_item_workflow_binding_ids or []),
                "workflow_run_ids": list(workflow_run_ids or []),
            },
            "counts": {
                "bugs": 0,
                "roadmap_items": 0,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 1,
            },
            "bugs": [],
            "roadmap_items": [],
            "cutover_gates": [],
            "work_item_workflow_bindings": [
                {
                    "work_item_workflow_binding_id": "binding.1",
                    "binding_kind": "governed_by",
                    "binding_status": "active",
                    "source": {"kind": "cutover_gate", "id": "gate.1"},
                    "targets": {
                        "workflow_class_id": "dispatch.1",
                        "workflow_run_id": "run.1",
                    },
                    "bound_by_decision_id": "decision.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return (
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.1",
                binding_kind="governed_by",
                binding_status="active",
                roadmap_item_id=None,
                bug_id=None,
                cutover_gate_id="gate.1",
                workflow_class_id="dispatch.1",
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision.1",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_route_authority",
        _fake_load_route_authority,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_dispatch_resolution",
        _fake_load_dispatch_resolution,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_cutover_status",
        _fake_load_cutover_status,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "resolve_native_instance",
        lambda env=None: _FakeInstance(),
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )

    with pytest.raises(native_operator_surface.NativeOperatorSurfaceError) as exc_info:
        native_operator_surface.query_native_operator_surface(
            run_id="run.1",
            env=env,
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_operator_surface.cutover_status_missing"
    assert exc_info.value.details["cockpit_reason_code"] == (
        "operator_cockpit.cutover_status_missing"
    )
    assert route_calls == [{"env": env, "as_of": as_of}]
    assert len(dispatch_calls) == 1
    assert len(dispatch_calls[0]["work_bindings"]) == 1
    assert dispatch_calls[0]["work_bindings"][0].workflow_run_id == "run.1"
    assert cutover_calls == [
        {
            "env": env,
            "run_id": "run.1",
            "as_of": as_of,
            "work_bindings": (
                WorkItemWorkflowBindingRecord(
                    work_item_workflow_binding_id="binding.1",
                    binding_kind="governed_by",
                    binding_status="active",
                    roadmap_item_id=None,
                    bug_id=None,
                    cutover_gate_id="gate.1",
                    workflow_class_id="dispatch.1",
                    schedule_definition_id=None,
                    workflow_run_id="run.1",
                    bound_by_decision_id="decision.1",
                    created_at=as_of,
                    updated_at=as_of,
                ),
            ),
        }
    ]
