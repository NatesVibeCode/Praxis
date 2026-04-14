from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

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


def test_native_operator_surface_scopes_run_context_and_database_handle_per_request(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    connect_calls: list[dict[str, object]] = []
    close_calls: list[str] = []
    workflow_run_context_reads: list[str] = []
    binding_loader_conns: list[object] = []
    persona_repository_conns: list[object] = []
    persona_selectors: list[object] = []
    fork_selectors: list[object] = []

    class _FakeConnection:
        async def fetchrow(self, query: str, *args: object):
            if "FROM workflow_runs" in query:
                workflow_run_context_reads.append(args[0])
                return {
                    "workspace_ref": "workspace.test",
                    "runtime_profile_ref": "runtime_profile.test",
                }
            if "FROM workflow_claim_lease_proposal_runtime" in query:
                return {
                    "share_mode": "shared",
                    "reuse_reason_code": "packet.authoritative_fork",
                    "sandbox_session_id": "sandbox.alpha",
                }
            raise AssertionError(f"unexpected fetchrow query: {query}")

        async def fetch(self, query: str, *args: object):
            if "FROM fork_worktree_bindings" in query:
                return [
                    {
                        "fork_worktree_binding_id": "binding.alpha",
                        "binding_scope": "native_runtime",
                        "workspace_ref": "workspace.test",
                        "runtime_profile_ref": "runtime_profile.test",
                        "fork_ref": "fork.alpha",
                        "worktree_ref": "worktree.alpha",
                    }
                ]
            raise AssertionError(f"unexpected fetch query: {query}")

        async def close(self) -> None:
            close_calls.append("closed")

    fake_connection = _FakeConnection()

    async def _fake_connect_database(source_env):
        connect_calls.append({"env": dict(source_env or {})})
        return fake_connection

    async def _fake_load_run_scoped_work_bindings(conn, *, workflow_run_id):
        binding_loader_conns.append(conn)
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
                workflow_run_id=workflow_run_id,
                bound_by_decision_id="decision.1",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    class _FakeAuthorityRepository:
        def __init__(self, conn) -> None:
            persona_repository_conns.append(conn)

        async def load_persona_activation(self, *, selector):
            persona_selectors.append(selector)
            return (
                {
                    "persona_profile_id": "persona.alpha",
                    "persona_name": "Operator",
                    "persona_kind": "native_operator",
                    "instruction_contract": {"kind": "instruction_contract"},
                    "effective_from": as_of,
                    "effective_to": None,
                    "decision_ref": "decision.alpha",
                    "created_at": as_of,
                },
                [],
            )

        async def load_fork_worktree_binding(self, *, selector):
            fork_selectors.append(selector)
            return {
                "fork_worktree_binding_id": "binding.alpha",
                "fork_profile_id": "fork_profile.alpha",
                "sandbox_session_id": "sandbox.alpha",
                "workflow_run_id": "run.1",
                "binding_scope": "native_runtime",
                "binding_status": "active",
                "workspace_ref": "workspace.test",
                "runtime_profile_ref": "runtime_profile.test",
                "base_ref": "base.alpha",
                "fork_ref": "fork.alpha",
                "worktree_ref": "worktree.alpha",
                "created_at": as_of,
                "retired_at": None,
                "decision_ref": "decision.alpha",
            }

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    class _FakeCockpit:
        def to_json(self) -> dict[str, object]:
            return {"kind": "operator_cockpit", "status_state": "fresh"}

    async def _fake_load_route_authority(self, *, env, as_of):
        del self, env, as_of
        return {"route_rows": 1}

    async def _fake_load_dispatch_resolution(self, *, env, as_of, work_bindings):
        del self, env, as_of, work_bindings
        return {"dispatch_rows": 1}

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        del self, env, run_id, as_of, work_bindings
        return {"cutover_rows": 1}

    async def _fake_load_smoke_freshness(self, *, env, as_of):
        del self, env
        return {
            "kind": "native_smoke_freshness",
            "state": "fresh",
            "last_run_id": "run:workflow.native-self-hosted-smoke:latest",
            "last_requested_at": as_of.isoformat(),
            "age_seconds": 0.0,
            "fail_streak": 0,
            "latest_failure_category": "success",
        }

    async def _fake_load_canonical_evidence(self, *, env, run_id):
        del self, env, run_id
        return ()

    def _fake_operator_cockpit_run(
        *,
        run_id,
        as_of,
        route_authority,
        dispatch_resolution,
        cutover_status,
    ) -> _FakeCockpit:
        del run_id, as_of, route_authority, dispatch_resolution, cutover_status
        return _FakeCockpit()

    def _fake_frontdoor_status(*, run_id: str, env=None) -> dict[str, object]:
        del env
        return {
            "native_instance": {"kind": "native_instance", "label": "shared"},
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.alpha",
                "request_id": "request.alpha",
                "request_digest": "digest.alpha",
                "workflow_definition_id": "workflow_definition.alpha.v1",
                "admitted_definition_hash": "sha256:alpha",
                "current_state": "running",
                "terminal_reason_code": None,
                "run_idempotency_key": "idem.alpha",
                "context_bundle_id": "context.alpha",
                "authority_context_digest": "digest.alpha",
                "admission_decision_id": "admission.alpha",
                "requested_at": as_of.isoformat(),
                "admitted_at": as_of.isoformat(),
                "started_at": as_of.isoformat(),
                "finished_at": None,
                "last_event_id": "event.alpha.2",
            },
            "inspection": {"kind": "workflow_inspection", "last_evidence_seq": 0},
            "observability": {
                "kind": "frontdoor_observability",
                "health_state": "healthy",
                "run_identity": {"request_digest": "digest.alpha"},
            },
        }

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
        del env, bug_ids, roadmap_item_ids, cutover_gate_ids
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
                    "source": {"kind": "cutover_gate", "id": "gate.1", "cutover_gate_id": "gate.1"},
                    "targets": {
                        "workflow_class_id": "dispatch.1",
                        "workflow_run_id": "run.1",
                    },
                    "bound_by_decision_id": "decision.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "workflow_run_observability": {
                "kind": "workflow_run_observability",
                "observability_digest": "1 runs | 100.0% packet coverage | dominant failure in_progress | 0 synthetic | 0 isolated",
                "workflow_run_count": 1,
                "packet_inspection_source_counts": {"materialized": 1},
                "packet_inspection_coverage_rate": 1.0,
                "failure_category_counts": {"in_progress": 1},
                "dominant_failure_category": "in_progress",
                "synthetic_run_count": 0,
                "isolated_run_count": 0,
                "missing_workflow_run_ids": [],
                "contract_drift_refs": [],
            },
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface,
        "load_work_item_workflow_bindings_for_workflow_run",
        _fake_load_run_scoped_work_bindings,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "PostgresPersonaAndForkAuthorityRepository",
        _FakeAuthorityRepository,
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
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_smoke_freshness",
        _fake_load_smoke_freshness,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_canonical_evidence",
        _fake_load_canonical_evidence,
    )
    monkeypatch.setattr(native_operator_surface, "operator_cockpit_run", _fake_operator_cockpit_run)
    monkeypatch.setattr(native_operator_surface, "frontdoor_status", _fake_frontdoor_status)
    monkeypatch.setattr(
        native_operator_surface,
        "resolve_native_instance",
        lambda env=None: _FakeInstance(),
    )

    frontdoor = native_operator_surface.NativeOperatorSurfaceFrontdoor(
        connect_database=_fake_connect_database
    )

    payload = frontdoor.query_native_operator_surface(
        run_id="run.1",
        env=env,
        as_of=as_of,
    )

    assert payload["persona"]["selector"]["workspace_ref"] == "workspace.test"
    assert payload["persona"]["selector"]["runtime_profile_ref"] == "runtime_profile.test"
    assert payload["fork_ownership"]["selector"] == {
        "run_id": "run.1",
        "workspace_ref": "workspace.test",
        "runtime_profile_ref": "runtime_profile.test",
        "fork_ref": "fork.alpha",
        "worktree_ref": "worktree.alpha",
    }
    assert connect_calls == [{"env": env}]
    assert close_calls == ["closed"]
    assert workflow_run_context_reads == ["run.1"]
    assert binding_loader_conns == [fake_connection]
    assert persona_repository_conns == [fake_connection]
    assert len(persona_selectors) == 1
    assert persona_selectors[0].workspace_ref == "workspace.test"
    assert persona_selectors[0].runtime_profile_ref == "runtime_profile.test"
    assert len(fork_selectors) == 1
    assert fork_selectors[0].workspace_ref == "workspace.test"
    assert fork_selectors[0].runtime_profile_ref == "runtime_profile.test"
    assert fork_selectors[0].fork_ref == "fork.alpha"
    assert fork_selectors[0].worktree_ref == "worktree.alpha"


def test_native_operator_surface_consolidates_query_and_cockpit_truth(monkeypatch) -> None:
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    query_calls: list[dict[str, object]] = []
    route_calls: list[dict[str, object]] = []
    dispatch_calls: list[dict[str, object]] = []
    cutover_calls: list[dict[str, object]] = []
    cockpit_calls: list[dict[str, object]] = []
    status_calls: list[dict[str, object]] = []
    evidence_calls: list[dict[str, object]] = []

    @dataclass(frozen=True)
    class _FakeReceipt:
        receipt_id: str
        receipt_type: str
        run_id: str
        status: str
        node_id: str | None
        failure_code: str | None
        transition_seq: int
        evidence_seq: int
        started_at: datetime
        finished_at: datetime

    @dataclass(frozen=True)
    class _FakeEvidenceRow:
        kind: str
        record: object

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    class _FakeCockpit:
        def to_json(self) -> dict[str, object]:
            return {"kind": "operator_cockpit", "status_state": "fresh"}

    async def _fake_load_route_authority(self, *, env, as_of):
        route_calls.append({"env": dict(env or {}), "as_of": as_of})
        return {"route_rows": 1}

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

    def _fake_operator_cockpit_run(
        *,
        run_id,
        as_of,
        route_authority,
        dispatch_resolution,
        cutover_status,
    ) -> _FakeCockpit:
        cockpit_calls.append(
            {
                "run_id": run_id,
                "as_of": as_of,
                "route_authority": route_authority,
                "dispatch_resolution": dispatch_resolution,
                "cutover_status": cutover_status,
            }
        )
        return _FakeCockpit()

    def _fake_frontdoor_status(*, run_id: str, env=None) -> dict[str, object]:
        status_calls.append({"run_id": run_id, "env": dict(env or {})})
        return {
            "native_instance": {"kind": "native_instance", "label": "shared"},
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.alpha",
                "request_id": "request.alpha",
                "request_digest": "digest.alpha",
                "workflow_definition_id": "workflow_definition.alpha.v1",
                "admitted_definition_hash": "sha256:alpha",
                "current_state": "running",
                "terminal_reason_code": None,
                "run_idempotency_key": "idem.alpha",
                "context_bundle_id": "context.alpha",
                "authority_context_digest": "digest.alpha",
                "admission_decision_id": "admission.alpha",
                "requested_at": as_of.isoformat(),
                "admitted_at": as_of.isoformat(),
                "started_at": as_of.isoformat(),
                "finished_at": None,
                "last_event_id": "event.alpha.2",
            },
            "inspection": {"kind": "workflow_inspection", "last_evidence_seq": 2},
            "observability": {
                "kind": "frontdoor_observability",
                "health_state": "healthy",
                "run_identity": {
                    "request_digest": "digest.alpha",
                },
            },
        }

    async def _fake_load_smoke_freshness(self, *, env, as_of):
        del self, env
        return {
            "kind": "native_smoke_freshness",
            "state": "fresh",
            "last_run_id": "run:workflow.native-self-hosted-smoke:latest",
            "last_requested_at": as_of.isoformat(),
            "age_seconds": 12.0,
            "fail_streak": 0,
            "latest_failure_category": "in_progress",
        }

    async def _fake_load_canonical_evidence(self, *, env, run_id):
        evidence_calls.append({"env": dict(env or {}), "run_id": run_id})
        return (
            _FakeEvidenceRow(kind="workflow_event", record={"event_id": "event.alpha.1"}),
            _FakeEvidenceRow(
                kind="receipt",
                record=_FakeReceipt(
                    receipt_id="receipt.alpha.2",
                    receipt_type="workflow_completion_receipt",
                    run_id=run_id,
                    status="succeeded",
                    node_id="node.alpha",
                    failure_code=None,
                    transition_seq=1,
                    evidence_seq=2,
                    started_at=as_of,
                    finished_at=as_of,
                ),
            ),
        )

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
        query_calls.append(
            {
                "env": dict(env or {}),
                "as_of": as_of,
                "bug_ids": bug_ids,
                "roadmap_item_ids": roadmap_item_ids,
                "cutover_gate_ids": cutover_gate_ids,
                "work_item_workflow_binding_ids": work_item_workflow_binding_ids,
                "workflow_run_ids": workflow_run_ids,
            }
        )
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": ["binding.1"],
                "workflow_run_ids": ["run.1"],
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
                    "source": {"kind": "cutover_gate", "id": "gate.1", "cutover_gate_id": "gate.1"},
                    "targets": {
                        "workflow_class_id": "dispatch.1",
                        "workflow_run_id": "run.1",
                    },
                    "bound_by_decision_id": "decision.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "workflow_run_observability": {
                "kind": "workflow_run_observability",
                "observability_digest": "1 runs | 100.0% packet coverage | dominant failure in_progress | 0 synthetic | 0 isolated",
                "workflow_run_count": 1,
                "packet_inspection_source_counts": {"materialized": 1},
                "packet_inspection_coverage_rate": 1.0,
                "failure_category_counts": {"in_progress": 1},
                "dominant_failure_category": "in_progress",
                "synthetic_run_count": 0,
                "isolated_run_count": 0,
                "missing_workflow_run_ids": [],
                "contract_drift_refs": [],
            },
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
        "operator_cockpit_run",
        _fake_operator_cockpit_run,
    )
    monkeypatch.setattr(native_operator_surface, "frontdoor_status", _fake_frontdoor_status)
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
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_canonical_evidence",
        _fake_load_canonical_evidence,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_smoke_freshness",
        _fake_load_smoke_freshness,
    )

    payload = native_operator_surface.query_native_operator_surface(
        run_id="run.1",
        env=env,
        as_of=as_of,
    )

    assert payload["kind"] == "native_operator_surface"
    assert payload["instruction_authority"] == {
        "kind": "native_operator_instruction_authority",
        "authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
        "orient_authority": "surfaces.api.handlers.workflow_admin._handle_orient",
        "packet_read_order": [
            "roadmap_truth",
            "queue_refs",
            "current_state_notes",
            "status",
            "observability",
            "receipts",
            "query",
            "cockpit",
        ],
        "roadmap_truth": {
            "authority": "surfaces.api.operator_read.query_operator_surface",
            "roadmap_item_ids": [],
            "items": [],
        },
        "queue_refs": {
            "run_id": "run.1",
            "workflow_id": "workflow.alpha",
            "request_id": "request.alpha",
            "workflow_definition_id": "workflow_definition.alpha.v1",
            "context_bundle_id": "context.alpha",
            "work_item_workflow_binding_ids": ["binding.1"],
            "roadmap_item_ids": [],
        },
        "current_state_notes": [
            {
                "note_code": "workflow_run_state",
                "authority": "surfaces.api.frontdoor.status",
                "run_id": "run.1",
                "message": "workflow_runs.current_state=running",
            },
            {
                "note_code": "receipt_terminal_status",
                "authority": "storage.postgres.PostgresEvidenceReader.load_evidence_timeline",
                "run_id": "run.1",
                "message": "latest_receipt.status=succeeded",
            },
            {
                "note_code": "status_receipt_mismatch",
                "authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
                "run_id": "run.1",
                "message": (
                    "workflow_runs.current_state and latest receipt status disagree; "
                    "read both before inferring execution state"
                ),
            },
        ],
        "directive": (
            "Read roadmap-backed truth, queue refs, current-state notes, and observability here before "
            "using repo files or prior chat state."
        ),
    }
    assert payload["provenance"] == {
        "kind": "native_operator_surface_provenance",
        "as_of": as_of.isoformat(),
        "native_instance_authority": "runtime.instance.resolve_native_instance",
        "section_authorities": {
            "instruction_authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
            "native_instance": "runtime.instance.resolve_native_instance",
            "query": "surfaces.api.operator_read.query_operator_surface",
            "cockpit": "observability.operator_dashboard.operator_cockpit_run",
            "observability": "surfaces.api.native_operator_surface.query_native_operator_surface",
        },
        "stitched_sections": [
            "instruction_authority",
            "native_instance",
            "query",
            "cockpit",
            "observability",
        ],
    }
    assert payload["native_instance"] == {"kind": "native_instance", "label": "shared"}
    assert payload["run_id"] == "run.1"
    assert payload["as_of"] == as_of.isoformat()
    assert payload["status"] == {
        "kind": "native_operator_status_truth",
        "authority": "surfaces.api.frontdoor.status",
        "run": {
            "run_id": "run.1",
            "workflow_id": "workflow.alpha",
            "request_id": "request.alpha",
            "request_digest": "digest.alpha",
            "workflow_definition_id": "workflow_definition.alpha.v1",
            "admitted_definition_hash": "sha256:alpha",
            "current_state": "running",
            "terminal_reason_code": None,
            "run_idempotency_key": "idem.alpha",
            "context_bundle_id": "context.alpha",
            "authority_context_digest": "digest.alpha",
            "admission_decision_id": "admission.alpha",
            "requested_at": as_of.isoformat(),
            "admitted_at": as_of.isoformat(),
            "started_at": as_of.isoformat(),
            "finished_at": None,
            "last_event_id": "event.alpha.2",
        },
        "inspection": {"kind": "workflow_inspection", "last_evidence_seq": 2},
        "observability": {
            "kind": "frontdoor_observability",
            "health_state": "healthy",
            "run_identity": {"request_digest": "digest.alpha"},
        },
    }
    assert payload["receipts"] == {
        "kind": "native_operator_receipt_truth",
        "authority": "storage.postgres.PostgresEvidenceReader.load_evidence_timeline",
        "run_id": "run.1",
        "evidence_row_count": 2,
        "receipt_count": 1,
        "latest_evidence_seq": 2,
        "latest_receipt_id": "receipt.alpha.2",
        "latest_receipt_type": "workflow_completion_receipt",
        "terminal_status": "succeeded",
        "status_counts": {"succeeded": 1},
        "receipts": [
            {
                "receipt_id": "receipt.alpha.2",
                "receipt_type": "workflow_completion_receipt",
                "status": "succeeded",
                "node_id": "node.alpha",
                "failure_code": None,
                "transition_seq": 1,
                "evidence_seq": 2,
                "started_at": as_of.isoformat(),
                "finished_at": as_of.isoformat(),
            }
        ],
    }
    assert payload["query"]["kind"] == "operator_query"
    assert "native_instance" not in payload["query"]
    assert "as_of" not in payload["query"]
    assert payload["cockpit"] == {"kind": "operator_cockpit", "status_state": "fresh"}
    assert payload["observability"] == {
        "kind": "native_operator_observability",
        "authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
        "run": {
            "kind": "frontdoor_observability",
            "health_state": "healthy",
            "run_identity": {"request_digest": "digest.alpha"},
        },
        "workflow_runs": {
            "kind": "workflow_run_observability",
            "observability_digest": "1 runs | 100.0% packet coverage | dominant failure in_progress | 0 synthetic | 0 isolated",
            "workflow_run_count": 1,
            "packet_inspection_source_counts": {"materialized": 1},
            "packet_inspection_coverage_rate": 1.0,
            "failure_category_counts": {"in_progress": 1},
            "dominant_failure_category": "in_progress",
            "synthetic_run_count": 0,
            "isolated_run_count": 0,
            "missing_workflow_run_ids": [],
            "contract_drift_refs": [],
        },
        "smoke_freshness": {
            "kind": "native_smoke_freshness",
            "state": "fresh",
            "last_run_id": "run:workflow.native-self-hosted-smoke:latest",
            "last_requested_at": as_of.isoformat(),
            "age_seconds": 12.0,
            "fail_streak": 0,
            "latest_failure_category": "in_progress",
        },
    }
    assert "graph" not in payload
    assert "cutover_status" not in payload
    assert status_calls == [{"run_id": "run.1", "env": env}]
    assert evidence_calls == [{"env": env, "run_id": "run.1"}]
    assert query_calls == [
        {
            "env": env,
            "as_of": as_of,
            "bug_ids": None,
            "roadmap_item_ids": None,
            "cutover_gate_ids": None,
            "work_item_workflow_binding_ids": ("binding.1",),
            "workflow_run_ids": ["run.1"],
        }
    ]
    assert route_calls == [{"env": env, "as_of": as_of}]
    assert dispatch_calls[0]["env"] == env
    assert dispatch_calls[0]["as_of"] == as_of
    assert len(dispatch_calls[0]["work_bindings"]) == 1
    assert dispatch_calls[0]["work_bindings"][0].workflow_run_id == "run.1"
    assert cutover_calls[0]["env"] == env
    assert cutover_calls[0]["run_id"] == "run.1"
    assert cutover_calls[0]["as_of"] == as_of
    assert len(cutover_calls[0]["work_bindings"]) == 1
    assert cutover_calls[0]["work_bindings"][0].workflow_run_id == "run.1"
    assert cockpit_calls == [
        {
            "run_id": "run.1",
            "as_of": as_of,
            "route_authority": {"route_rows": 1},
            "dispatch_resolution": {"dispatch_rows": 1},
            "cutover_status": {"cutover_rows": 1},
        }
    ]


def test_native_operator_surface_fails_closed_on_nested_query_native_instance_mismatch(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return ()

    def _bad_query_operator_surface(**_: object) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": None,
                "workflow_run_ids": ["run.1"],
            },
            "counts": {
                "bugs": 0,
                "roadmap_items": 0,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 0,
            },
            "bugs": [],
            "roadmap_items": [],
            "cutover_gates": [],
            "work_item_workflow_bindings": [],
            "native_instance": {"kind": "native_instance", "label": "drifted"},
        }

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _bad_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
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

    try:
        native_operator_surface.query_native_operator_surface(
            run_id="run.1",
            env={"PRAXIS_RUNTIME_PROFILE": "praxis"},
            as_of=as_of,
        )
    except native_operator_surface.NativeOperatorSurfaceError as exc:
        assert exc.reason_code == "native_operator_surface.query_native_instance_mismatch"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected NativeOperatorSurfaceError")


def test_native_operator_surface_fails_closed_on_query_binding_echo_mismatch(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

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

    def _bad_query_operator_surface(**_: object) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": ["binding.1"],
                "workflow_run_ids": ["run.1"],
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
                    "source": {"kind": "cutover_gate", "id": "gate.1", "cutover_gate_id": "gate.1"},
                    "targets": {"workflow_class_id": "dispatch.1"},
                    "bound_by_decision_id": "decision.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _bad_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
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
            env={"PRAXIS_RUNTIME_PROFILE": "praxis"},
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_operator_surface.query_binding_echo_mismatch"


def test_native_operator_surface_fails_closed_on_duplicate_query_binding_echo(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

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

    def _bad_query_operator_surface(**_: object) -> dict[str, object]:
        echoed_binding = {
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
        }
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": ["binding.1"],
                "workflow_run_ids": ["run.1"],
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
            "work_item_workflow_bindings": [echoed_binding, echoed_binding],
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _bad_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
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
            env={"PRAXIS_RUNTIME_PROFILE": "praxis"},
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_operator_surface.query_binding_echo_mismatch"
    assert exc_info.value.details["echoed_duplicate_binding_ids"] == ("binding.1",)


def test_native_operator_surface_fails_closed_on_nested_query_as_of_mismatch(monkeypatch) -> None:
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "shared"}

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return ()

    def _bad_query_operator_surface(**_: object) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": datetime(2026, 4, 2, 21, 5, tzinfo=timezone.utc).isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": None,
                "workflow_run_ids": ["run.1"],
            },
            "counts": {
                "bugs": 0,
                "roadmap_items": 0,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 0,
            },
            "bugs": [],
            "roadmap_items": [],
            "cutover_gates": [],
            "work_item_workflow_bindings": [],
            "native_instance": {"kind": "native_instance", "label": "shared"},
        }

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _bad_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
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

    try:
        native_operator_surface.query_native_operator_surface(
            run_id="run.1",
            env={"PRAXIS_RUNTIME_PROFILE": "praxis"},
            as_of=as_of,
        )
    except native_operator_surface.NativeOperatorSurfaceError as exc:
        assert exc.reason_code == "native_operator_surface.query_as_of_mismatch"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected NativeOperatorSurfaceError")
