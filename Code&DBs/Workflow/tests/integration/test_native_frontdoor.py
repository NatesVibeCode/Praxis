from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
)
from observability.read_models import (
    InspectionReadModel,
    OperatorFrameReadModel,
    ProjectionCompleteness,
    ProjectionWatermark,
)
from registry.domain import RegistryResolver, RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    NativeWorkflowInstance,
    NativeInstanceResolutionError,
)
from surfaces.api import frontdoor

_REPO_ROOT = str(Path(__file__).resolve().parents[4])


def _request_payload() -> dict[str, object]:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return {
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "workflow_id": "workflow.alpha",
        "request_id": "request.alpha",
        "workflow_definition_id": "workflow_definition.alpha.v1",
        "definition_hash": "sha256:1111222233334444",
        "workspace_ref": workspace_ref,
        "runtime_profile_ref": runtime_profile_ref,
        "nodes": [
            {
                "node_id": "node_0",
                "node_type": MINIMAL_WORKFLOW_NODE_TYPE,
                "adapter_type": MINIMAL_WORKFLOW_NODE_TYPE,
                "display_name": "prepare",
                "inputs": {"task_name": "prepare"},
                "expected_outputs": {"result": "prepared"},
                "success_condition": {"status": "success"},
                "failure_behavior": {"status": "fail_closed"},
                "authority_requirements": {
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                "execution_boundary": {"workspace_ref": workspace_ref},
                "position_index": 0,
            },
            {
                "node_id": "node_1",
                "node_type": MINIMAL_WORKFLOW_NODE_TYPE,
                "adapter_type": MINIMAL_WORKFLOW_NODE_TYPE,
                "display_name": "admit",
                "inputs": {"task_name": "admit"},
                "expected_outputs": {"result": "admitted"},
                "success_condition": {"status": "success"},
                "failure_behavior": {"status": "fail_closed"},
                "authority_requirements": {
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                "execution_boundary": {"workspace_ref": workspace_ref},
                "position_index": 1,
            },
        ],
        "edges": [
            {
                "edge_id": "edge_0",
                "edge_type": MINIMAL_WORKFLOW_EDGE_TYPE,
                "from_node_id": "node_0",
                "to_node_id": "node_1",
                "release_condition": {"upstream_result": "success"},
                "payload_mapping": {},
                "position_index": 0,
            },
        ],
    }


def _registry() -> RegistryResolver:
    return RegistryResolver(
        workspace_records={
            "workspace.alpha": (
                WorkspaceAuthorityRecord(
                    workspace_ref="workspace.alpha",
                    repo_root=_REPO_ROOT,
                    workdir=_REPO_ROOT,
                ),
            ),
        },
        runtime_profile_records={
            "runtime_profile.alpha": (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref="runtime_profile.alpha",
                    model_profile_id="model.alpha",
                    provider_policy_id="provider.alpha",
                ),
            ),
        },
    )


@dataclass
class _FakeStatus:
    schema_bootstrapped: bool

    def to_json(self) -> dict[str, object]:
        return {
            "data_dir": f"{_REPO_ROOT}/Code&DBs/Databases/postgres-dev/data",
            "log_file": f"{_REPO_ROOT}/Code&DBs/Databases/postgres-dev/log/postgres.log",
            "database_url": "postgresql://workflow@127.0.0.1:5432/workflow",
            "pid": 1234,
            "port": 5432,
            "process_running": True,
            "database_reachable": True,
            "schema_bootstrapped": self.schema_bootstrapped,
        }


class _MissingColumnError(RuntimeError):
    def __init__(self, column_name: str) -> None:
        super().__init__(f'column "{column_name}" does not exist')
        self.sqlstate = "42703"


class _FakeConnection:
    def __init__(
        self,
        *,
        run_row: dict[str, object],
        seen: dict[str, object],
        packet_row: dict[str, object] | None = None,
        missing_columns: frozenset[str] = frozenset(),
    ):
        self._run_row = run_row
        self._seen = seen
        self._packet_row = packet_row
        self._missing_columns = missing_columns

    async def fetchrow(self, query: str, *args: object):
        self._seen["status_queries"].append((query, args))
        if (
            "packet_inspection" in query
            and "NULL::jsonb AS packet_inspection" not in query
            and "packet_inspection" in self._missing_columns
        ):
            raise _MissingColumnError("packet_inspection")
        if "execution_packets" in query and self._packet_row is not None:
            row = dict(self._packet_row)
            row.setdefault("run_id", args[0])
            return row
        row = dict(self._run_row)
        run_id = args[0]
        row["run_id"] = run_id
        row["context_bundle_id"] = f"context:{run_id}"
        row["admission_decision_id"] = f"admission:{run_id}"
        return row

    async def fetch(self, query: str, *args: object):
        self._seen["status_queries"].append((query, args))
        return [
            {
                "label": "build.codegen",
                "status": "running",
                "attempt": 1,
                "agent_slug": "agent.alpha",
                "resolved_agent": None,
                "last_error_code": None,
                "created_at": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
                "ready_at": None,
                "claimed_at": None,
                "started_at": None,
                "finished_at": None,
                "submission_id": "sub-1",
                "submission_result_kind": "code_change",
                "submission_summary": "sealed worker output",
                "submission_comparison_status": "matched",
                "submission_operation_set": [{"path": "runtime/workflow/submission_capture.py", "action": "update"}],
                "latest_submission_review_decision": "approve",
                "latest_submission_review_summary": "looks good",
            }
        ]

    async def close(self) -> None:
        self._seen["closed_connections"] += 1


def test_native_frontdoor_status_serializes_operator_frame_summaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native_instance = NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )
    env = {"WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow"}
    run_row = {
        "run_id": "run:workflow.operator:frames",
        "workflow_id": "workflow.operator",
        "request_id": "request.operator",
        "request_digest": "digest.operator",
        "workflow_definition_id": "workflow_definition.operator.v1",
        "admitted_definition_hash": "sha256:operator",
        "current_state": "running",
        "terminal_reason_code": None,
        "run_idempotency_key": "request.operator",
        "context_bundle_id": "context.operator",
        "authority_context_digest": "digest.operator",
        "admission_decision_id": "admission.operator",
        "requested_at": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
        "admitted_at": datetime(2026, 4, 2, 12, 0, 1, tzinfo=timezone.utc),
        "started_at": datetime(2026, 4, 2, 12, 0, 2, tzinfo=timezone.utc),
        "finished_at": None,
        "last_event_id": "event:workflow.operator:18",
        "request_envelope": {"workflow_id": "workflow.operator"},
    }
    seen = {
        "status_queries": [],
        "closed_connections": 0,
        "resolved_envs": [],
    }

    def _resolve_instance(*, env: dict[str, str] | None = None):
        seen["resolved_envs"].append(dict(env or {}))
        return native_instance

    async def _connect_database(env_arg: dict[str, str] | None = None):
        assert env_arg == env
        return _FakeConnection(run_row=run_row, seen=seen)

    class _FakeRuntimeOrchestrator:
        def __init__(self, *, evidence_reader):
            self._evidence_reader = evidence_reader

        def inspect_run(self, *, run_id: str) -> InspectionReadModel:
            assert run_id == run_row["run_id"]
            return InspectionReadModel(
                run_id=run_id,
                request_id="request.operator",
                completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
                watermark=ProjectionWatermark(evidence_seq=18, source="canonical_evidence"),
                evidence_refs=("event.1", "receipt.2"),
                current_state="running",
                node_timeline=("source:succeeded", "foreach:running"),
                terminal_reason=None,
                operator_frame_source="canonical_operator_frames",
                operator_frames=(
                    OperatorFrameReadModel(
                        operator_frame_id="operator_frame.alpha",
                        node_id="foreach",
                        operator_kind="foreach",
                        frame_state="running",
                        item_index=0,
                        iteration_index=None,
                        source_snapshot={"item": "alpha"},
                        aggregate_outputs={"item": "alpha"},
                        active_count=1,
                        stop_reason=None,
                        started_at=datetime(2026, 4, 2, 12, 0, 3, tzinfo=timezone.utc),
                        finished_at=None,
                    ),
                ),
            )

    monkeypatch.setattr(frontdoor, "resolve_native_instance", _resolve_instance)
    monkeypatch.setattr(frontdoor, "RuntimeOrchestrator", _FakeRuntimeOrchestrator)

    api = frontdoor.NativeWorkflowFrontdoor(
        registry=_registry(),
        connect_database=_connect_database,
        evidence_reader_factory=lambda env_arg: {"env": env_arg},
    )

    status_payload = api.status(run_id=run_row["run_id"], env=env)

    assert seen["resolved_envs"] == [env]
    assert status_payload["inspection"] is not None
    assert status_payload["inspection"]["operator_frame_source"] == "canonical_operator_frames"
    assert status_payload["inspection"]["operator_frames"] == [
        {
            "operator_frame_id": "operator_frame.alpha",
            "node_id": "foreach",
            "operator_kind": "foreach",
            "frame_state": "running",
            "item_index": 0,
            "iteration_index": None,
            "source_snapshot": {"item": "alpha"},
            "aggregate_outputs": {"item": "alpha"},
            "active_count": 1,
            "stop_reason": None,
            "started_at": "2026-04-02T12:00:03+00:00",
            "finished_at": None,
        }
    ]


def test_native_frontdoor_stays_repo_local_and_fails_closed_without_native_instance_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_payload = _request_payload()
    native_instance = NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )
    env = {"WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow"}
    run_row = {
        "run_id": "run:workflow.alpha:cf4660b3f72a2f98",
        "workflow_id": "workflow.alpha",
        "request_id": "request.alpha",
        "request_digest": "digest.alpha",
        "workflow_definition_id": "workflow_definition.alpha.v1",
        "admitted_definition_hash": "sha256:1111222233334444",
        "current_state": "claim_accepted",
        "terminal_reason_code": None,
        "run_idempotency_key": "request.alpha",
        "context_bundle_id": "context:run:workflow.alpha:cf4660b3f72a2f98",
        "authority_context_digest": "digest.alpha",
        "admission_decision_id": "admission:validation:cf4660b3f72a2f98",
        "requested_at": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
        "admitted_at": datetime(2026, 4, 2, 12, 0, 1, tzinfo=timezone.utc),
        "started_at": None,
        "finished_at": None,
        "last_event_id": None,
    }
    seen: dict[str, object] = {
        "resolved_envs": [],
        "health_envs": [],
        "connect_envs": [],
        "submissions": [],
        "status_queries": [],
        "bootstrap_calls": 0,
        "closed_connections": 0,
    }

    def _resolve_instance(*, env=None, config_path=None):
        assert config_path is None
        seen["resolved_envs"].append(dict(env or {}))
        return native_instance

    def _postgres_health(env=None):
        seen["health_envs"].append(dict(env or {}))
        return _FakeStatus(schema_bootstrapped=True)

    async def _connect_database(env=None):
        seen["connect_envs"].append(dict(env or {}))
        return _FakeConnection(run_row=run_row, seen=seen)

    async def _bootstrap_schema(conn) -> None:
        seen["bootstrap_calls"] += 1

    async def _persist_submission(conn, *, submission):
        seen["submissions"].append(submission)
        run_row.update(
            {
                "run_id": submission.run.run_id,
                "workflow_id": submission.run.workflow_id,
                "request_id": submission.run.request_id,
                "workflow_definition_id": submission.run.workflow_definition_id,
                "current_state": submission.run.current_state,
                "terminal_reason_code": submission.run.terminal_reason_code,
                "run_idempotency_key": submission.run.run_idempotency_key,
                "context_bundle_id": submission.run.context_bundle_id,
                "authority_context_digest": submission.run.authority_context_digest,
                "admission_decision_id": submission.run.admission_decision_id,
                "requested_at": submission.run.requested_at,
                "admitted_at": submission.run.admitted_at,
                "started_at": submission.run.started_at,
                "finished_at": submission.run.finished_at,
                "last_event_id": submission.run.last_event_id,
            }
        )
        return type(
            "WriteResult",
            (),
            {
                "admission_decision_id": submission.decision.admission_decision_id,
                "run_id": submission.run.run_id,
            },
        )()

    monkeypatch.setattr(frontdoor, "resolve_native_instance", _resolve_instance)

    api = frontdoor.NativeWorkflowFrontdoor(
        registry=_registry(),
        postgres_health_service=_postgres_health,
        connect_database=_connect_database,
        bootstrap_schema=_bootstrap_schema,
        persist_submission=_persist_submission,
    )

    submit_payload = api.submit(request_payload=request_payload, env=env)
    status_payload = api.status(run_id=submit_payload["run"]["run_id"], env=env)
    health_payload = api.health(env=env)

    assert len(seen["resolved_envs"]) == 3
    assert seen["connect_envs"] == [env, env]
    assert seen["health_envs"] == [env]
    assert seen["bootstrap_calls"] == 1
    assert seen["closed_connections"] == 2
    assert len(seen["submissions"]) == 1
    submission = seen["submissions"][0]
    assert submission.run.workflow_id == "workflow.alpha"
    assert submission.run.request_id == "request.alpha"
    assert submission.run.current_state == "claim_accepted"
    assert submit_payload["native_instance"]["repo_root"] == native_instance.repo_root
    assert submit_payload["run"]["workflow_definition_id"] == "workflow_definition.alpha.v1"
    assert submit_payload["run"]["current_state"] == "claim_accepted"
    assert submit_payload["admission_decision"]["decision"] == "admit"
    assert status_payload["native_instance"]["workdir"] == native_instance.workdir
    assert status_payload["run"]["run_id"] == submit_payload["run"]["run_id"]
    assert status_payload["run"]["current_state"] == "claim_accepted"
    assert status_payload["run"]["request_digest"] == "digest.alpha"
    assert status_payload["run"]["admitted_definition_hash"] == "sha256:1111222233334444"
    assert status_payload["run"]["jobs"][0]["submission"] == {
        "submission_id": "sub-1",
        "result_kind": "code_change",
        "summary": "sealed worker output",
        "comparison_status": "matched",
        "measured_summary": {"create": 0, "update": 1, "delete": 0, "rename": 0, "total": 1},
        "latest_review_decision": "approve",
        "latest_review_summary": "looks good",
    }
    assert status_payload["inspection"] is None
    assert status_payload["observability"]["kind"] == "frontdoor_observability"
    assert status_payload["observability"]["health_state"] == "degraded"
    assert status_payload["observability"]["contract_drift"]["status"] == "aligned"
    assert status_payload["observability"]["run_identity"]["request_digest"] == "digest.alpha"
    assert status_payload["observability"]["job_count"] == 1
    assert status_payload["observability"]["job_status_counts"] == {"running": 1}
    assert status_payload["observability"]["anomaly_digest"]["headline"] == "No inspection snapshot was produced for this run"
    assert "FROM workflow_runs" in seen["status_queries"][0][0]
    assert health_payload["native_instance"]["praxis_receipts_dir"] == native_instance.receipts_dir
    assert health_payload["database"]["schema_bootstrapped"] is True


def test_native_frontdoor_surfaces_shadow_packet_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native_instance = NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )
    env = {"WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow"}
    run_id = "run:workflow.shadow:packet"
    run_row = {
        "run_id": run_id,
        "workflow_id": "workflow.shadow",
        "request_id": "request.shadow",
        "request_digest": "digest.shadow",
        "workflow_definition_id": "workflow_definition.shadow.v1",
        "admitted_definition_hash": "sha256:shadow",
        "current_state": "claim_accepted",
        "terminal_reason_code": None,
        "run_idempotency_key": "request.shadow",
        "context_bundle_id": "context.shadow",
        "authority_context_digest": "digest.shadow",
        "admission_decision_id": "admission.shadow",
        "requested_at": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
        "admitted_at": datetime(2026, 4, 2, 12, 0, 1, tzinfo=timezone.utc),
        "started_at": datetime(2026, 4, 2, 12, 1, tzinfo=timezone.utc),
        "finished_at": None,
        "last_event_id": None,
        "request_envelope": {
            "name": "shadow-spec",
            "spec_snapshot": {
                "definition_revision": "definition.rev.exec",
                "plan_revision": "plan.rev.exec",
                "verify_refs": ["verify.exec"],
                "packet_provenance": {
                    "source_kind": "shadow_execution",
                    "workspace_ref": "workspace.alpha",
                    "runtime_profile_ref": "runtime.alpha",
                },
            },
        },
    }
    packet_payload = {
        "definition_revision": "definition.rev.exec",
        "plan_revision": "plan.rev.packet",
        "packet_version": 1,
        "workflow_id": "workflow.shadow",
        "run_id": run_id,
        "spec_name": "shadow-spec",
        "source_kind": "shadow_execution",
        "authority_refs": ["definition.rev.exec", "plan.rev.packet"],
        "model_messages": [
            {
                "job_label": "job-shadow",
                "adapter_type": "chat",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "system seed"},
                    {"role": "user", "content": "inspect me"},
                ],
            }
        ],
        "reference_bindings": [
            {
                "binding_kind": "model_input",
                "ref": "binding:model.shadow",
            }
        ],
        "capability_bindings": [
            {
                "binding_kind": "filesystem",
                "ref": "capability:fs.read",
            }
        ],
        "verify_refs": ["verify.packet"],
        "packet_provenance": {
            "source_kind": "shadow_execution",
            "workspace_ref": "workspace.alpha",
            "runtime_profile_ref": "runtime.alpha",
        },
        "authority_inputs": {
            "packet_provenance": {
                "source_kind": "shadow_execution",
                "workspace_ref": "workspace.alpha",
                "runtime_profile_ref": "runtime.alpha",
            },
        },
        "file_inputs": {
            "workdir": _REPO_ROOT,
            "scope_read": ["input.py"],
            "scope_write": ["output.py"],
        },
        "packet_hash": "packet.hash.shadow",
        "packet_revision": "packet.rev.shadow.1",
        "decision_ref": "decision.packet.shadow.1",
    }
    seen: dict[str, object] = {
        "resolved_envs": [],
        "health_envs": [],
        "connect_envs": [],
        "submissions": [],
        "status_queries": [],
        "bootstrap_calls": 0,
        "closed_connections": 0,
    }

    def _resolve_instance(*, env=None, config_path=None):
        assert config_path is None
        seen["resolved_envs"].append(dict(env or {}))
        return native_instance

    def _postgres_health(env=None):
        seen["health_envs"].append(dict(env or {}))
        return _FakeStatus(schema_bootstrapped=True)

    async def _connect_database(env=None):
        seen["connect_envs"].append(dict(env or {}))
        return _FakeConnection(run_row=run_row, seen=seen, packet_row={"packets": [packet_payload]})

    async def _bootstrap_schema(conn) -> None:
        seen["bootstrap_calls"] += 1

    async def _persist_submission(conn, *, submission):
        seen["submissions"].append(submission)
        return type(
            "WriteResult",
            (),
            {
                "admission_decision_id": submission.decision.admission_decision_id,
                "run_id": submission.run.run_id,
            },
        )()

    monkeypatch.setattr(frontdoor, "resolve_native_instance", _resolve_instance)

    api = frontdoor.NativeWorkflowFrontdoor(
        registry=_registry(),
        postgres_health_service=_postgres_health,
        connect_database=_connect_database,
        bootstrap_schema=_bootstrap_schema,
        persist_submission=_persist_submission,
    )

    status_payload = api.status(run_id=run_id, env=env)

    assert status_payload["run"]["run_id"] == run_id
    assert status_payload["packet_inspection"]["packet_revision"] == "packet.rev.shadow.1"
    assert status_payload["packet_inspection"]["current_packet"]["model_messages"][0]["messages"][1]["content"] == "inspect me"
    assert status_payload["packet_inspection"]["current_packet"]["reference_bindings"][0]["ref"] == "binding:model.shadow"
    assert status_payload["packet_inspection"]["current_packet"]["packet_provenance"]["workspace_ref"] == "workspace.alpha"
    assert status_payload["packet_inspection"]["drift"]["status"] == "drifted"
    assert status_payload["observability"]["packet_inspection_source"] == "derived"
    assert status_payload["observability"]["run_identity"]["packet_revision"] == "packet.rev.shadow.1"
    assert status_payload["observability"]["failure_taxonomy"]["dominant_category"] == "packet_drift"


def test_native_frontdoor_status_tolerates_legacy_schema_without_packet_inspection_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native_instance = NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )
    env = {"WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow"}
    run_id = "run:workflow.shadow:legacy-schema"
    run_row = {
        "run_id": run_id,
        "workflow_id": "workflow.shadow",
        "request_id": "request.shadow",
        "request_digest": "digest.shadow",
        "workflow_definition_id": "workflow_definition.shadow.v1",
        "admitted_definition_hash": "sha256:shadow",
        "current_state": "claim_accepted",
        "terminal_reason_code": None,
        "run_idempotency_key": "request.shadow",
        "context_bundle_id": "context.shadow",
        "authority_context_digest": "digest.shadow",
        "admission_decision_id": "admission.shadow",
        "requested_at": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
        "admitted_at": datetime(2026, 4, 2, 12, 0, 1, tzinfo=timezone.utc),
        "started_at": datetime(2026, 4, 2, 12, 1, tzinfo=timezone.utc),
        "finished_at": None,
        "last_event_id": None,
        "request_envelope": {
            "name": "shadow-spec",
            "spec_snapshot": {
                "definition_revision": "definition.rev.exec",
                "plan_revision": "plan.rev.exec",
                "verify_refs": ["verify.exec"],
                "packet_provenance": {
                    "source_kind": "shadow_execution",
                    "workspace_ref": "workspace.alpha",
                    "runtime_profile_ref": "runtime.alpha",
                },
            },
        },
    }
    packet_payload = {
        "definition_revision": "definition.rev.exec",
        "plan_revision": "plan.rev.packet",
        "packet_version": 1,
        "workflow_id": "workflow.shadow",
        "run_id": run_id,
        "spec_name": "shadow-spec",
        "source_kind": "shadow_execution",
        "authority_refs": ["definition.rev.exec", "plan.rev.packet"],
        "model_messages": [
            {
                "job_label": "job-shadow",
                "adapter_type": "chat",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "system seed"},
                    {"role": "user", "content": "inspect me"},
                ],
            }
        ],
        "reference_bindings": [
            {
                "binding_kind": "model_input",
                "ref": "binding:model.shadow",
            }
        ],
        "capability_bindings": [
            {
                "binding_kind": "filesystem",
                "ref": "capability:fs.read",
            }
        ],
        "verify_refs": ["verify.packet"],
        "packet_provenance": {
            "source_kind": "shadow_execution",
            "workspace_ref": "workspace.alpha",
            "runtime_profile_ref": "runtime.alpha",
        },
        "authority_inputs": {
            "packet_provenance": {
                "source_kind": "shadow_execution",
                "workspace_ref": "workspace.alpha",
                "runtime_profile_ref": "runtime.alpha",
            },
        },
        "file_inputs": {
            "workdir": _REPO_ROOT,
            "scope_read": ["input.py"],
            "scope_write": ["output.py"],
        },
        "packet_hash": "packet.hash.shadow",
        "packet_revision": "packet.rev.shadow.1",
        "decision_ref": "decision.packet.shadow.1",
    }
    seen: dict[str, object] = {
        "resolved_envs": [],
        "health_envs": [],
        "connect_envs": [],
        "submissions": [],
        "status_queries": [],
        "bootstrap_calls": 0,
        "closed_connections": 0,
    }

    def _resolve_instance(*, env=None, config_path=None):
        assert config_path is None
        seen["resolved_envs"].append(dict(env or {}))
        return native_instance

    def _postgres_health(env=None):
        seen["health_envs"].append(dict(env or {}))
        return _FakeStatus(schema_bootstrapped=True)

    async def _connect_database(env=None):
        seen["connect_envs"].append(dict(env or {}))
        return _FakeConnection(
            run_row=run_row,
            seen=seen,
            packet_row={"packets": [packet_payload]},
            missing_columns=frozenset({"packet_inspection"}),
        )

    async def _bootstrap_schema(conn) -> None:
        seen["bootstrap_calls"] += 1

    async def _persist_submission(conn, *, submission):
        seen["submissions"].append(submission)
        return type(
            "WriteResult",
            (),
            {
                "admission_decision_id": submission.decision.admission_decision_id,
                "run_id": submission.run.run_id,
            },
        )()

    monkeypatch.setattr(frontdoor, "resolve_native_instance", _resolve_instance)

    api = frontdoor.NativeWorkflowFrontdoor(
        registry=_registry(),
        postgres_health_service=_postgres_health,
        connect_database=_connect_database,
        bootstrap_schema=_bootstrap_schema,
        persist_submission=_persist_submission,
    )

    status_payload = api.status(run_id=run_id, env=env)

    assert status_payload["run"]["run_id"] == run_id
    assert status_payload["packet_inspection"]["packet_revision"] == "packet.rev.shadow.1"
    assert status_payload["packet_inspection"]["drift"]["status"] == "drifted"
    assert status_payload["observability"]["packet_drift_status"] == "drifted"
    assert status_payload["observability"]["packet_inspection_source"] == "derived"
    assert status_payload["observability"]["health_state"] == "degraded"
    assert status_payload["observability"]["contract_drift"]["status"] == "drifted"
    assert status_payload["observability"]["contract_drift"]["issues"][0]["issue_code"] == "workflow_runs.packet_inspection_column_missing"
    assert any(
        "NULL::jsonb AS packet_inspection" in query
        for query, _ in seen["status_queries"]
        if isinstance(query, str)
    )


@pytest.mark.parametrize(
    ("env_override", "expected_details"),
    [
        (
            {
                PRAXIS_RUNTIME_PROFILE_ENV: "runtime-profile.alt",
            },
            {
                "environment_variable": PRAXIS_RUNTIME_PROFILE_ENV,
                "expected": "praxis",
                "actual": "runtime-profile.alt",
            },
        ),
        (
            {
                "PRAXIS_RECEIPTS_DIR": "/tmp/legacy-control/runtime_receipts",
            },
            {
                "environment_variable": "PRAXIS_RECEIPTS_DIR",
                "expected": str(
                    Path(f"{_REPO_ROOT}/artifacts/runtime_receipts").resolve()
                ),
                "actual": str(Path("/tmp/legacy-control/runtime_receipts").resolve()),
            },
        ),
    ],
)
def test_native_frontdoor_fails_closed_before_downstream_services_when_native_instance_contract_mismatches(
    env_override: dict[str, str],
    expected_details: dict[str, str],
) -> None:
    request_payload = _request_payload()
    env = {
        "WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow",
        **env_override,
    }
    seen = {
        "health_calls": 0,
        "connect_calls": 0,
        "bootstrap_calls": 0,
        "persist_calls": 0,
    }

    def _postgres_health(env=None):
        seen["health_calls"] += 1
        raise AssertionError("native instance resolution should fail before health dependencies run")

    async def _connect_database(env=None):
        seen["connect_calls"] += 1
        raise AssertionError("native instance resolution should fail before database access")

    async def _bootstrap_schema(conn) -> None:
        seen["bootstrap_calls"] += 1
        raise AssertionError("native instance resolution should fail before schema bootstrap")

    async def _persist_submission(conn, *, submission):
        seen["persist_calls"] += 1
        raise AssertionError("native instance resolution should fail before persistence")

    api = frontdoor.NativeWorkflowFrontdoor(
        registry=_registry(),
        postgres_health_service=_postgres_health,
        connect_database=_connect_database,
        bootstrap_schema=_bootstrap_schema,
        persist_submission=_persist_submission,
    )

    for invoke in (
        lambda: api.submit(request_payload=request_payload, env=env),
        lambda: api.status(run_id="run:missing", env=env),
        lambda: api.health(env=env),
    ):
        with pytest.raises(NativeInstanceResolutionError) as exc_info:
            invoke()
        assert exc_info.value.reason_code == "native_instance.boundary_mismatch"
        assert exc_info.value.details == expected_details

    assert seen == {
        "health_calls": 0,
        "connect_calls": 0,
        "bootstrap_calls": 0,
        "persist_calls": 0,
    }
