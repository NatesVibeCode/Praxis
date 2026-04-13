from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import threading
import time
from typing import Any
import uuid

import pytest

from adapters.deterministic import AdapterRegistry, DeterministicTaskRequest, DeterministicTaskResult
from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner
from runtime.persistent_evidence import PostgresEvidenceWriter
from storage.postgres import (
    PostgresConfigurationError,
    PostgresEvidenceReader,
    PostgresOperatorFrameRepository,
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowRunWrite,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    ensure_postgres_available,
    persist_workflow_admission,
)

_TEST_ENV = {"WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test"}


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _seed_workflow_run(conn, *, suffix: str) -> dict[str, str]:
    workflow_id = f"workflow.operator_frames.{suffix}"
    workflow_definition_id = f"workflow_definition.operator_frames.{suffix}"
    admission_decision_id = f"admission_decision.operator_frames.{suffix}"
    run_id = f"run.operator_frames.{suffix}"
    request_id = f"request.operator_frames.{suffix}"
    requested_at = datetime(2026, 4, 11, 12, 0, tzinfo=timezone.utc)
    admitted_at = datetime(2026, 4, 11, 12, 0, 5, tzinfo=timezone.utc)

    conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES ($1, $2, 1, 1, $3, 'admitted', $4::jsonb, $5::jsonb, $6, NULL)
        """,
        workflow_definition_id,
        workflow_id,
        f"sha256:{suffix}",
        json.dumps({"workflow_id": workflow_id}),
        json.dumps({"workflow_id": workflow_id}),
        requested_at,
    )
    conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES ($1, $2, $3, 'admit', $4, $5, $6, $7, $8, $9)
        """,
        admission_decision_id,
        workflow_id,
        request_id,
        "policy.admit",
        admitted_at,
        "policy.engine",
        f"policy_snapshot.{suffix}",
        f"validation_result.{suffix}",
        f"authority_context.{suffix}",
    )
    conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 1,
            $9::jsonb, $10, $11, 'running', NULL, $12, $13, $13, NULL, NULL
        )
        """,
        run_id,
        workflow_id,
        request_id,
        f"digest.{suffix}",
        f"authority_digest.{suffix}",
        workflow_definition_id,
        f"sha256:{suffix}",
        request_id,
        json.dumps({"workflow_id": workflow_id, "run_id": run_id}),
        f"context_bundle.{suffix}",
        admission_decision_id,
        requested_at,
        admitted_at,
    )
    return {
        "workflow_definition_id": workflow_definition_id,
        "admission_decision_id": admission_decision_id,
        "workflow_id": workflow_id,
        "run_id": run_id,
    }


def _resolver(*, suffix: str) -> RegistryResolver:
    workspace_ref = f"workspace.{suffix}"
    runtime_profile_ref = f"runtime_profile.{suffix}"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=f"/tmp/{workspace_ref}",
                    workdir=f"/tmp/{workspace_ref}/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id=f"model.{suffix}",
                    provider_policy_id=f"provider_policy.{suffix}",
                ),
            ),
        },
    )


def _node(
    *,
    suffix: str,
    node_id: str,
    position_index: int,
    display_name: str,
    adapter_type: str = MINIMAL_WORKFLOW_NODE_TYPE,
    inputs: dict[str, Any] | None = None,
    expected_outputs: dict[str, Any] | None = None,
    template_owner_node_id: str | None = None,
) -> WorkflowNodeContract:
    workspace_ref = f"workspace.{suffix}"
    runtime_profile_ref = f"runtime_profile.{suffix}"
    return WorkflowNodeContract(
        node_id=node_id,
        node_type=MINIMAL_WORKFLOW_NODE_TYPE,
        adapter_type=adapter_type,
        display_name=display_name,
        inputs=inputs or {"task_name": display_name},
        expected_outputs=expected_outputs or {},
        success_condition={"status": "success"},
        failure_behavior={"status": "fail_closed"},
        authority_requirements={
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
        },
        execution_boundary={"workspace_ref": workspace_ref},
        position_index=position_index,
        template_owner_node_id=template_owner_node_id,
    )


def _edge(
    *,
    edge_id: str,
    from_node_id: str,
    to_node_id: str,
    position_index: int,
    edge_type: str = MINIMAL_WORKFLOW_EDGE_TYPE,
    release_condition: dict[str, Any] | None = None,
    payload_mapping: dict[str, Any] | None = None,
    template_owner_node_id: str | None = None,
) -> WorkflowEdgeContract:
    return WorkflowEdgeContract(
        edge_id=edge_id,
        edge_type=edge_type,
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        release_condition=release_condition or {"upstream_result": "success"},
        payload_mapping=payload_mapping or {},
        position_index=position_index,
        template_owner_node_id=template_owner_node_id,
    )


def _request_envelope(request: WorkflowRequest) -> dict[str, object]:
    workflow_definition_id = request.workflow_definition_id
    return {
        "schema_version": request.schema_version,
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": workflow_definition_id,
        "definition_version": 1,
        "definition_hash": request.definition_hash,
        "workspace_ref": request.workspace_ref,
        "runtime_profile_ref": request.runtime_profile_ref,
        "nodes": [
            {
                "workflow_definition_node_id": f"{workflow_definition_id}:{node.node_id}",
                "workflow_definition_id": workflow_definition_id,
                "node_id": node.node_id,
                "node_type": node.node_type,
                "schema_version": request.schema_version,
                "adapter_type": node.adapter_type,
                "display_name": node.display_name,
                "inputs": dict(node.inputs),
                "expected_outputs": dict(node.expected_outputs),
                "success_condition": dict(node.success_condition),
                "failure_behavior": dict(node.failure_behavior),
                "authority_requirements": dict(node.authority_requirements),
                "execution_boundary": dict(node.execution_boundary),
                "position_index": node.position_index,
                "template_owner_node_id": node.template_owner_node_id,
            }
            for node in request.nodes
        ],
        "edges": [
            {
                "workflow_definition_edge_id": f"{workflow_definition_id}:{edge.edge_id}",
                "workflow_definition_id": workflow_definition_id,
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "schema_version": request.schema_version,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "release_condition": dict(edge.release_condition),
                "payload_mapping": dict(edge.payload_mapping),
                "position_index": edge.position_index,
                "template_owner_node_id": edge.template_owner_node_id,
            }
            for edge in request.edges
        ],
    }


def _submission_from_outcome(*, outcome, requested_at) -> WorkflowAdmissionSubmission:
    request = outcome.workflow_request
    decision = outcome.admission_decision
    return WorkflowAdmissionSubmission(
        decision=WorkflowAdmissionDecisionWrite(
            admission_decision_id=decision.admission_decision_id,
            workflow_id=request.workflow_id,
            request_id=request.request_id,
            decision=decision.decision.value,
            reason_code=decision.reason_code,
            decided_at=decision.decided_at,
            decided_by=decision.decided_by,
            policy_snapshot_ref=decision.policy_snapshot_ref,
            validation_result_ref=decision.validation_result_ref,
            authority_context_ref=decision.authority_context_ref,
        ),
        run=WorkflowRunWrite(
            run_id=outcome.run_id,
            workflow_id=request.workflow_id,
            request_id=request.request_id,
            request_digest=outcome.request_digest,
            authority_context_digest=outcome.route_identity.authority_context_digest,
            workflow_definition_id=outcome.admitted_definition_ref or request.workflow_definition_id,
            admitted_definition_hash=outcome.admitted_definition_hash or request.definition_hash,
            run_idempotency_key=outcome.run_idempotency_key,
            schema_version=request.schema_version,
            request_envelope=_request_envelope(request),
            context_bundle_id=decision.authority_context_ref,
            admission_decision_id=decision.admission_decision_id,
            current_state=outcome.current_state.value,
            requested_at=requested_at,
            admitted_at=decision.decided_at,
            terminal_reason_code=None,
            started_at=None,
            finished_at=None,
            last_event_id=None,
        ),
    )


class LoopEchoAdapter:
    executor_type = "adapter.loop_echo"

    def __init__(self, *, sleep_s: float = 0.0) -> None:
        self._sleep_s = sleep_s
        self._lock = threading.Lock()
        self._active = 0
        self.max_seen = 0

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        payload = dict(request.input_payload)
        started_at = datetime.now(timezone.utc)
        with self._lock:
            self._active += 1
            self.max_seen = max(self.max_seen, self._active)
        try:
            if self._sleep_s:
                time.sleep(self._sleep_s)
            item = payload.get("loop_item")
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs={"input_payload": payload},
                outputs={"item": item, "item_index": payload.get("loop_item_index")},
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
        finally:
            with self._lock:
                self._active -= 1


class TriggerablePostgresEvidenceWriter(PostgresEvidenceWriter):
    def __init__(self, *, database_url: str) -> None:
        super().__init__(database_url=database_url)
        self._cancelled_runs: set[str] = set()
        self._lock = threading.Lock()

    def request_cancel(self, run_id: str) -> None:
        with self._lock:
            self._cancelled_runs.add(run_id)

    def current_state_for_run(self, run_id: str) -> str | None:
        with self._lock:
            if run_id in self._cancelled_runs:
                return "cancelled"
        return super().current_state_for_run(run_id)


class CancelAfterFirstItemAdapter:
    executor_type = "adapter.cancel_after_first_item"

    def __init__(self, *, cancel_once) -> None:
        self._cancel_once = cancel_once
        self._lock = threading.Lock()
        self._cancelled = False

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        payload = dict(request.input_payload)
        started_at = datetime.now(timezone.utc)
        with self._lock:
            if not self._cancelled:
                self._cancelled = True
                self._cancel_once()
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs={"input_payload": payload},
            outputs={"item": payload.get("loop_item"), "item_index": payload.get("loop_item_index")},
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
        )


def test_postgres_operator_frame_repository_round_trips_frame_state() -> None:
    try:
        conn = ensure_postgres_available(env=_TEST_ENV)
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for operator frame repository integration test: "
            f"{exc.reason_code}"
        )

    suffix = _unique_suffix()
    authority = _seed_workflow_run(conn, suffix=suffix)
    repository = PostgresOperatorFrameRepository(conn)
    frame_id = f"operator_frame.{suffix}"

    try:
        created = repository.create_frame(
            operator_frame_id=frame_id,
            run_id=authority["run_id"],
            node_id="foreach",
            operator_kind="foreach",
            item_index=0,
            iteration_index=None,
            source_snapshot={"loop_item": "alpha"},
            active_count=0,
        )
        assert created.frame_state == "created"
        assert created.source_snapshot == {"loop_item": "alpha"}

        running = repository.mark_running(
            operator_frame_id=frame_id,
            active_count=1,
        )
        assert running.frame_state == "running"
        assert running.active_count == 1

        succeeded = repository.mark_succeeded(
            operator_frame_id=frame_id,
            aggregate_outputs={"item": "alpha", "item_index": 0},
            stop_reason="all_items_completed",
            active_count=0,
        )
        assert succeeded.frame_state == "succeeded"
        assert succeeded.aggregate_outputs == {"item": "alpha", "item_index": 0}
        assert succeeded.stop_reason == "all_items_completed"
        assert succeeded.finished_at is not None

        listed = repository.list_for_node(run_id=authority["run_id"], node_id="foreach")
        assert len(listed) == 1
        assert listed[0] == succeeded
    finally:
        conn.execute("DELETE FROM run_operator_frames WHERE run_id = $1", authority["run_id"])
        conn.execute("DELETE FROM workflow_runs WHERE run_id = $1", authority["run_id"])
        conn.execute(
            "DELETE FROM admission_decisions WHERE admission_decision_id = $1",
            authority["admission_decision_id"],
        )
        conn.execute(
            "DELETE FROM workflow_definitions WHERE workflow_definition_id = $1",
            authority["workflow_definition_id"],
        )


def test_postgres_operator_frame_repository_marks_cancelled_frames() -> None:
    try:
        conn = ensure_postgres_available(env=_TEST_ENV)
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for operator frame repository integration test: "
            f"{exc.reason_code}"
        )

    suffix = _unique_suffix()
    authority = _seed_workflow_run(conn, suffix=suffix)
    repository = PostgresOperatorFrameRepository(conn)
    frame_id = f"operator_frame.cancelled.{suffix}"

    try:
        repository.create_frame(
            operator_frame_id=frame_id,
            run_id=authority["run_id"],
            node_id="foreach",
            operator_kind="foreach",
            item_index=0,
            iteration_index=None,
            source_snapshot={"loop_item": "alpha"},
            active_count=0,
        )
        repository.mark_running(operator_frame_id=frame_id, active_count=1)
        cancelled = repository.mark_cancelled(
            operator_frame_id=frame_id,
            aggregate_outputs={"loop_item": "alpha"},
            stop_reason="workflow_cancelled",
            active_count=0,
        )

        assert cancelled.frame_state == "cancelled"
        assert cancelled.stop_reason == "workflow_cancelled"
        assert cancelled.finished_at is not None
        assert repository.list_for_node(run_id=authority["run_id"], node_id="foreach")[0] == cancelled
    finally:
        conn.execute("DELETE FROM run_operator_frames WHERE run_id = $1", authority["run_id"])
        conn.execute("DELETE FROM workflow_runs WHERE run_id = $1", authority["run_id"])
        conn.execute(
            "DELETE FROM admission_decisions WHERE admission_decision_id = $1",
            authority["admission_decision_id"],
        )
        conn.execute(
            "DELETE FROM workflow_definitions WHERE workflow_definition_id = $1",
            authority["workflow_definition_id"],
        )


def test_postgres_evidence_execution_persists_foreach_operator_frames() -> None:
    suffix = _unique_suffix()
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id=f"workflow.foreach.persisted.{suffix}",
        request_id=f"request.foreach.persisted.{suffix}",
        workflow_definition_id=f"workflow_definition.foreach.persisted.{suffix}.v1",
        definition_hash=f"sha256:foreach-persisted:{suffix}",
        workspace_ref=f"workspace.{suffix}",
        runtime_profile_ref=f"runtime_profile.{suffix}",
        nodes=(
            _node(
                suffix=suffix,
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["alpha", "beta"]},
            ),
            _node(
                suffix=suffix,
                node_id="foreach",
                position_index=1,
                display_name="foreach",
                adapter_type="control_operator",
                inputs={
                    "task_name": "foreach",
                    "operator": {
                        "kind": "foreach",
                        "source_ref": {"from_node_id": "source", "output_key": "items"},
                        "max_items": 10,
                        "max_parallel": 2,
                        "aggregate_mode": "ordered_results",
                        "result_key": "results",
                    },
                },
            ),
            _node(
                suffix=suffix,
                node_id="foreach_body",
                position_index=2,
                display_name="foreach_body",
                adapter_type="api_task",
                inputs={"task_name": "foreach_body", "input_payload": {}},
                template_owner_node_id="foreach",
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_source_foreach",
                from_node_id="source",
                to_node_id="foreach",
                position_index=0,
            ),
        ),
    )
    planner = WorkflowIntakePlanner(registry=_resolver(suffix=suffix))
    outcome = planner.plan(request=request)
    requested_at = outcome.admission_decision.decided_at
    submission = _submission_from_outcome(outcome=outcome, requested_at=requested_at)
    registry = AdapterRegistry()
    registry.register("api_task", LoopEchoAdapter(sleep_s=0.02))

    sync_conn = ensure_postgres_available(env=_TEST_ENV)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async_conn = loop.run_until_complete(connect_workflow_database(env=_TEST_ENV))
    writer = TriggerablePostgresEvidenceWriter(database_url=_TEST_ENV["WORKFLOW_DATABASE_URL"])
    try:
        loop.run_until_complete(bootstrap_control_plane_schema(async_conn))
        loop.run_until_complete(persist_workflow_admission(async_conn, submission=submission))

        result = RuntimeOrchestrator(adapter_registry=registry).execute_deterministic_path(
            intake_outcome=outcome,
            evidence_writer=writer,
        )

        rows = sync_conn.execute(
            """
            SELECT
                operator_frame_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                stop_reason
            FROM run_operator_frames
            WHERE run_id = $1
            ORDER BY item_index, operator_frame_id
            """,
            outcome.run_id,
        )

        assert result.current_state.value == "succeeded"
        assert len(rows) == 2
        assert [row["frame_state"] for row in rows] == ["succeeded", "succeeded"]
        assert [row["item_index"] for row in rows] == [0, 1]
        assert [_json_value(row["source_snapshot"])["item"] for row in rows] == ["alpha", "beta"]
        assert [_json_value(row["aggregate_outputs"])["item"] for row in rows] == ["alpha", "beta"]
        assert {row["stop_reason"] for row in rows} == {"completed"}

        inspection = RuntimeOrchestrator(
            evidence_reader=PostgresEvidenceReader(env=_TEST_ENV),
        ).inspect_run(run_id=outcome.run_id)
        replay = RuntimeOrchestrator(
            evidence_reader=PostgresEvidenceReader(env=_TEST_ENV),
        ).replay_run(run_id=outcome.run_id)
        assert inspection.operator_frame_source == "canonical_operator_frames"
        assert len(inspection.operator_frames) == 2
        assert [frame.node_id for frame in inspection.operator_frames] == ["foreach", "foreach"]
        assert [frame.item_index for frame in inspection.operator_frames] == [0, 1]
        assert [frame.frame_state for frame in inspection.operator_frames] == [
            "succeeded",
            "succeeded",
        ]
        assert [frame.source_snapshot["item"] for frame in inspection.operator_frames] == [
            "alpha",
            "beta",
        ]
        assert [frame.aggregate_outputs["item"] for frame in inspection.operator_frames] == [
            "alpha",
            "beta",
        ]
        assert replay.operator_frame_source == "canonical_operator_frames"
        assert len(replay.operator_frames) == 2
        assert [frame.item_index for frame in replay.operator_frames] == [0, 1]
    finally:
        try:
            writer._bridge.run(writer.close())
        finally:
            writer._bridge.close()
        loop.run_until_complete(async_conn.close())
        loop.close()
        asyncio.set_event_loop(None)
        sync_conn.execute("DELETE FROM run_operator_frames WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM receipts WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM workflow_events WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM workflow_outbox WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM workflow_runs WHERE run_id = $1", outcome.run_id)
        sync_conn.execute(
            "DELETE FROM admission_decisions WHERE admission_decision_id = $1",
            outcome.admission_decision.admission_decision_id,
        )
        sync_conn.execute(
            "DELETE FROM workflow_definition_edges WHERE workflow_definition_id = $1",
            outcome.admitted_definition_ref or request.workflow_definition_id,
        )
        sync_conn.execute(
            "DELETE FROM workflow_definition_nodes WHERE workflow_definition_id = $1",
            outcome.admitted_definition_ref or request.workflow_definition_id,
        )
        sync_conn.execute(
            "DELETE FROM workflow_definitions WHERE workflow_definition_id = $1",
            outcome.admitted_definition_ref or request.workflow_definition_id,
        )


def test_postgres_evidence_execution_cancels_foreach_run_at_batch_boundary() -> None:
    suffix = _unique_suffix()
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id=f"workflow.foreach.cancelled.{suffix}",
        request_id=f"request.foreach.cancelled.{suffix}",
        workflow_definition_id=f"workflow_definition.foreach.cancelled.{suffix}.v1",
        definition_hash=f"sha256:foreach-cancelled:{suffix}",
        workspace_ref=f"workspace.{suffix}",
        runtime_profile_ref=f"runtime_profile.{suffix}",
        nodes=(
            _node(
                suffix=suffix,
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["alpha", "beta", "gamma"]},
            ),
            _node(
                suffix=suffix,
                node_id="foreach",
                position_index=1,
                display_name="foreach",
                adapter_type="control_operator",
                inputs={
                    "task_name": "foreach",
                    "operator": {
                        "kind": "foreach",
                        "source_ref": {"from_node_id": "source", "output_key": "items"},
                        "max_items": 10,
                        "max_parallel": 1,
                        "aggregate_mode": "ordered_results",
                        "result_key": "results",
                    },
                },
            ),
            _node(
                suffix=suffix,
                node_id="after",
                position_index=2,
                display_name="after",
                expected_outputs={"done": True},
            ),
            _node(
                suffix=suffix,
                node_id="foreach_body",
                position_index=3,
                display_name="foreach_body",
                adapter_type="api_task",
                inputs={"task_name": "foreach_body", "input_payload": {}},
                template_owner_node_id="foreach",
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_source_foreach",
                from_node_id="source",
                to_node_id="foreach",
                position_index=0,
            ),
            _edge(
                edge_id="edge_foreach_after",
                from_node_id="foreach",
                to_node_id="after",
                position_index=1,
            ),
        ),
    )
    planner = WorkflowIntakePlanner(registry=_resolver(suffix=suffix))
    outcome = planner.plan(request=request)
    requested_at = outcome.admission_decision.decided_at
    submission = _submission_from_outcome(outcome=outcome, requested_at=requested_at)
    registry = AdapterRegistry()
    writer = TriggerablePostgresEvidenceWriter(database_url=_TEST_ENV["WORKFLOW_DATABASE_URL"])
    registry.register(
        "api_task",
        CancelAfterFirstItemAdapter(cancel_once=lambda: writer.request_cancel(outcome.run_id)),
    )

    sync_conn = ensure_postgres_available(env=_TEST_ENV)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async_conn = loop.run_until_complete(connect_workflow_database(env=_TEST_ENV))
    try:
        loop.run_until_complete(bootstrap_control_plane_schema(async_conn))
        loop.run_until_complete(persist_workflow_admission(async_conn, submission=submission))

        result = RuntimeOrchestrator(adapter_registry=registry).execute_deterministic_path(
            intake_outcome=outcome,
            evidence_writer=writer,
        )

        run_row = sync_conn.execute(
            """
            SELECT current_state, terminal_reason_code, finished_at
            FROM workflow_runs
            WHERE run_id = $1
            """,
            outcome.run_id,
        )[0]
        frame_rows = sync_conn.execute(
            """
            SELECT frame_state, item_index, stop_reason
            FROM run_operator_frames
            WHERE run_id = $1
            ORDER BY item_index, operator_frame_id
            """,
            outcome.run_id,
        )
        receipt_rows = sync_conn.execute(
            """
            SELECT receipt_type, node_id, status, failure_code, outputs
            FROM receipts
            WHERE run_id = $1
            ORDER BY evidence_seq
            """,
            outcome.run_id,
        )

        assert result.current_state.value == "cancelled"
        assert result.terminal_reason_code == "workflow_cancelled"
        assert run_row["current_state"] == "cancelled"
        assert run_row["terminal_reason_code"] == "workflow_cancelled"
        assert run_row["finished_at"] is not None
        assert [row["frame_state"] for row in frame_rows] == ["succeeded"]
        assert [row["item_index"] for row in frame_rows] == [0]
        assert {row["stop_reason"] for row in frame_rows} == {"completed"}
        foreach_receipt = next(
            row
            for row in receipt_rows
            if row["node_id"] == "foreach" and row["receipt_type"] == "node_execution_receipt"
        )
        assert foreach_receipt["status"] == "cancelled"
        assert foreach_receipt["failure_code"] == "workflow_cancelled"
        assert _json_value(foreach_receipt["outputs"])["items_processed"] == 1
        assert _json_value(foreach_receipt["outputs"])["stop_reason"] == "workflow_cancelled"
        assert any(row["receipt_type"] == "workflow_cancelled_receipt" for row in receipt_rows)

        inspection = RuntimeOrchestrator(
            evidence_reader=PostgresEvidenceReader(env=_TEST_ENV),
        ).inspect_run(run_id=outcome.run_id)
        assert inspection.current_state == "cancelled"
        assert inspection.terminal_reason == "workflow_cancelled"
        assert inspection.operator_frame_source == "canonical_operator_frames"
        assert len(inspection.operator_frames) == 1
        assert inspection.operator_frames[0].frame_state == "succeeded"
        assert inspection.operator_frames[0].item_index == 0
    finally:
        try:
            writer._bridge.run(writer.close())
        finally:
            writer._bridge.close()
        loop.run_until_complete(async_conn.close())
        loop.close()
        asyncio.set_event_loop(None)
        sync_conn.execute("DELETE FROM run_operator_frames WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM receipts WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM workflow_events WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM workflow_outbox WHERE run_id = $1", outcome.run_id)
        sync_conn.execute("DELETE FROM workflow_runs WHERE run_id = $1", outcome.run_id)
        sync_conn.execute(
            "DELETE FROM admission_decisions WHERE admission_decision_id = $1",
            outcome.admission_decision.admission_decision_id,
        )
        sync_conn.execute(
            "DELETE FROM workflow_definition_edges WHERE workflow_definition_id = $1",
            outcome.admitted_definition_ref or request.workflow_definition_id,
        )
        sync_conn.execute(
            "DELETE FROM workflow_definition_nodes WHERE workflow_definition_id = $1",
            outcome.admitted_definition_ref or request.workflow_definition_id,
        )
        sync_conn.execute(
            "DELETE FROM workflow_definitions WHERE workflow_definition_id = $1",
            outcome.admitted_definition_ref or request.workflow_definition_id,
        )
