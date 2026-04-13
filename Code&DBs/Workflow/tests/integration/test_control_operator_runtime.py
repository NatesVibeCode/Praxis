from __future__ import annotations

from datetime import datetime, timezone
import threading
import time
from typing import Any

from adapters.deterministic import (
    AdapterRegistry,
    DeterministicTaskRequest,
    DeterministicTaskResult,
)
from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from receipts import AppendOnlyWorkflowEvidenceWriter
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime.control_operator_frames import InMemoryOperatorFrameRepository
from runtime import RunState, RuntimeOrchestrator, WorkflowIntakePlanner


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _resolver() -> RegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                ),
            ),
        },
    )


def _node(
    *,
    node_id: str,
    position_index: int,
    display_name: str,
    adapter_type: str = MINIMAL_WORKFLOW_NODE_TYPE,
    inputs: dict[str, Any] | None = None,
    expected_outputs: dict[str, Any] | None = None,
    template_owner_node_id: str | None = None,
) -> WorkflowNodeContract:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
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


def _run_request(
    request: WorkflowRequest,
    *,
    adapter_registry: AdapterRegistry | None = None,
) -> tuple[Any, AppendOnlyWorkflowEvidenceWriter, WorkflowIntakePlanner]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator(adapter_registry=adapter_registry or AdapterRegistry())
    result = orchestrator.execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return result, writer, planner


def _receipt(
    writer: AppendOnlyWorkflowEvidenceWriter,
    run_id: str,
    *,
    node_id: str,
    receipt_type: str = "node_execution_receipt",
):
    return next(
        receipt
        for receipt in writer.receipts(run_id)
        if receipt.node_id == node_id and receipt.receipt_type == receipt_type
    )


def _plain(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return _plain(value.to_dict())
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


class CancellableEvidenceWriter(AppendOnlyWorkflowEvidenceWriter):
    def __init__(self) -> None:
        super().__init__()
        self._states: dict[str, str] = {}
        self._lock = threading.Lock()

    def request_cancel(self, run_id: str) -> None:
        with self._lock:
            self._states[run_id] = RunState.CANCELLED.value

    def current_state_for_run(self, run_id: str) -> str | None:
        with self._lock:
            return self._states.get(run_id)


class CancelOnExecuteAdapter:
    executor_type = "adapter.cancel_once"

    def __init__(
        self,
        *,
        cancel_once,
        outputs: dict[str, Any] | None = None,
    ) -> None:
        self._cancel_once = cancel_once
        self._outputs = dict(outputs or {})
        self._lock = threading.Lock()
        self._cancelled = False

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        started_at = _utc_now()
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
            inputs={"input_payload": dict(request.input_payload)},
            outputs=dict(self._outputs),
            started_at=started_at,
            finished_at=_utc_now(),
        )


class LoopEchoAdapter:
    executor_type = "adapter.loop_echo"

    def __init__(
        self,
        *,
        sleep_s: float = 0.0,
        fail_items: set[Any] | None = None,
    ) -> None:
        self._sleep_s = sleep_s
        self._fail_items = set(fail_items or ())
        self._lock = threading.Lock()
        self._active = 0
        self.max_seen = 0

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        payload = dict(request.input_payload)
        item = payload.get("loop_item")
        started_at = _utc_now()
        with self._lock:
            self._active += 1
            self.max_seen = max(self.max_seen, self._active)
        try:
            if self._sleep_s:
                time.sleep(self._sleep_s)
            if item in self._fail_items:
                return DeterministicTaskResult(
                    node_id=request.node_id,
                    task_name=request.task_name,
                    status="failed",
                    reason_code="adapter.command_failed",
                    executor_type=self.executor_type,
                    inputs={"input_payload": payload},
                    outputs={},
                    started_at=started_at,
                    finished_at=_utc_now(),
                    failure_code="adapter.command_failed",
                )
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs={"input_payload": payload},
                outputs={
                    "item": item,
                    "item_index": payload.get("loop_item_index"),
                },
                started_at=started_at,
                finished_at=_utc_now(),
            )
        finally:
            with self._lock:
                self._active -= 1


class LoopCounterAdapter:
    executor_type = "adapter.loop_counter"

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        payload = dict(request.input_payload)
        previous = payload.get("loop_previous")
        if not isinstance(previous, dict):
            previous = {}
        count = int(previous.get("count", 0)) + 1
        started_at = _utc_now()
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs={"input_payload": payload},
            outputs={
                "count": count,
                "iteration_index": payload.get("loop_iteration_index"),
            },
            started_at=started_at,
            finished_at=_utc_now(),
        )


class WaitForCancellationAdapter:
    executor_type = "adapter.wait_for_cancellation"

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.cancel_observed = threading.Event()

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        started_at = _utc_now()
        self.entered.set()
        control = request.execution_control
        if control is None:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="adapter.execution_control_missing",
                executor_type=self.executor_type,
                inputs={"input_payload": dict(request.input_payload)},
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="adapter.execution_control_missing",
            )
        if not control.wait_for_cancel(timeout=5):
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="adapter.cancel_timeout",
                executor_type=self.executor_type,
                inputs={"input_payload": dict(request.input_payload)},
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="adapter.cancel_timeout",
            )
        self.cancel_observed.set()
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="cancelled",
            reason_code="workflow_cancelled",
            executor_type=self.executor_type,
            inputs={"input_payload": dict(request.input_payload)},
            outputs={"cancel_observed": True},
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code="workflow_cancelled",
        )


def test_if_helper_lowers_and_skips_the_unselected_branch() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.if.runtime",
        request_id="request.if.runtime",
        workflow_definition_id="workflow_definition.if.runtime.v1",
        definition_hash="sha256:if-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"flag": True},
            ),
            _node(
                node_id="route_if",
                position_index=1,
                display_name="route_if",
                adapter_type="control_operator",
                inputs={
                    "task_name": "route_if",
                    "operator": {
                        "kind": "if",
                        "predicate": {"field": "flag", "op": "equals", "value": True},
                    },
                },
            ),
            _node(
                node_id="then_node",
                position_index=2,
                display_name="then_node",
                expected_outputs={"path": "then"},
            ),
            _node(
                node_id="else_node",
                position_index=3,
                display_name="else_node",
                expected_outputs={"path": "else"},
            ),
        ),
        edges=(
            _edge(edge_id="edge_source_if", from_node_id="source", to_node_id="route_if", position_index=0),
            _edge(
                edge_id="edge_if_then",
                from_node_id="route_if",
                to_node_id="then_node",
                position_index=1,
                release_condition={"branch": "then"},
            ),
            _edge(
                edge_id="edge_if_else",
                from_node_id="route_if",
                to_node_id="else_node",
                position_index=2,
                release_condition={"branch": "else"},
            ),
        ),
    )

    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    assert outcome.validation_result.is_valid is True
    assert outcome.validation_result.normalized_request is not None
    assert all(node.node_id != "route_if" for node in outcome.validation_result.normalized_request.nodes)

    writer = AppendOnlyWorkflowEvidenceWriter()
    result = RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    by_id = {node.node_id: node for node in result.node_results}
    assert result.current_state is RunState.SUCCEEDED
    assert by_id["then_node"].status == "succeeded"
    assert by_id["else_node"].status == "skipped"
    assert any(
        row.record.event_type == "node_skipped"
        for row in writer.evidence_timeline(result.run_id)
        if row.kind == "workflow_event"
    )


def test_switch_helper_routes_only_the_selected_case() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.switch.runtime",
        request_id="request.switch.runtime",
        workflow_definition_id="workflow_definition.switch.runtime.v1",
        definition_hash="sha256:switch-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"status": "approved"},
            ),
            _node(
                node_id="route_switch",
                position_index=1,
                display_name="route_switch",
                adapter_type="control_operator",
                inputs={
                    "task_name": "route_switch",
                    "operator": {
                        "kind": "switch",
                        "field": "status",
                        "cases": (
                            {"branch": "approved", "value": "approved"},
                            {"branch": "rejected", "value": "rejected"},
                        ),
                    },
                },
            ),
            _node(
                node_id="approved_node",
                position_index=2,
                display_name="approved_node",
                expected_outputs={"path": "approved"},
            ),
            _node(
                node_id="rejected_node",
                position_index=3,
                display_name="rejected_node",
                expected_outputs={"path": "rejected"},
            ),
        ),
        edges=(
            _edge(edge_id="edge_source_switch", from_node_id="source", to_node_id="route_switch", position_index=0),
            _edge(
                edge_id="edge_switch_approved",
                from_node_id="route_switch",
                to_node_id="approved_node",
                position_index=1,
                release_condition={"branch": "approved"},
            ),
            _edge(
                edge_id="edge_switch_rejected",
                from_node_id="route_switch",
                to_node_id="rejected_node",
                position_index=2,
                release_condition={"branch": "rejected"},
            ),
        ),
    )

    result, writer, _planner = _run_request(request)

    by_id = {node.node_id: node for node in result.node_results}
    assert result.current_state is RunState.SUCCEEDED
    assert by_id["approved_node"].status == "succeeded"
    assert by_id["rejected_node"].status == "skipped"
    assert _receipt(writer, result.run_id, node_id="rejected_node").status == "skipped"


def test_join_all_lowers_to_multi_inbound_dependency_wiring() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.join.runtime",
        request_id="request.join.runtime",
        workflow_definition_id="workflow_definition.join.runtime.v1",
        definition_hash="sha256:join-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="left",
                position_index=0,
                display_name="left",
                expected_outputs={"left": "L"},
            ),
            _node(
                node_id="right",
                position_index=1,
                display_name="right",
                expected_outputs={"right": "R"},
            ),
            _node(
                node_id="join_all",
                position_index=2,
                display_name="join_all",
                adapter_type="control_operator",
                inputs={
                    "task_name": "join_all",
                    "operator": {"kind": "join_all"},
                },
            ),
            _node(
                node_id="sink",
                position_index=3,
                display_name="sink",
                expected_outputs={"done": True},
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_left_join",
                from_node_id="left",
                to_node_id="join_all",
                position_index=0,
                payload_mapping={"left_value": "left"},
            ),
            _edge(
                edge_id="edge_right_join",
                from_node_id="right",
                to_node_id="join_all",
                position_index=1,
                payload_mapping={"right_value": "right"},
            ),
            _edge(
                edge_id="edge_join_sink",
                from_node_id="join_all",
                to_node_id="sink",
                position_index=2,
            ),
        ),
    )

    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    assert outcome.validation_result.is_valid is True
    assert outcome.validation_result.normalized_request is not None
    assert all(node.node_id != "join_all" for node in outcome.validation_result.normalized_request.nodes)

    writer = AppendOnlyWorkflowEvidenceWriter()
    result = RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.SUCCEEDED
    sink_start = _receipt(writer, result.run_id, node_id="sink", receipt_type="node_start_receipt")
    assert sink_start.inputs["dependency_inputs"] == {
        "left_value": "L",
        "right_value": "R",
    }
    sink_event = next(
        row.record
        for row in writer.evidence_timeline(result.run_id)
        if row.kind == "workflow_event"
        and row.record.event_type == "node_started"
        and row.record.node_id == "sink"
    )
    assert len(sink_event.payload["dependency_receipts"]) == 2


def test_mixed_wave_keeps_adjacent_regular_nodes_parallel_when_control_operator_is_ready() -> None:
    regular_adapter = LoopEchoAdapter(sleep_s=0.05)
    registry = AdapterRegistry()
    registry.register("api_task", regular_adapter)
    registry.register("mcp_task", LoopCounterAdapter())
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.mixed_wave.runtime",
        request_id="request.mixed_wave.runtime",
        workflow_definition_id="workflow_definition.mixed_wave.runtime.v1",
        definition_hash="sha256:mixed-wave-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="regular_a",
                position_index=0,
                display_name="regular_a",
                adapter_type="api_task",
                inputs={"task_name": "regular_a", "input_payload": {}},
            ),
            _node(
                node_id="regular_b",
                position_index=1,
                display_name="regular_b",
                adapter_type="api_task",
                inputs={"task_name": "regular_b", "input_payload": {}},
            ),
            _node(
                node_id="repeat_until",
                position_index=2,
                display_name="repeat_until",
                adapter_type="control_operator",
                inputs={
                    "task_name": "repeat_until",
                    "operator": {
                        "kind": "repeat_until",
                        "max_iterations": 1,
                        "predicate": {"field": "count", "op": "equals", "value": 1},
                        "aggregate_mode": "iteration_results",
                        "result_key": "iterations",
                    },
                },
            ),
            _node(
                node_id="repeat_body",
                position_index=3,
                display_name="repeat_body",
                adapter_type="mcp_task",
                inputs={"task_name": "repeat_body", "input_payload": {}},
                template_owner_node_id="repeat_until",
            ),
        ),
        edges=(),
    )

    result, _writer, _planner = _run_request(request, adapter_registry=registry)

    assert result.current_state is RunState.SUCCEEDED
    assert regular_adapter.max_seen == 2


def test_foreach_runs_template_frames_in_order_with_lineage_and_parallel_cap() -> None:
    loop_echo = LoopEchoAdapter(sleep_s=0.05)
    registry = AdapterRegistry()
    registry.register("api_task", loop_echo)
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.foreach.runtime",
        request_id="request.foreach.runtime",
        workflow_definition_id="workflow_definition.foreach.runtime.v1",
        definition_hash="sha256:foreach-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["alpha", "beta", "gamma"]},
            ),
            _node(
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
                node_id="consumer",
                position_index=2,
                display_name="consumer",
                expected_outputs={"done": True},
            ),
            _node(
                node_id="foreach_body",
                position_index=3,
                display_name="foreach_body",
                adapter_type="api_task",
                inputs={"task_name": "foreach_body", "input_payload": {}},
                template_owner_node_id="foreach",
            ),
        ),
        edges=(
            _edge(edge_id="edge_source_foreach", from_node_id="source", to_node_id="foreach", position_index=0),
            _edge(
                edge_id="edge_foreach_consumer",
                from_node_id="foreach",
                to_node_id="consumer",
                position_index=1,
                payload_mapping={"loop_results": "results"},
            ),
        ),
    )

    result, writer, _planner = _run_request(request, adapter_registry=registry)

    by_id = {node.node_id: node for node in result.node_results}
    child_results = [
        node
        for node in result.node_results
        if node.logical_parent_node_id == "foreach_body"
    ]
    assert result.current_state is RunState.SUCCEEDED
    assert by_id["foreach"].outputs["results"] == [
        {"item": "alpha", "item_index": 0},
        {"item": "beta", "item_index": 1},
        {"item": "gamma", "item_index": 2},
    ]
    assert loop_echo.max_seen == 2
    assert len(child_results) == 3
    assert [node.iteration_index for node in child_results] == [0, 1, 2]
    assert all(node.operator_frame_id for node in child_results)
    consumer_start = _receipt(writer, result.run_id, node_id="consumer", receipt_type="node_start_receipt")
    assert _plain(consumer_start.inputs["dependency_inputs"]["loop_results"]) == by_id["foreach"].outputs["results"]


def test_dependency_collisions_fail_closed_instead_of_overwriting_inputs() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.dependency.collision.runtime",
        request_id="request.dependency.collision.runtime",
        workflow_definition_id="workflow_definition.dependency.collision.runtime.v1",
        definition_hash="sha256:dependency-collision-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="left",
                position_index=0,
                display_name="left",
                expected_outputs={"left": "L"},
            ),
            _node(
                node_id="right",
                position_index=1,
                display_name="right",
                expected_outputs={"right": "R"},
            ),
            _node(
                node_id="sink",
                position_index=2,
                display_name="sink",
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_left_sink",
                from_node_id="left",
                to_node_id="sink",
                position_index=0,
                payload_mapping={"shared": "left"},
            ),
            _edge(
                edge_id="edge_right_sink",
                from_node_id="right",
                to_node_id="sink",
                position_index=1,
                payload_mapping={"shared": "right"},
            ),
        ),
    )

    result, writer, _planner = _run_request(request)

    assert result.current_state is RunState.FAILED
    assert result.terminal_reason_code == "runtime.dependency_target_key_collision"
    assert all(receipt.node_id != "sink" for receipt in writer.receipts(result.run_id))


def test_foreach_fails_closed_when_source_is_not_an_array() -> None:
    registry = AdapterRegistry()
    registry.register("api_task", LoopEchoAdapter())
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.foreach.invalid_source",
        request_id="request.foreach.invalid_source",
        workflow_definition_id="workflow_definition.foreach.invalid_source.v1",
        definition_hash="sha256:foreach-invalid-source",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": "not-a-list"},
            ),
            _node(
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
                node_id="foreach_body",
                position_index=2,
                display_name="foreach_body",
                adapter_type="api_task",
                inputs={"task_name": "foreach_body", "input_payload": {}},
                template_owner_node_id="foreach",
            ),
        ),
        edges=(
            _edge(edge_id="edge_source_foreach", from_node_id="source", to_node_id="foreach", position_index=0),
        ),
    )

    result, writer, _planner = _run_request(request, adapter_registry=registry)

    assert result.current_state is RunState.FAILED
    assert result.terminal_reason_code == "runtime.control_operator_invalid_source"
    assert _receipt(writer, result.run_id, node_id="foreach").failure_code == "runtime.control_operator_invalid_source"


def test_repeat_until_stops_on_predicate_match_and_exports_iteration_results() -> None:
    registry = AdapterRegistry()
    registry.register("api_task", LoopCounterAdapter())
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.repeat.runtime",
        request_id="request.repeat.runtime",
        workflow_definition_id="workflow_definition.repeat.runtime.v1",
        definition_hash="sha256:repeat-runtime",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="repeat_until",
                position_index=0,
                display_name="repeat_until",
                adapter_type="control_operator",
                inputs={
                    "task_name": "repeat_until",
                    "operator": {
                        "kind": "repeat_until",
                        "max_iterations": 5,
                        "predicate": {"field": "count", "op": "equals", "value": 3},
                        "aggregate_mode": "iteration_results",
                        "result_key": "iterations",
                    },
                },
            ),
            _node(
                node_id="consumer",
                position_index=1,
                display_name="consumer",
                expected_outputs={"done": True},
            ),
            _node(
                node_id="repeat_body",
                position_index=2,
                display_name="repeat_body",
                adapter_type="api_task",
                inputs={"task_name": "repeat_body", "input_payload": {}},
                template_owner_node_id="repeat_until",
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_repeat_consumer",
                from_node_id="repeat_until",
                to_node_id="consumer",
                position_index=0,
                payload_mapping={"iteration_results": "iterations"},
            ),
        ),
    )

    result, writer, _planner = _run_request(request, adapter_registry=registry)

    by_id = {node.node_id: node for node in result.node_results}
    child_results = [
        node
        for node in result.node_results
        if node.logical_parent_node_id == "repeat_body"
    ]
    assert result.current_state is RunState.SUCCEEDED
    assert by_id["repeat_until"].outputs["iterations"] == [
        {"count": 1, "iteration_index": 0},
        {"count": 2, "iteration_index": 1},
        {"count": 3, "iteration_index": 2},
    ]
    assert by_id["repeat_until"].outputs["iterations_completed"] == 3
    assert by_id["repeat_until"].outputs["last_iteration_output"] == {
        "count": 3,
        "iteration_index": 2,
    }
    assert by_id["repeat_until"].outputs["stop_reason"] == "predicate_matched"
    assert [node.iteration_index for node in child_results] == [0, 1, 2]
    consumer_start = _receipt(writer, result.run_id, node_id="consumer", receipt_type="node_start_receipt")
    assert _plain(consumer_start.inputs["dependency_inputs"]["iteration_results"]) == by_id["repeat_until"].outputs["iterations"]


def test_repeat_until_fails_closed_when_max_iterations_is_exhausted() -> None:
    registry = AdapterRegistry()
    registry.register("api_task", LoopCounterAdapter())
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.repeat.exhausted",
        request_id="request.repeat.exhausted",
        workflow_definition_id="workflow_definition.repeat.exhausted.v1",
        definition_hash="sha256:repeat-exhausted",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="repeat_until",
                position_index=0,
                display_name="repeat_until",
                adapter_type="control_operator",
                inputs={
                    "task_name": "repeat_until",
                    "operator": {
                        "kind": "repeat_until",
                        "max_iterations": 2,
                        "predicate": {"field": "count", "op": "equals", "value": 5},
                        "aggregate_mode": "iteration_results",
                        "result_key": "iterations",
                    },
                },
            ),
            _node(
                node_id="repeat_body",
                position_index=1,
                display_name="repeat_body",
                adapter_type="api_task",
                inputs={"task_name": "repeat_body", "input_payload": {}},
                template_owner_node_id="repeat_until",
            ),
        ),
        edges=(),
    )

    result, writer, _planner = _run_request(request, adapter_registry=registry)

    repeat_receipt = _receipt(writer, result.run_id, node_id="repeat_until")
    assert result.current_state is RunState.FAILED
    assert result.terminal_reason_code == "runtime.control_operator_max_iterations_exceeded"
    assert repeat_receipt.failure_code == "runtime.control_operator_max_iterations_exceeded"


def test_cancelled_run_stops_before_next_regular_node() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.cancel.regular",
        request_id="request.cancel.regular",
        workflow_definition_id="workflow_definition.cancel.regular.v1",
        definition_hash="sha256:cancel-regular",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="first",
                position_index=0,
                display_name="first",
                adapter_type="api_task",
                inputs={"task_name": "first", "input_payload": {}},
            ),
            _node(
                node_id="second",
                position_index=1,
                display_name="second",
                expected_outputs={"done": True},
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_first_second",
                from_node_id="first",
                to_node_id="second",
                position_index=0,
            ),
        ),
    )

    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    writer = CancellableEvidenceWriter()
    registry = AdapterRegistry()
    registry.register(
        "api_task",
        CancelOnExecuteAdapter(
            cancel_once=lambda: writer.request_cancel(outcome.run_id),
            outputs={"step": "first"},
        ),
    )

    result = RuntimeOrchestrator(adapter_registry=registry).execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.CANCELLED
    assert result.terminal_reason_code == "workflow_cancelled"
    assert [record.node_id for record in result.node_results] == ["first"]
    assert all(receipt.node_id != "second" for receipt in writer.receipts(result.run_id))
    assert writer.receipts(result.run_id)[-1].receipt_type == "workflow_cancelled_receipt"


def test_running_regular_node_receives_inflight_cancel_signal() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.cancel.regular.inflight",
        request_id="request.cancel.regular.inflight",
        workflow_definition_id="workflow_definition.cancel.regular.inflight.v1",
        definition_hash="sha256:cancel-regular-inflight",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="first",
                position_index=0,
                display_name="first",
                adapter_type="api_task",
                inputs={"task_name": "first", "input_payload": {}},
            ),
            _node(
                node_id="second",
                position_index=1,
                display_name="second",
                expected_outputs={"done": True},
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_first_second",
                from_node_id="first",
                to_node_id="second",
                position_index=0,
            ),
        ),
    )

    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    writer = CancellableEvidenceWriter()
    adapter = WaitForCancellationAdapter()
    registry = AdapterRegistry()
    registry.register("api_task", adapter)

    result_box: dict[str, Any] = {}
    error_box: dict[str, Exception] = {}

    def _run() -> None:
        try:
            result_box["result"] = RuntimeOrchestrator(
                adapter_registry=registry,
            ).execute_deterministic_path(
                intake_outcome=outcome,
                evidence_writer=writer,
            )
        except Exception as exc:  # pragma: no cover - thread boundary
            error_box["error"] = exc

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    assert adapter.entered.wait(timeout=2)
    writer.request_cancel(outcome.run_id)
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert "error" not in error_box
    assert adapter.cancel_observed.is_set()

    result = result_box["result"]
    first_receipt = _receipt(writer, result.run_id, node_id="first")

    assert result.current_state is RunState.CANCELLED
    assert result.terminal_reason_code == "workflow_cancelled"
    assert [record.node_id for record in result.node_results] == ["first"]
    assert first_receipt.status == "cancelled"
    assert first_receipt.failure_code == "workflow_cancelled"
    assert all(receipt.node_id != "second" for receipt in writer.receipts(result.run_id))


def test_foreach_cancellation_stops_before_next_batch_and_cancels_operator_node() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.foreach.cancelled",
        request_id="request.foreach.cancelled",
        workflow_definition_id="workflow_definition.foreach.cancelled.v1",
        definition_hash="sha256:foreach-cancelled",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["alpha", "beta", "gamma"]},
            ),
            _node(
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
                node_id="after",
                position_index=2,
                display_name="after",
                expected_outputs={"done": True},
            ),
            _node(
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

    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    writer = CancellableEvidenceWriter()
    registry = AdapterRegistry()
    registry.register(
        "api_task",
        CancelOnExecuteAdapter(
            cancel_once=lambda: writer.request_cancel(outcome.run_id),
            outputs={"item": "alpha", "item_index": 0},
        ),
    )

    result = RuntimeOrchestrator(adapter_registry=registry).execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    child_results = [
        record
        for record in result.node_results
        if record.logical_parent_node_id == "foreach_body"
    ]
    foreach_receipt = _receipt(writer, result.run_id, node_id="foreach")

    assert result.current_state is RunState.CANCELLED
    assert result.terminal_reason_code == "workflow_cancelled"
    assert len(child_results) == 1
    assert child_results[0].iteration_index == 0
    assert foreach_receipt.status == "cancelled"
    assert foreach_receipt.failure_code == "workflow_cancelled"
    assert _plain(foreach_receipt.outputs["results"]) == [{"item": "alpha", "item_index": 0}]
    assert foreach_receipt.outputs["items_processed"] == 1
    assert foreach_receipt.outputs["stop_reason"] == "workflow_cancelled"
    assert all(receipt.node_id != "after" for receipt in writer.receipts(result.run_id))


def test_fresh_execution_clears_stale_operator_frames_before_retrying_same_run_id() -> None:
    registry = AdapterRegistry()
    registry.register("api_task", LoopEchoAdapter())
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.foreach.retry",
        request_id="request.foreach.retry",
        workflow_definition_id="workflow_definition.foreach.retry.v1",
        definition_hash="sha256:foreach-retry",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["alpha", "beta"]},
            ),
            _node(
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
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=request)
    operator_frames = InMemoryOperatorFrameRepository()
    orchestrator = RuntimeOrchestrator(
        adapter_registry=registry,
        operator_frame_repository=operator_frames,
    )

    first_result = orchestrator.execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=AppendOnlyWorkflowEvidenceWriter(),
    )
    assert first_result.current_state is RunState.SUCCEEDED
    assert len(operator_frames.list_for_run(run_id=outcome.run_id)) == 2

    second_result = orchestrator.execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=AppendOnlyWorkflowEvidenceWriter(),
    )
    frames = operator_frames.list_for_run(run_id=outcome.run_id)

    assert second_result.current_state is RunState.SUCCEEDED
    assert len(frames) == 2
    assert [frame.item_index for frame in frames] == [0, 1]
    assert [frame.source_snapshot["item"] for frame in frames] == ["alpha", "beta"]
