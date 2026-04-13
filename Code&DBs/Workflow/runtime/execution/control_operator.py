"""Control operator execution: foreach, batch, repeat_until, while."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

from adapters import build_transition_proof
from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest

from ..control_operator_frames import OperatorFrameRepository, RunOperatorFrame
from ..domain import RunState
from ..intake import WorkflowIntakeOutcome
from .dependency import _ExecutionCursor, _FailureReason
from .evidence import _event_id, _now, _receipt_id, _release_refs
from .records import (
    NODE_EXECUTION_RECEIPT_TYPE,
    NODE_FAILED_EVENT_TYPE,
    NODE_CANCELLED_EVENT_TYPE,
    NODE_SUCCEEDED_EVENT_TYPE,
    NODE_START_RECEIPT_TYPE,
    NODE_STARTED_EVENT_TYPE,
    NodeExecutionRecord,
)
from .template_cloning import (
    _clone_template_graph,
    _operator_frame_payload,
    _template_graph,
    _template_terminal_node_id,
)

# Forward declaration — import string avoids circular dep
_ExecuteGraphFn = Callable[..., "_FailureReason | None"]
_TransitionProofWriter = Any  # Protocol defined in orchestrator; use Any here


def _current_run_state_value(
    *,
    run_id: str,
    run_state_reader: Any,
) -> str | None:
    if run_state_reader is None:
        return None
    state = run_state_reader.current_state_for_run(run_id)
    if state is None:
        return None
    normalized = str(state).strip().lower()
    return normalized or None


def _inbound_edges_map(
    edges: Sequence[WorkflowEdgeContract],
) -> dict[str, list[WorkflowEdgeContract]]:
    inbound: dict[str, list[WorkflowEdgeContract]] = {}
    for edge in edges:
        inbound.setdefault(edge.to_node_id, []).append(edge)
    for edge_rows in inbound.values():
        edge_rows.sort(key=lambda item: (item.position_index, item.edge_id))
    return inbound


def _predicate_matches(
    outputs: Mapping[str, Any],
    predicate: Mapping[str, Any],
) -> bool:
    from runtime.condition_evaluator import evaluate_condition_tree

    try:
        return bool(evaluate_condition_tree(dict(outputs), dict(predicate)))
    except Exception:
        return False


def _cancel_open_operator_frames(
    *,
    operator_frame_repository: OperatorFrameRepository,
    run_id: str,
    node_id: str | None = None,
    stop_reason: str,
) -> tuple[dict[str, Any], ...]:
    if node_id is None:
        frames = operator_frame_repository.list_for_run(run_id=run_id)
    else:
        frames = operator_frame_repository.list_for_node(run_id=run_id, node_id=node_id)
    cancelled_payload: list[dict[str, Any]] = []
    for frame in frames:
        if frame.frame_state not in {"created", "running"}:
            continue
        cancelled = operator_frame_repository.mark_cancelled(
            operator_frame_id=frame.operator_frame_id,
            aggregate_outputs=dict(frame.aggregate_outputs),
            stop_reason=stop_reason,
            active_count=0,
        )
        cancelled_payload.append(_operator_frame_payload(cancelled))
    return tuple(cancelled_payload)


def _complete_control_operator_record(
    *,
    node: WorkflowNodeContract,
    writer: _TransitionProofWriter,
    cursor: _ExecutionCursor,
    intake_outcome: WorkflowIntakeOutcome,
    started_at: datetime,
    start_receipt_id: str,
    operator_kind: str,
    operator: Mapping[str, Any],
    release_refs: Sequence[Mapping[str, str]],
    status: str,
    reason_code: str,
    operator_outputs: Mapping[str, Any],
    frames_payload: Sequence[Mapping[str, Any]],
    reason_details: Mapping[str, Any] | None = None,
) -> NodeExecutionRecord:
    finished_at = _now()
    if status == "succeeded":
        event_type = NODE_SUCCEEDED_EVENT_TYPE
        failure_code = None
    elif status == "cancelled":
        event_type = NODE_CANCELLED_EVENT_TYPE
        failure_code = reason_code
    else:
        event_type = NODE_FAILED_EVENT_TYPE
        failure_code = reason_code

    outputs_payload = {
        **dict(operator_outputs),
        "operator_frames": list(frames_payload),
    }
    if reason_details is not None:
        detail_key = "cancel_reason" if status == "cancelled" else "failure_reason"
        outputs_payload[detail_key] = dict(reason_details)

    terminal_proof = build_transition_proof(
        route_identity=cursor.identity_for_current_transition(),
        transition_seq=cursor.transition_seq,
        event_id=_event_id(
            run_id=intake_outcome.run_id,
            evidence_seq=cursor.next_evidence_seq,
        ),
        receipt_id=_receipt_id(
            run_id=intake_outcome.run_id,
            evidence_seq=cursor.next_evidence_seq + 1,
        ),
        event_type=event_type,
        receipt_type=NODE_EXECUTION_RECEIPT_TYPE,
        reason_code=reason_code,
        evidence_seq=cursor.next_evidence_seq,
        occurred_at=finished_at,
        started_at=started_at,
        finished_at=finished_at,
        executor_type="runtime.control_operator",
        status=status,
        payload={
            "node_id": node.node_id,
            "task_name": node.display_name,
            "operator_kind": operator_kind,
            "dependency_receipts": tuple(release_refs),
            "operator_frames": list(frames_payload),
        },
        inputs={
            "task_name": node.display_name,
            "operator": dict(operator),
            "input_payload": dict(node.inputs),
        },
        outputs=outputs_payload,
        node_id=node.node_id,
        failure_code=failure_code,
        causation_id=_receipt_id(
            run_id=intake_outcome.run_id,
            evidence_seq=cursor.next_evidence_seq - 1,
        ),
    )
    completion_result = writer.append_transition_proof(terminal_proof)
    cursor.advance(result=completion_result)
    return NodeExecutionRecord(
        node_id=node.node_id,
        task_name=node.display_name,
        status=status,
        outputs=outputs_payload,
        started_at=started_at,
        finished_at=finished_at,
        start_receipt_id=start_receipt_id,
        completion_receipt_id=completion_result.receipt_id,
        failure_code=failure_code,
    )


def execute_control_operator(
    *,
    node: WorkflowNodeContract,
    operator_frame_repository: OperatorFrameRepository,
    run_state_reader: Any,
    request: WorkflowRequest,
    intake_outcome: WorkflowIntakeOutcome,
    writer: _TransitionProofWriter,
    cursor: _ExecutionCursor,
    inbound_edges: Mapping[str, Sequence[WorkflowEdgeContract]],
    completed_nodes: Mapping[str, NodeExecutionRecord],
    execution_boundary_ref: str,
    max_parallel_nodes: int,
    execute_graph_fn: _ExecuteGraphFn,
) -> tuple[list[NodeExecutionRecord], NodeExecutionRecord]:
    operator = dict(node.inputs.get("operator") or {})
    operator_kind = str(operator.get("kind") or "").strip()
    release_refs = _release_refs(
        inbound_edges=inbound_edges.get(node.node_id, ()),
        completed_nodes=completed_nodes,
    )
    started_at = _now()
    start_proof = build_transition_proof(
        route_identity=cursor.identity_for_current_transition(),
        transition_seq=cursor.transition_seq,
        event_id=_event_id(
            run_id=intake_outcome.run_id,
            evidence_seq=cursor.next_evidence_seq,
        ),
        receipt_id=_receipt_id(
            run_id=intake_outcome.run_id,
            evidence_seq=cursor.next_evidence_seq + 1,
        ),
        event_type=NODE_STARTED_EVENT_TYPE,
        receipt_type=NODE_START_RECEIPT_TYPE,
        reason_code="runtime.node_started",
        evidence_seq=cursor.next_evidence_seq,
        occurred_at=started_at,
        started_at=started_at,
        finished_at=started_at,
        executor_type="runtime.control_operator",
        status="running",
        payload={
            "node_id": node.node_id,
            "task_name": node.display_name,
            "dependency_receipts": release_refs,
            "operator_kind": operator_kind,
        },
        inputs={
            "task_name": node.display_name,
            "operator": operator,
            "input_payload": dict(node.inputs),
        },
        outputs={"node_id": node.node_id, "status": "running"},
        node_id=node.node_id,
    )
    start_result = writer.append_transition_proof(start_proof)
    cursor.advance(result=start_result)

    template_nodes, template_edges = _template_graph(request, operator_node_id=node.node_id)
    template_terminal_id = _template_terminal_node_id(template_nodes, template_edges)
    child_records: list[NodeExecutionRecord] = []
    frames_payload: list[dict[str, Any]] = []
    failure_code: str | None = None
    failure_details: dict[str, Any] | None = None
    operator_outputs: dict[str, Any] = {}

    def _cancel_and_return(*, result_key: str, count_key: str, count: int, items: list[Any]) -> tuple[list[NodeExecutionRecord], NodeExecutionRecord]:
        frames_payload.extend(
            _cancel_open_operator_frames(
                operator_frame_repository=operator_frame_repository,
                run_id=intake_outcome.run_id,
                node_id=node.node_id,
                stop_reason="workflow_cancelled",
            )
        )
        return child_records, _complete_control_operator_record(
            node=node,
            writer=writer,
            cursor=cursor,
            intake_outcome=intake_outcome,
            started_at=started_at,
            start_receipt_id=start_result.receipt_id,
            operator_kind=operator_kind,
            operator=operator,
            release_refs=release_refs,
            status="cancelled",
            reason_code="workflow_cancelled",
            operator_outputs={
                result_key: items,
                "operator_kind": operator_kind,
                count_key: count,
                "stop_reason": "workflow_cancelled",
            },
            frames_payload=frames_payload,
            reason_details={
                "node_id": node.node_id,
                count_key: count,
            },
        )

    if not template_nodes or template_terminal_id is None:
        failure_code = "runtime.control_operator_template_missing"
        failure_details = {"node_id": node.node_id, "operator_kind": operator_kind}
    elif operator_kind == "foreach":
        source_ref = operator.get("source_ref")
        source_node_id = str(source_ref.get("from_node_id") or "") if isinstance(source_ref, Mapping) else ""
        output_key = str(source_ref.get("output_key") or "") if isinstance(source_ref, Mapping) else ""
        source_record = completed_nodes.get(source_node_id)
        items = source_record.outputs.get(output_key) if source_record else None
        if not isinstance(items, list):
            failure_code = "runtime.control_operator_invalid_source"
            failure_details = {
                "node_id": node.node_id,
                "source_node_id": source_node_id,
                "output_key": output_key,
            }
        else:
            max_items = int(operator.get("max_items") or len(items))
            max_parallel = int(operator.get("max_parallel") or 1)
            result_key = str(operator.get("result_key") or "results")
            ordered_results: list[Any] = []
            capped_items = list(items[:max_items])
            for batch_start in range(0, len(capped_items), max_parallel):
                if (
                    _current_run_state_value(
                        run_id=intake_outcome.run_id,
                        run_state_reader=run_state_reader,
                    )
                    == RunState.CANCELLED.value
                ):
                    return _cancel_and_return(
                        result_key=result_key,
                        count_key="items_processed",
                        count=len(ordered_results),
                        items=ordered_results,
                    )
                batch_items = capped_items[batch_start: batch_start + max_parallel]
                batch_meta: list[tuple[int, RunOperatorFrame, str | None]] = []
                batch_pending_nodes: dict[str, WorkflowNodeContract] = {}
                batch_edges: list[WorkflowEdgeContract] = []
                for offset, item in enumerate(batch_items):
                    item_index = batch_start + offset
                    frame_id = f"{start_result.receipt_id}:item:{item_index}"
                    frame = operator_frame_repository.create_frame(
                        operator_frame_id=frame_id,
                        run_id=intake_outcome.run_id,
                        node_id=node.node_id,
                        operator_kind=operator_kind,
                        item_index=item_index,
                        iteration_index=None,
                        source_snapshot={"item": item},
                        active_count=len(batch_items),
                    )
                    operator_frame_repository.mark_running(
                        operator_frame_id=frame.operator_frame_id,
                        active_count=len(batch_items),
                    )
                    cloned_nodes, cloned_edges, cloned_terminal_id = _clone_template_graph(
                        operator_node_id=node.node_id,
                        frame=frame,
                        template_nodes=template_nodes,
                        template_edges=template_edges,
                        injected_payload={"loop_item": item, "loop_item_index": item_index},
                    )
                    batch_pending_nodes.update(cloned_nodes)
                    batch_edges.extend(cloned_edges)
                    batch_meta.append((item_index, frame, cloned_terminal_id))
                local_results: list[NodeExecutionRecord] = []
                local_completed: dict[str, NodeExecutionRecord] = {}
                local_order: list[str] = []
                failure = execute_graph_fn(
                    request=request,
                    intake_outcome=intake_outcome,
                    operator_frame_repository=operator_frame_repository,
                    run_state_reader=run_state_reader,
                    writer=writer,
                    cursor=cursor,
                    pending_nodes=batch_pending_nodes,
                    inbound_edges=_inbound_edges_map(tuple(batch_edges)),
                    completed_nodes=local_completed,
                    node_results=local_results,
                    execution_order=local_order,
                    execution_boundary_ref=execution_boundary_ref,
                    max_parallel_nodes=max(1, min(max_parallel_nodes, max_parallel)),
                    context_accumulator=None,
                )
                child_records.extend(local_results)
                for item_index, frame, cloned_terminal_id in batch_meta:
                    terminal_record = (
                        local_completed.get(cloned_terminal_id)
                        if cloned_terminal_id is not None
                        else None
                    )
                    if terminal_record is None or terminal_record.status != "succeeded":
                        if failure_code is None:
                            failure_code = (
                                failure.reason_code
                                if failure is not None
                                else "runtime.control_operator_frame_failed"
                            )
                            failure_details = dict((failure.details if failure is not None else {}) or {})
                            failure_details.update(
                                {
                                    "node_id": node.node_id,
                                    "operator_frame_id": frame.operator_frame_id,
                                    "item_index": item_index,
                                }
                            )
                        failed_frame = operator_frame_repository.mark_failed(
                            operator_frame_id=frame.operator_frame_id,
                            aggregate_outputs=dict(terminal_record.outputs) if terminal_record else {},
                            stop_reason=failure_code,
                            active_count=0,
                        )
                        frames_payload.append(_operator_frame_payload(failed_frame))
                        continue
                    ordered_results.append(dict(terminal_record.outputs))
                    succeeded_frame = operator_frame_repository.mark_succeeded(
                        operator_frame_id=frame.operator_frame_id,
                        aggregate_outputs=dict(terminal_record.outputs),
                        stop_reason="completed",
                        active_count=0,
                    )
                    frames_payload.append(_operator_frame_payload(succeeded_frame))
                if failure_code is not None:
                    break
            if failure_code is None:
                operator_outputs = {
                    result_key: ordered_results,
                    "operator_kind": operator_kind,
                    "operator_frames": frames_payload,
                    "items_processed": len(ordered_results),
                    "stop_reason": "completed",
                }
    elif operator_kind == "batch":
        source_ref = operator.get("source_ref")
        source_node_id = str(source_ref.get("from_node_id") or "") if isinstance(source_ref, Mapping) else ""
        output_key = str(source_ref.get("output_key") or "") if isinstance(source_ref, Mapping) else ""
        source_record = completed_nodes.get(source_node_id)
        items = source_record.outputs.get(output_key) if source_record else None
        if not isinstance(items, list):
            failure_code = "runtime.control_operator_invalid_source"
            failure_details = {
                "node_id": node.node_id,
                "source_node_id": source_node_id,
                "output_key": output_key,
            }
        else:
            batch_size = int(operator.get("batch_size") or 0)
            max_batches = int(operator.get("max_batches") or 0)
            max_parallel = int(operator.get("max_parallel") or 1)
            result_key = str(operator.get("result_key") or "results")
            ordered_results: list[Any] = []
            batches = [
                list(items[index: index + batch_size])
                for index in range(0, len(items), batch_size)
            ][:max_batches]
            for batch_start in range(0, len(batches), max_parallel):
                if (
                    _current_run_state_value(
                        run_id=intake_outcome.run_id,
                        run_state_reader=run_state_reader,
                    )
                    == RunState.CANCELLED.value
                ):
                    return _cancel_and_return(
                        result_key=result_key,
                        count_key="batches_processed",
                        count=len(ordered_results),
                        items=ordered_results,
                    )
                batch_group = batches[batch_start: batch_start + max_parallel]
                batch_meta2: list[tuple[int, int, RunOperatorFrame, str | None]] = []
                batch_pending_nodes: dict[str, WorkflowNodeContract] = {}
                batch_edges: list[WorkflowEdgeContract] = []
                for offset, batch_items in enumerate(batch_group):
                    batch_index = batch_start + offset
                    frame_id = f"{start_result.receipt_id}:batch:{batch_index}"
                    frame = operator_frame_repository.create_frame(
                        operator_frame_id=frame_id,
                        run_id=intake_outcome.run_id,
                        node_id=node.node_id,
                        operator_kind=operator_kind,
                        item_index=batch_index,
                        iteration_index=None,
                        source_snapshot={"items": batch_items},
                        active_count=len(batch_group),
                    )
                    operator_frame_repository.mark_running(
                        operator_frame_id=frame.operator_frame_id,
                        active_count=len(batch_group),
                    )
                    cloned_nodes, cloned_edges, cloned_terminal_id = _clone_template_graph(
                        operator_node_id=node.node_id,
                        frame=frame,
                        template_nodes=template_nodes,
                        template_edges=template_edges,
                        injected_payload={
                            "loop_batch": list(batch_items),
                            "loop_batch_index": batch_index,
                            "loop_batch_size": len(batch_items),
                            "loop_batch_start": batch_index * batch_size,
                        },
                    )
                    batch_pending_nodes.update(cloned_nodes)
                    batch_edges.extend(cloned_edges)
                    batch_meta2.append((batch_index, len(batch_items), frame, cloned_terminal_id))
                local_results: list[NodeExecutionRecord] = []
                local_completed: dict[str, NodeExecutionRecord] = {}
                local_order: list[str] = []
                failure = execute_graph_fn(
                    request=request,
                    intake_outcome=intake_outcome,
                    operator_frame_repository=operator_frame_repository,
                    run_state_reader=run_state_reader,
                    writer=writer,
                    cursor=cursor,
                    pending_nodes=batch_pending_nodes,
                    inbound_edges=_inbound_edges_map(tuple(batch_edges)),
                    completed_nodes=local_completed,
                    node_results=local_results,
                    execution_order=local_order,
                    execution_boundary_ref=execution_boundary_ref,
                    max_parallel_nodes=max(1, min(max_parallel_nodes, max_parallel)),
                    context_accumulator=None,
                )
                child_records.extend(local_results)
                for batch_index, _item_count, frame, cloned_terminal_id in batch_meta2:
                    terminal_record = (
                        local_completed.get(cloned_terminal_id)
                        if cloned_terminal_id is not None
                        else None
                    )
                    if terminal_record is None or terminal_record.status != "succeeded":
                        if failure_code is None:
                            failure_code = (
                                failure.reason_code
                                if failure is not None
                                else "runtime.control_operator_frame_failed"
                            )
                            failure_details = dict((failure.details if failure is not None else {}) or {})
                            failure_details.update(
                                {
                                    "node_id": node.node_id,
                                    "operator_frame_id": frame.operator_frame_id,
                                    "batch_index": batch_index,
                                }
                            )
                        failed_frame = operator_frame_repository.mark_failed(
                            operator_frame_id=frame.operator_frame_id,
                            aggregate_outputs=dict(terminal_record.outputs) if terminal_record else {},
                            stop_reason=failure_code,
                            active_count=0,
                        )
                        frames_payload.append(_operator_frame_payload(failed_frame))
                        continue
                    ordered_results.append(dict(terminal_record.outputs))
                    succeeded_frame = operator_frame_repository.mark_succeeded(
                        operator_frame_id=frame.operator_frame_id,
                        aggregate_outputs=dict(terminal_record.outputs),
                        stop_reason="completed",
                        active_count=0,
                    )
                    frames_payload.append(_operator_frame_payload(succeeded_frame))
                if failure_code is not None:
                    break
            if failure_code is None:
                operator_outputs = {
                    result_key: ordered_results,
                    "operator_kind": operator_kind,
                    "operator_frames": frames_payload,
                    "batches_processed": len(ordered_results),
                    "stop_reason": "completed",
                }
    elif operator_kind in {"repeat_until", "while"}:
        max_iterations = int(operator.get("max_iterations") or 0)
        predicate = dict(operator.get("predicate") or {})
        result_key = str(operator.get("result_key") or "iterations")
        iteration_results: list[Any] = []
        loop_previous: dict[str, Any] | None = None
        stop_reason = "max_iterations_exceeded"
        continue_while_matched = operator_kind == "while"
        for iteration_index in range(max_iterations):
            if (
                _current_run_state_value(
                    run_id=intake_outcome.run_id,
                    run_state_reader=run_state_reader,
                )
                == RunState.CANCELLED.value
            ):
                return _cancel_and_return(
                    result_key=result_key,
                    count_key="iterations_completed",
                    count=len(iteration_results),
                    items=iteration_results,
                )
            frame_id = f"{start_result.receipt_id}:iteration:{iteration_index}"
            frame = operator_frame_repository.create_frame(
                operator_frame_id=frame_id,
                run_id=intake_outcome.run_id,
                node_id=node.node_id,
                operator_kind=operator_kind,
                item_index=None,
                iteration_index=iteration_index,
                source_snapshot={"loop_previous": dict(loop_previous or {})},
                active_count=1,
            )
            operator_frame_repository.mark_running(
                operator_frame_id=frame.operator_frame_id,
                active_count=1,
            )
            cloned_nodes, cloned_edges, cloned_terminal_id = _clone_template_graph(
                operator_node_id=node.node_id,
                frame=frame,
                template_nodes=template_nodes,
                template_edges=template_edges,
                injected_payload={
                    "loop_previous": dict(loop_previous or {}),
                    "loop_iteration_index": iteration_index,
                },
            )
            local_results: list[NodeExecutionRecord] = []
            local_completed: dict[str, NodeExecutionRecord] = {}
            local_order: list[str] = []
            failure = execute_graph_fn(
                request=request,
                intake_outcome=intake_outcome,
                operator_frame_repository=operator_frame_repository,
                run_state_reader=run_state_reader,
                writer=writer,
                cursor=cursor,
                pending_nodes=cloned_nodes,
                inbound_edges=_inbound_edges_map(cloned_edges),
                completed_nodes=local_completed,
                node_results=local_results,
                execution_order=local_order,
                execution_boundary_ref=execution_boundary_ref,
                max_parallel_nodes=max_parallel_nodes,
                context_accumulator=None,
            )
            child_records.extend(local_results)
            if (
                failure is not None
                or cloned_terminal_id is None
                or cloned_terminal_id not in local_completed
                or local_completed[cloned_terminal_id].status != "succeeded"
            ):
                failure_code = (
                    failure.reason_code
                    if failure is not None
                    else "runtime.control_operator_iteration_failed"
                )
                failure_details = dict((failure.details if failure is not None else {}) or {})
                failure_details.update(
                    {
                        "node_id": node.node_id,
                        "operator_frame_id": frame.operator_frame_id,
                        "iteration_index": iteration_index,
                    }
                )
                failed_frame = operator_frame_repository.mark_failed(
                    operator_frame_id=frame.operator_frame_id,
                    stop_reason=failure_code,
                    active_count=0,
                )
                frames_payload.append(_operator_frame_payload(failed_frame))
                break
            terminal_record = local_completed[cloned_terminal_id]
            iteration_output = dict(terminal_record.outputs)
            iteration_results.append(iteration_output)
            loop_previous = iteration_output
            matched = _predicate_matches(iteration_output, predicate)
            if continue_while_matched:
                frame_stop_reason = "predicate_matched" if matched else "predicate_not_matched"
            else:
                frame_stop_reason = "predicate_matched" if matched else "completed_iteration"
            succeeded_frame = operator_frame_repository.mark_succeeded(
                operator_frame_id=frame.operator_frame_id,
                aggregate_outputs=iteration_output,
                stop_reason=frame_stop_reason,
                active_count=0,
            )
            frames_payload.append(_operator_frame_payload(succeeded_frame))
            if continue_while_matched:
                if not matched:
                    stop_reason = "predicate_not_matched"
                    break
            elif matched:
                stop_reason = "predicate_matched"
                break
        expected_stop_reason = "predicate_not_matched" if continue_while_matched else "predicate_matched"
        if failure_code is None and stop_reason != expected_stop_reason:
            failure_code = "runtime.control_operator_max_iterations_exceeded"
            failure_details = {
                "node_id": node.node_id,
                "max_iterations": max_iterations,
            }
        if failure_code is None:
            operator_outputs = {
                result_key: iteration_results,
                "operator_kind": operator_kind,
                "operator_frames": frames_payload,
                "iterations_completed": len(iteration_results),
                "last_iteration_output": dict(loop_previous or {}),
                "stop_reason": stop_reason,
            }
    else:
        failure_code = "runtime.control_operator_kind_unsupported"
        failure_details = {"node_id": node.node_id, "operator_kind": operator_kind}

    return child_records, _complete_control_operator_record(
        node=node,
        writer=writer,
        cursor=cursor,
        intake_outcome=intake_outcome,
        started_at=started_at,
        start_receipt_id=start_result.receipt_id,
        operator_kind=operator_kind,
        operator=operator,
        release_refs=release_refs,
        status="failed" if failure_code else "succeeded",
        reason_code=failure_code or "runtime.node_succeeded",
        operator_outputs=operator_outputs,
        frames_payload=frames_payload,
        reason_details=failure_details if failure_code else None,
    )


__all__ = [
    "_cancel_open_operator_frames",
    "_complete_control_operator_record",
    "_inbound_edges_map",
    "_predicate_matches",
    "execute_control_operator",
]
