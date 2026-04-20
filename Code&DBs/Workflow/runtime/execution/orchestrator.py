"""Deterministic runtime execution for the first runnable workflow slice.

This module handles:
  - Workflow state machine transitions
  - Sequential and parallel node execution
  - Adapter routing and task request building
  - Context accumulation across pipeline steps
  - Dynamic scope resolution from upstream outputs

Dynamic Scope Resolution
------------------------
When a pipeline step produces file paths as output (e.g., a research step),
the next step can automatically resolve those files' dependencies via the
import graph. This is enabled by setting scope_source="upstream" on the
WorkflowStep.

The runtime:
  1. Extracts file references from the completed step's outputs
     (parsed_output, files/paths/write_scope keys, or text patterns)
  2. Calls resolve_scope() from scope_resolver.py to compute read scope
     and context sections
  3. Injects context sections into the next step's input_payload before
     execution
  4. Records resolution evidence in _scope_resolution metadata
  5. Fails upstream-scope steps by default when resolution fails

See extract_file_refs() and the execution loop in execute_deterministic_path()
for implementation details.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import replace
from datetime import datetime
import threading
from typing import Any, Protocol

from adapters import (
    AdapterRegistry,
    AdapterResolutionError,
    DeterministicExecutionControl,
    DeterministicTaskRequest,
    DeterministicTaskResult,
    build_transition_proof,
)
from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest
from observability import inspect_run as build_inspection_view
from observability import replay_run as build_replay_view
from observability.read_models import InspectionReadModel, OperatorFrameReadModel, ReplayReadModel
from policy.domain import AdmissionDecisionKind
from receipts import EvidenceRow
from receipts.evidence import TransitionProofV1

from ..context_accumulator import ContextAccumulator
from ..control_operator_frames import (
    InMemoryOperatorFrameRepository,
    OperatorFrameRepository,
    RunOperatorFrame,
)
from registry.model_context_limits import safe_context_budget
from ..domain import (
    AtomicEvidenceWriter,
    EvidenceCommitResult,
    LifecycleTransition,
    RouteIdentity,
    RunState,
    RuntimeBoundaryError,
    RuntimeLifecycleError,
    RuntimeOrchestrator as RuntimeOrchestratorContract,
)
from ..intake import WorkflowIntakeOutcome
from .context import extract_file_refs, inject_accumulated_context
from .control_operator import (
    _cancel_open_operator_frames,
    _complete_control_operator_record,
    _inbound_edges_map,
    _predicate_matches,
    execute_control_operator,
)
from .records import (
    CLAIM_REJECTED_EVENT_TYPE,
    CLAIM_VALIDATED_EVENT_TYPE,
    CLAIM_VALIDATION_RECEIPT_TYPE,
    NODE_CANCELLED_EVENT_TYPE,
    NODE_EXECUTION_RECEIPT_TYPE,
    NODE_FAILED_EVENT_TYPE,
    NODE_SKIPPED_EVENT_TYPE,
    NODE_START_RECEIPT_TYPE,
    NODE_STARTED_EVENT_TYPE,
    NODE_SUCCEEDED_EVENT_TYPE,
    NodeExecutionRecord,
    RunExecutionResult,
    WORKFLOW_CANCELLED_EVENT_TYPE,
    WORKFLOW_CANCELLED_RECEIPT_TYPE,
    WORKFLOW_COMPLETION_RECEIPT_TYPE,
    WORKFLOW_FAILED_EVENT_TYPE,
    WORKFLOW_QUEUE_RECEIPT_TYPE,
    WORKFLOW_QUEUED_EVENT_TYPE,
    WORKFLOW_START_RECEIPT_TYPE,
    WORKFLOW_STARTED_EVENT_TYPE,
    WORKFLOW_SUCCEEDED_EVENT_TYPE,
    _RegularNodeStartRecord,
)
from .template_cloning import (
    _clone_template_graph,
    _operator_frame_payload,
    _operator_frame_read_model,
    _template_graph,
    _template_terminal_node_id,
)
from .evidence import (
    ADMISSION_DECISION_SOURCE_TABLE,
    _decision_refs_for_admission,
    _event_id,
    _now,
    _receipt_id,
    _release_refs,
)
from .lineage import _lineage_value, _node_lineage, _with_lineage
from .request_building import (
    _authority_payload_hash,
    _execution_boundary_ref,
    _inject_context_compiler_runtime_metadata,
    _task_request,
    _workflow_request_payload,
)
from .dependency import (
    _DependencyResolution,
    _ExecutionCursor,
    _FailureReason,
    frontier_failure_reason,
    inbound_edges as _inbound_edges,
    node_order as _node_order,
    resolve_dependencies,
)
from .state_machine import validate_transition

class TransitionProofWriter(AtomicEvidenceWriter, Protocol):
    """Evidence writer contract required by deterministic execution."""

    def append_transition_proof(
        self,
        proof: TransitionProofV1,
    ) -> EvidenceCommitResult:
        """Append one event/receipt transition proof."""


class CanonicalEvidenceReader(Protocol):
    """Provides the single canonical evidence slice for one run."""

    def evidence_timeline(self, run_id: str) -> Sequence[EvidenceRow]:
        """Return the canonical evidence rows for one run."""


class RunStateReader(Protocol):
    """Provides canonical workflow_runs state for one run."""

    def current_state_for_run(self, run_id: str) -> str | None:
        """Return the current persisted run state."""


class RunCancellationSignal(Protocol):
    """Provides explicit run-cancellation observation for one run."""

    def cancel_requested(self) -> bool:
        """Return True once the run has been cancelled."""

    def wait_for_cancel(self, timeout: float | None = None) -> bool:
        """Block until cancellation is observed or the timeout elapses."""

    def close(self) -> None:
        """Release any resources bound to the signal."""


def _validate_execution_writer(
    evidence_writer: AtomicEvidenceWriter,
) -> TransitionProofWriter:
    if not hasattr(evidence_writer, "append_transition_proof"):
        raise RuntimeBoundaryError(
            "deterministic execution requires a receipts writer with append_transition_proof()"
        )
    return evidence_writer  # type: ignore[return-value]


def _operator_frame_repository_for_execution(
    *,
    evidence_writer: AtomicEvidenceWriter,
    default_repository: OperatorFrameRepository,
) -> OperatorFrameRepository:
    repository = _bound_operator_frame_repository(evidence_writer)
    return repository or default_repository


def _bound_operator_frame_repository(authority: object) -> OperatorFrameRepository | None:
    loader = getattr(authority, "operator_frame_repository", None)
    if not callable(loader):
        return None
    repository = loader()
    if not hasattr(repository, "list_for_node") or not hasattr(repository, "list_for_run"):
        return None
    return repository  # type: ignore[return-value]


def _run_state_reader_for_execution(
    *,
    evidence_writer: AtomicEvidenceWriter,
    evidence_reader: CanonicalEvidenceReader | None,
) -> RunStateReader | None:
    for authority in (evidence_writer, evidence_reader):
        if authority is None:
            continue
        reader = _bound_run_state_reader(authority)
        if reader is not None:
            return reader
    return None


def _bound_run_state_reader(authority: object) -> RunStateReader | None:
    loader = getattr(authority, "current_state_for_run", None)
    if not callable(loader):
        return None
    return authority  # type: ignore[return-value]


def _bound_run_cancellation_signal(
    authority: object,
    *,
    run_id: str,
) -> RunCancellationSignal | None:
    loader = getattr(authority, "run_cancellation_signal", None)
    if not callable(loader):
        return None
    signal = loader(run_id)
    required_methods = ("cancel_requested", "wait_for_cancel", "close")
    if not all(callable(getattr(signal, method, None)) for method in required_methods):
        return None
    return signal  # type: ignore[return-value]


def _current_run_state(
    *,
    run_id: str,
    run_state_reader: RunStateReader | None,
) -> str | None:
    if run_state_reader is None:
        return None
    state = run_state_reader.current_state_for_run(run_id)
    if state is None:
        return None
    normalized = str(state).strip().lower()
    return normalized or None


class _NullRunCancellationSignal:
    def cancel_requested(self) -> bool:
        return False

    def wait_for_cancel(self, timeout: float | None = None) -> bool:
        if timeout is not None and timeout > 0:
            threading.Event().wait(timeout)
        return False

    def close(self) -> None:
        return


class _PollingRunCancellationSignal:
    """Fallback signal that samples persisted state with bounded backoff."""

    def __init__(
        self,
        *,
        run_id: str,
        run_state_reader: RunStateReader,
        initial_state: str | None,
        min_interval_seconds: float = 0.05,
        max_interval_seconds: float = 0.5,
    ) -> None:
        self._run_id = run_id
        self._run_state_reader = run_state_reader
        self._min_interval_seconds = min_interval_seconds
        self._max_interval_seconds = max_interval_seconds
        self._cancel_event = threading.Event()
        self._close_event = threading.Event()
        if initial_state == RunState.CANCELLED.value:
            self._cancel_event.set()

    def cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    def wait_for_cancel(self, timeout: float | None = None) -> bool:
        if self._cancel_event.is_set():
            return True
        remaining = timeout
        next_interval = self._min_interval_seconds
        while not self._close_event.is_set():
            if self._refresh():
                return True
            wait_interval = next_interval
            if remaining is not None:
                if remaining <= 0:
                    return self._cancel_event.is_set()
                wait_interval = min(wait_interval, remaining)
            if self._close_event.wait(timeout=wait_interval):
                return self._cancel_event.is_set()
            if remaining is not None:
                remaining -= wait_interval
            next_interval = min(next_interval * 2, self._max_interval_seconds)
        return self._cancel_event.is_set()

    def close(self) -> None:
        self._close_event.set()

    def _refresh(self) -> bool:
        if self._cancel_event.is_set():
            return True
        state = _current_run_state(
            run_id=self._run_id,
            run_state_reader=self._run_state_reader,
        )
        if state == RunState.CANCELLED.value:
            self._cancel_event.set()
            return True
        return False


class _RegularWaveCancellationBridge:
    """Requests in-flight node cancellation without polling in the wait loop."""

    def __init__(
        self,
        *,
        cancel_signal: RunCancellationSignal,
        request_cancel: Callable[[], None],
    ) -> None:
        self._cancel_signal = cancel_signal
        self._request_cancel = request_cancel
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="deterministic-wave-cancellation",
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=3)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            if self._cancel_signal.wait_for_cancel(timeout=0.5):
                self._request_cancel()
                return


def _run_cancellation_signal_for_execution(
    *,
    run_id: str,
    evidence_writer: AtomicEvidenceWriter,
    evidence_reader: CanonicalEvidenceReader | None,
    run_state_reader: RunStateReader | None,
) -> RunCancellationSignal:
    for authority in (evidence_writer, evidence_reader):
        if authority is None:
            continue
        signal = _bound_run_cancellation_signal(authority, run_id=run_id)
        if signal is not None:
            return signal
    initial_state = _current_run_state(run_id=run_id, run_state_reader=run_state_reader)
    if run_state_reader is None:
        return _NullRunCancellationSignal()
    return _PollingRunCancellationSignal(
        run_id=run_id,
        run_state_reader=run_state_reader,
        initial_state=initial_state,
    )


def _terminal_reason_for_result(result: DeterministicTaskResult) -> str:
    return result.failure_code or result.reason_code


class RuntimeOrchestrator(RuntimeOrchestratorContract):
    """Runtime authority for the first deterministic execution slice."""

    def __init__(
        self,
        *,
        adapter_registry: AdapterRegistry | None = None,
        evidence_reader: CanonicalEvidenceReader | None = None,
        operator_frame_repository: OperatorFrameRepository | None = None,
    ) -> None:
        self._adapter_registry = adapter_registry or AdapterRegistry()
        self._evidence_reader = evidence_reader
        self._default_operator_frames = operator_frame_repository or InMemoryOperatorFrameRepository()
        self._operator_frames_explicitly_bound = operator_frame_repository is not None

    def submit_run(
        self,
        *,
        route_identity: RouteIdentity,
        admitted_definition_ref: str,
        admitted_definition_hash: str,
        request_payload: Mapping[str, Any],
        evidence_writer: AtomicEvidenceWriter,
    ) -> EvidenceCommitResult:
        submission_identity = replace(route_identity, transition_seq=1)
        return evidence_writer.commit_submission(
            route_identity=submission_identity,
            admitted_definition_ref=admitted_definition_ref,
            admitted_definition_hash=admitted_definition_hash,
            request_payload=request_payload,
        )

    def advance_run(
        self,
        *,
        transition: LifecycleTransition,
        evidence_writer: AtomicEvidenceWriter,
    ) -> EvidenceCommitResult:
        validate_transition(transition)
        return evidence_writer.commit_transition(transition=transition)

    def inspect_run(self, *, run_id: str) -> InspectionReadModel:
        operator_frame_source, operator_frames = self._inspection_operator_frame_snapshot(
            run_id=run_id,
        )
        return build_inspection_view(
            run_id=run_id,
            canonical_evidence=self._canonical_evidence(run_id=run_id),
            operator_frame_source=operator_frame_source,
            operator_frames=operator_frames,
        )

    def replay_run(self, *, run_id: str) -> ReplayReadModel:
        operator_frame_source, operator_frames = self._inspection_operator_frame_snapshot(
            run_id=run_id,
        )
        return build_replay_view(
            run_id=run_id,
            canonical_evidence=self._canonical_evidence(run_id=run_id),
            operator_frame_source=operator_frame_source,
            operator_frames=operator_frames,
        )

    def _prepare_node_for_execution(
        self,
        *,
        node: WorkflowNodeContract,
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        node_results: Sequence[NodeExecutionRecord],
        context_accumulator: ContextAccumulator | None,
    ) -> tuple[WorkflowNodeContract | None, _FailureReason | None]:
        exec_node = node
        scope_resolution_evidence: dict[str, Any] | None = None

        input_payload = node.inputs.get("input_payload")
        if isinstance(input_payload, Mapping):
            scope_source = input_payload.get("scope_source", "none")
            scope_strict = input_payload.get("scope_strict", True)
        else:
            scope_source = node.inputs.get("scope_source", "none")
            scope_strict = node.inputs.get("scope_strict", True)

        if scope_source == "upstream" and node_results:
            upstream_outputs: dict[str, Any] = {}
            for prev_record in reversed(node_results):
                if prev_record.status == "succeeded":
                    upstream_outputs = dict(prev_record.outputs)
                    break

            if upstream_outputs:
                file_refs = extract_file_refs(upstream_outputs)

                if file_refs:
                    from ..scope_resolver import resolve_scope

                    root_dir = "."
                    node_input_payload = node.inputs.get("input_payload")
                    if isinstance(node_input_payload, Mapping):
                        root_dir = node_input_payload.get("root_dir", ".")
                        if not isinstance(root_dir, str):
                            root_dir = "."

                    try:
                        resolution = resolve_scope(
                            write_scope=file_refs,
                            root_dir=root_dir,
                        )
                        scope_resolution_evidence = {
                            "scope_source": scope_source,
                            "upstream_node_ids": tuple(r.node_id for r in node_results),
                            "file_refs_extracted": len(file_refs),
                            "files_resolved": len(resolution.computed_read_scope),
                            "context_sections_added": len(resolution.context_sections),
                            "resolved_files": resolution.computed_read_scope,
                            "resolution_status": "success",
                        }
                        updated_inputs = dict(node.inputs)
                        inner_payload = updated_inputs.get("input_payload")
                        if isinstance(inner_payload, Mapping):
                            inner_payload = dict(inner_payload)
                            existing_sections = list(inner_payload.get("context_sections") or [])
                            existing_sections.extend(resolution.context_sections)
                            inner_payload["context_sections"] = existing_sections
                            inner_payload["_scope_resolution"] = scope_resolution_evidence
                            updated_inputs["input_payload"] = inner_payload
                        else:
                            existing_sections = list(updated_inputs.get("context_sections") or [])
                            existing_sections.extend(resolution.context_sections)
                            updated_inputs["context_sections"] = existing_sections
                            updated_inputs["_scope_resolution"] = scope_resolution_evidence
                        exec_node = replace(node, inputs=updated_inputs)
                    except Exception as exc:  # pragma: no cover - defensive runtime boundary
                        scope_resolution_evidence = {
                            "scope_source": scope_source,
                            "file_refs_extracted": len(file_refs),
                            "resolution_status": "failed",
                            "error": str(exc),
                        }
                        if scope_strict:
                            return None, _FailureReason(
                                reason_code="runtime.scope_resolution_failed",
                                details={
                                    "node_id": node.node_id,
                                    "error": str(exc),
                                    "scope_source": scope_source,
                                    "file_refs_extracted": len(file_refs),
                                },
                            )
                else:
                    scope_resolution_evidence = {
                        "scope_source": scope_source,
                        "file_refs_extracted": 0,
                        "resolution_status": "no_refs_found",
                    }
                    if scope_strict:
                        return None, _FailureReason(
                            reason_code="runtime.scope_no_file_refs",
                            details={
                                "node_id": node.node_id,
                                "scope_source": scope_source,
                            },
                        )

        if context_accumulator:
            exec_node = inject_accumulated_context(exec_node, context_accumulator)

        exec_node = _inject_context_compiler_runtime_metadata(
            node=exec_node,
            intake_outcome=intake_outcome,
            request=request,
        )

        if scope_resolution_evidence is not None:
            updated_inputs = dict(exec_node.inputs)
            inner_payload = updated_inputs.get("input_payload")
            if isinstance(inner_payload, Mapping):
                inner_payload = dict(inner_payload)
                inner_payload["_scope_resolution"] = scope_resolution_evidence
                updated_inputs["input_payload"] = inner_payload
            else:
                updated_inputs["_scope_resolution"] = scope_resolution_evidence
            exec_node = replace(exec_node, inputs=updated_inputs)

        return exec_node, None

    def _emit_skipped_node(
        self,
        *,
        node: WorkflowNodeContract,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        intake_outcome: WorkflowIntakeOutcome,
        inbound_edges: Mapping[str, Sequence[WorkflowEdgeContract]],
        completed_nodes: Mapping[str, NodeExecutionRecord],
        reason_code: str,
        reason_details: Mapping[str, Any] | None = None,
    ) -> NodeExecutionRecord:
        occurred_at = _now()
        release_refs = _release_refs(
            inbound_edges=inbound_edges.get(node.node_id, ()),
            completed_nodes=completed_nodes,
        )
        lineage = _node_lineage(node)
        proof = build_transition_proof(
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
            event_type=NODE_SKIPPED_EVENT_TYPE,
            receipt_type=NODE_EXECUTION_RECEIPT_TYPE,
            reason_code=reason_code,
            evidence_seq=cursor.next_evidence_seq,
            occurred_at=occurred_at,
            started_at=occurred_at,
            finished_at=occurred_at,
            executor_type="runtime.skip",
            status="skipped",
            payload=_with_lineage(
                {
                    "node_id": node.node_id,
                    "task_name": node.display_name,
                    "dependency_receipts": release_refs,
                    "skip_reason": dict(reason_details or {}),
                },
                lineage=lineage,
            ),
            inputs=_with_lineage(
                {
                    "task_name": node.display_name,
                    "input_payload": dict(node.inputs),
                    "dependency_inputs": {},
                    "skip_reason": dict(reason_details or {}),
                },
                lineage=lineage,
            ),
            outputs=_with_lineage(
                {
                    "node_id": node.node_id,
                    "status": "skipped",
                    "skip_reason": dict(reason_details or {}),
                },
                lineage=lineage,
            ),
            node_id=node.node_id,
        )
        result = writer.append_transition_proof(proof)
        cursor.advance(result=result)
        return NodeExecutionRecord(
            node_id=node.node_id,
            task_name=node.display_name,
            status="skipped",
            outputs={"skip_reason": dict(reason_details or {})},
            started_at=occurred_at,
            finished_at=occurred_at,
            start_receipt_id=result.receipt_id,
            completion_receipt_id=result.receipt_id,
            failure_code=None,
            operator_frame_id=_lineage_value(lineage, "operator_frame_id"),
            logical_parent_node_id=_lineage_value(lineage, "logical_parent_node_id"),
            iteration_index=_lineage_value(lineage, "iteration_index"),
        )

    def _start_regular_node(
        self,
        *,
        node: WorkflowNodeContract,
        task_request: DeterministicTaskRequest,
        started_at: datetime,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        intake_outcome: WorkflowIntakeOutcome,
        inbound_edges: Mapping[str, Sequence[WorkflowEdgeContract]],
        completed_nodes: Mapping[str, NodeExecutionRecord],
    ) -> _RegularNodeStartRecord:
        release_refs = _release_refs(
            inbound_edges=inbound_edges.get(node.node_id, ()),
            completed_nodes=completed_nodes,
        )
        lineage = _node_lineage(node)
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
            executor_type="runtime.execute",
            status="running",
            payload=_with_lineage(
                {
                    "node_id": node.node_id,
                    "task_name": task_request.task_name,
                    "release_condition": "after_success",
                    "dependency_receipts": release_refs,
                },
                lineage=lineage,
            ),
            inputs=_with_lineage(
                {
                    "task_name": task_request.task_name,
                    "input_payload": dict(task_request.input_payload),
                    "dependency_inputs": dict(task_request.dependency_inputs),
                    "execution_boundary_ref": task_request.execution_boundary_ref,
                },
                lineage=lineage,
            ),
            outputs=_with_lineage(
                {
                    "node_id": node.node_id,
                    "status": "running",
                },
                lineage=lineage,
            ),
            node_id=node.node_id,
        )
        start_result = writer.append_transition_proof(start_proof)
        cursor.advance(result=start_result)
        return _RegularNodeStartRecord(
            start_receipt_id=start_result.receipt_id,
            release_refs=release_refs,
            lineage=lineage,
        )

    def _complete_regular_node_record(
        self,
        *,
        node: WorkflowNodeContract,
        task_request: DeterministicTaskRequest,
        adapter_result: DeterministicTaskResult,
        start_record: _RegularNodeStartRecord,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        intake_outcome: WorkflowIntakeOutcome,
    ) -> NodeExecutionRecord:
        if adapter_result.status == "succeeded":
            terminal_event_type = NODE_SUCCEEDED_EVENT_TYPE
            terminal_reason_code = "runtime.node_succeeded"
        elif adapter_result.status == "cancelled":
            terminal_event_type = NODE_CANCELLED_EVENT_TYPE
            terminal_reason_code = _terminal_reason_for_result(adapter_result)
        else:
            terminal_event_type = NODE_FAILED_EVENT_TYPE
            terminal_reason_code = _terminal_reason_for_result(adapter_result)
        node_outputs = dict(adapter_result.outputs)
        if adapter_result.status == "succeeded":
            completion_text = node_outputs.get("completion")
            if isinstance(completion_text, str) and completion_text.strip():
                from ..output_parser import parse_json_from_completion

                parsed = parse_json_from_completion(completion_text)
                if parsed is not None:
                    node_outputs["parsed_output"] = parsed

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
            event_type=terminal_event_type,
            receipt_type=NODE_EXECUTION_RECEIPT_TYPE,
            reason_code=terminal_reason_code,
            evidence_seq=cursor.next_evidence_seq,
            occurred_at=adapter_result.finished_at,
            started_at=adapter_result.started_at,
            finished_at=adapter_result.finished_at,
            executor_type=adapter_result.executor_type,
            status=adapter_result.status,
            payload=_with_lineage(
                {
                    "node_id": node.node_id,
                    "task_name": adapter_result.task_name,
                    "release_condition": "after_success",
                    "dependency_receipts": start_record.release_refs,
                    "start_receipt_id": start_record.start_receipt_id,
                },
                lineage=start_record.lineage,
            ),
            inputs=_with_lineage(dict(adapter_result.inputs), lineage=start_record.lineage),
            outputs=_with_lineage(node_outputs, lineage=start_record.lineage),
            node_id=node.node_id,
            failure_code=adapter_result.failure_code,
        )
        completion_result = writer.append_transition_proof(terminal_proof)
        cursor.advance(result=completion_result)
        return NodeExecutionRecord(
            node_id=node.node_id,
            task_name=adapter_result.task_name,
            status=adapter_result.status,
            outputs=node_outputs,
            started_at=adapter_result.started_at,
            finished_at=adapter_result.finished_at,
            start_receipt_id=start_record.start_receipt_id,
            completion_receipt_id=completion_result.receipt_id,
            failure_code=adapter_result.failure_code,
            operator_frame_id=_lineage_value(start_record.lineage, "operator_frame_id"),
            logical_parent_node_id=_lineage_value(start_record.lineage, "logical_parent_node_id"),
            iteration_index=_lineage_value(start_record.lineage, "iteration_index"),
        )

    def _execute_control_operator(
        self,
        *,
        node: WorkflowNodeContract,
        operator_frame_repository: OperatorFrameRepository,
        run_state_reader: RunStateReader | None,
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        inbound_edges: Mapping[str, Sequence[WorkflowEdgeContract]],
        completed_nodes: Mapping[str, NodeExecutionRecord],
        execution_boundary_ref: str,
        max_parallel_nodes: int,
        cancel_signal: RunCancellationSignal,
    ) -> tuple[list[NodeExecutionRecord], NodeExecutionRecord]:
        return execute_control_operator(
            node=node,
            operator_frame_repository=operator_frame_repository,
            run_state_reader=run_state_reader,
            request=request,
            intake_outcome=intake_outcome,
            writer=writer,
            cursor=cursor,
            inbound_edges=inbound_edges,
            completed_nodes=completed_nodes,
            execution_boundary_ref=execution_boundary_ref,
            max_parallel_nodes=max_parallel_nodes,
            cancel_signal=cancel_signal,
            execute_graph_fn=self._execute_graph,
        )

    def _execute_regular_wave(
        self,
        *,
        wave_nodes: Sequence[tuple[str, WorkflowNodeContract, Mapping[str, Any]]],
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        run_state_reader: RunStateReader | None,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        inbound_edges: Mapping[str, Sequence[WorkflowEdgeContract]],
        completed_nodes: dict[str, NodeExecutionRecord],
        node_results: list[NodeExecutionRecord],
        execution_order: list[str],
        execution_boundary_ref: str,
        max_parallel_nodes: int,
        context_accumulator: ContextAccumulator | None,
        cancel_signal: RunCancellationSignal,
    ) -> _FailureReason | None:
        wave_prepared: list[
            tuple[str, WorkflowNodeContract, Mapping[str, Any], Any, DeterministicTaskRequest]
        ] = []
        for node_id, node, dep_inputs in wave_nodes:
            prepared_node, failure = self._prepare_node_for_execution(
                node=node,
                request=request,
                intake_outcome=intake_outcome,
                node_results=node_results,
                context_accumulator=context_accumulator,
            )
            if failure is not None or prepared_node is None:
                return failure
            try:
                adapter = self._adapter_registry.resolve(adapter_type=prepared_node.adapter_type)
            except AdapterResolutionError as exc:
                return _FailureReason(
                    reason_code=exc.reason_code,
                    details={
                        "node_id": prepared_node.node_id,
                        "pending_node_ids": (),
                        "completed_node_ids": tuple(sorted(completed_nodes)),
                    },
                )
            task_request = _task_request(
                execution_boundary_ref=execution_boundary_ref,
                node=prepared_node,
                dependency_inputs=dep_inputs,
            )
            wave_prepared.append((node_id, prepared_node, dep_inputs, adapter, task_request))

        effective_parallelism = max(1, min(max_parallel_nodes, len(wave_prepared)))
        future_to_node: dict[
            Future[DeterministicTaskResult],
            tuple[
                str,
                WorkflowNodeContract,
                DeterministicTaskRequest,
                _RegularNodeStartRecord,
                DeterministicExecutionControl,
            ],
        ] = {}
        adapter_results: dict[str, NodeExecutionRecord] = {}
        cancel_requested = cancel_signal.cancel_requested()

        with ThreadPoolExecutor(max_workers=effective_parallelism) as pool:
            for node_id, prepared_node, _dep_inputs, adapter, task_request in wave_prepared:
                execution_control = DeterministicExecutionControl()
                controlled_request = replace(task_request, execution_control=execution_control)
                started_at = _now()
                start_record = self._start_regular_node(
                    node=prepared_node,
                    task_request=controlled_request,
                    started_at=started_at,
                    writer=writer,
                    cursor=cursor,
                    intake_outcome=intake_outcome,
                    inbound_edges=inbound_edges,
                    completed_nodes=completed_nodes,
                )
                future = pool.submit(adapter.execute, request=controlled_request)
                future_to_node[future] = (
                    node_id,
                    prepared_node,
                    controlled_request,
                    start_record,
                    execution_control,
                )

            pending_futures: set[Future[DeterministicTaskResult]] = set(future_to_node)
            pending_lock = threading.Lock()

            def _request_pending_cancellation() -> None:
                nonlocal cancel_requested
                with pending_lock:
                    cancel_requested = True
                    for future in pending_futures:
                        future_to_node[future][4].request_cancel()

            cancellation_bridge = _RegularWaveCancellationBridge(
                cancel_signal=cancel_signal,
                request_cancel=_request_pending_cancellation,
            )
            cancellation_bridge.start()
            try:
                if cancel_requested:
                    _request_pending_cancellation()
                while pending_futures:
                    done_futures, remaining_futures = wait(
                        pending_futures,
                        return_when=FIRST_COMPLETED,
                    )
                    with pending_lock:
                        pending_futures = set(remaining_futures)
                    for future in done_futures:
                        (
                            node_id,
                            prepared_node,
                            task_request,
                            start_record,
                            _execution_control,
                        ) = future_to_node[future]
                        adapter_result = future.result()
                        adapter_results[node_id] = self._complete_regular_node_record(
                            node=prepared_node,
                            task_request=task_request,
                            adapter_result=adapter_result,
                            start_record=start_record,
                            writer=writer,
                            cursor=cursor,
                            intake_outcome=intake_outcome,
                        )
            finally:
                cancellation_bridge.close()

        for node_id, prepared_node, _dep_inputs, _adapter, _task_request_unused in wave_prepared:
            record = adapter_results[node_id]
            execution_order.append(record.node_id)
            node_results.append(record)
            completed_nodes[node_id] = record
            if context_accumulator is not None:
                context_accumulator.add_node_result(
                    node_id=record.node_id,
                    node_name=record.task_name or prepared_node.display_name,
                    status=record.status,
                    outputs=dict(record.outputs),
                )
        if cancel_requested or any(record.status == "cancelled" for record in adapter_results.values()):
            return _FailureReason(
                reason_code="workflow_cancelled",
                details={
                    "run_id": intake_outcome.run_id,
                    "cancelled_node_ids": tuple(
                        sorted(
                            record.node_id
                            for record in adapter_results.values()
                            if record.status == "cancelled"
                        )
                    ),
                    "pending_node_ids": (),
                    "completed_node_ids": tuple(sorted(completed_nodes)),
                },
            )
        return None

    def _execute_graph(
        self,
        *,
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        operator_frame_repository: OperatorFrameRepository,
        run_state_reader: RunStateReader | None,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        pending_nodes: dict[str, WorkflowNodeContract],
        inbound_edges: Mapping[str, Sequence[WorkflowEdgeContract]],
        completed_nodes: dict[str, NodeExecutionRecord],
        node_results: list[NodeExecutionRecord],
        execution_order: list[str],
        execution_boundary_ref: str,
        max_parallel_nodes: int,
        context_accumulator: ContextAccumulator | None,
        cancel_signal: RunCancellationSignal,
    ) -> _FailureReason | None:
        while pending_nodes:
            current_run_state = (
                RunState.CANCELLED.value
                if cancel_signal.cancel_requested()
                else _current_run_state(
                    run_id=intake_outcome.run_id,
                    run_state_reader=run_state_reader,
                )
            )
            if current_run_state == RunState.CANCELLED.value:
                _cancel_open_operator_frames(
                    operator_frame_repository=operator_frame_repository,
                    run_id=intake_outcome.run_id,
                    stop_reason="workflow_cancelled",
                )
                return _FailureReason(
                    reason_code="workflow_cancelled",
                    details={
                        "run_id": intake_outcome.run_id,
                        "pending_node_ids": tuple(sorted(pending_nodes)),
                        "completed_node_ids": tuple(sorted(completed_nodes)),
                    },
                )
            eligible_nodes: list[tuple[int, str, WorkflowNodeContract, Mapping[str, Any]]] = []
            skipped_nodes: list[tuple[int, str, WorkflowNodeContract, _DependencyResolution]] = []
            for node_id, node in pending_nodes.items():
                dependency_resolution = resolve_dependencies(
                    node=node,
                    inbound_edges=inbound_edges.get(node_id, ()),
                    completed_nodes=completed_nodes,
                )
                if dependency_resolution.state == "ready":
                    eligible_nodes.append(
                        (
                            node.position_index,
                            node.node_id,
                            node,
                            dependency_resolution.dependency_inputs,
                        )
                    )
                elif dependency_resolution.state == "skipped":
                    skipped_nodes.append(
                        (
                            node.position_index,
                            node.node_id,
                            node,
                            dependency_resolution,
                        )
                    )

            if skipped_nodes:
                skipped_nodes.sort(key=lambda item: (item[0], item[1]))
                for _, node_id, node, resolution in skipped_nodes:
                    del pending_nodes[node_id]
                    skipped_record = self._emit_skipped_node(
                        node=node,
                        writer=writer,
                        cursor=cursor,
                        intake_outcome=intake_outcome,
                        inbound_edges=inbound_edges,
                        completed_nodes=completed_nodes,
                        reason_code=resolution.reason_code or "runtime.dependency_path_not_selected",
                        reason_details=resolution.reason_details,
                    )
                    execution_order.append(skipped_record.node_id)
                    node_results.append(skipped_record)
                    completed_nodes[node_id] = skipped_record
                continue

            if not eligible_nodes:
                return frontier_failure_reason(
                    pending_nodes=pending_nodes,
                    inbound_edges_map=inbound_edges,
                    completed_nodes=completed_nodes,
                )

            eligible_nodes.sort(key=lambda item: (item[0], item[1]))
            wave_nodes: list[tuple[str, WorkflowNodeContract, Mapping[str, Any]]] = []
            for _, node_id, node, dep_inputs in eligible_nodes:
                del pending_nodes[node_id]
                wave_nodes.append((node_id, node, dep_inputs))

            if any(node.adapter_type == "control_operator" for _, node, _ in wave_nodes):
                regular_chunk: list[tuple[str, WorkflowNodeContract, Mapping[str, Any]]] = []

                def _flush_regular_chunk() -> _FailureReason | None:
                    if not regular_chunk:
                        return None
                    chunk = tuple(regular_chunk)
                    regular_chunk.clear()
                    return self._execute_regular_wave(
                        wave_nodes=chunk,
                        request=request,
                        intake_outcome=intake_outcome,
                        run_state_reader=run_state_reader,
                        writer=writer,
                        cursor=cursor,
                        inbound_edges=inbound_edges,
                        completed_nodes=completed_nodes,
                        node_results=node_results,
                        execution_order=execution_order,
                        execution_boundary_ref=execution_boundary_ref,
                        max_parallel_nodes=max_parallel_nodes,
                        context_accumulator=context_accumulator,
                        cancel_signal=cancel_signal,
                    )

                for node_id, node, dep_inputs in wave_nodes:
                    if node.adapter_type != "control_operator":
                        regular_chunk.append((node_id, node, dep_inputs))
                        continue

                    failure = _flush_regular_chunk()
                    if failure is not None:
                        return failure

                    prepared_node, failure = self._prepare_node_for_execution(
                        node=node,
                        request=request,
                        intake_outcome=intake_outcome,
                        node_results=node_results,
                        context_accumulator=context_accumulator,
                    )
                    if failure is not None or prepared_node is None:
                        return failure
                    child_records, operator_record = self._execute_control_operator(
                        node=prepared_node,
                        operator_frame_repository=operator_frame_repository,
                        run_state_reader=run_state_reader,
                        request=request,
                        intake_outcome=intake_outcome,
                        writer=writer,
                        cursor=cursor,
                        inbound_edges=inbound_edges,
                        completed_nodes=completed_nodes,
                        execution_boundary_ref=execution_boundary_ref,
                        max_parallel_nodes=max_parallel_nodes,
                        cancel_signal=cancel_signal,
                    )
                    for child_record in child_records:
                        execution_order.append(child_record.node_id)
                        node_results.append(child_record)
                        completed_nodes[child_record.node_id] = child_record
                    execution_order.append(operator_record.node_id)
                    node_results.append(operator_record)
                    completed_nodes[node_id] = operator_record
                    if context_accumulator is not None:
                        context_accumulator.add_node_result(
                            node_id=operator_record.node_id,
                            node_name=operator_record.task_name or prepared_node.display_name,
                            status=operator_record.status,
                            outputs=dict(operator_record.outputs),
                        )
                    if operator_record.status == RunState.CANCELLED.value:
                        return _FailureReason(
                            reason_code=operator_record.failure_code or "workflow_cancelled",
                            details={
                                "node_id": operator_record.node_id,
                                "pending_node_ids": tuple(sorted(pending_nodes)),
                                "completed_node_ids": tuple(sorted(completed_nodes)),
                            },
                        )

                failure = _flush_regular_chunk()
                if failure is not None:
                    return failure
                continue

            failure = self._execute_regular_wave(
                wave_nodes=tuple(wave_nodes),
                request=request,
                intake_outcome=intake_outcome,
                run_state_reader=run_state_reader,
                writer=writer,
                cursor=cursor,
                inbound_edges=inbound_edges,
                completed_nodes=completed_nodes,
                node_results=node_results,
                execution_order=execution_order,
                execution_boundary_ref=execution_boundary_ref,
                max_parallel_nodes=max_parallel_nodes,
                context_accumulator=context_accumulator,
                cancel_signal=cancel_signal,
            )
        if failure is not None:
            return failure

        return None

    def _context_accumulator_for_request(
        self,
        *,
        request: WorkflowRequest,
        pending_nodes: Mapping[str, WorkflowNodeContract],
        accumulate_context: bool,
        max_context_tokens: int | None,
    ) -> ContextAccumulator | None:
        if not accumulate_context:
            return None
        budget: int | None
        if max_context_tokens is not None:
            budget = max_context_tokens
        else:
            first_node = next(iter(pending_nodes.values()), None)
            provider = (
                first_node.inputs.get("provider_slug", "anthropic")
                if first_node is not None
                else "anthropic"
            )
            model = first_node.inputs.get("model_slug") if first_node is not None else None
            if isinstance(model, str) and model.strip():
                try:
                    budget = safe_context_budget(provider, model)
                except Exception:
                    budget = None
            else:
                budget = None
        return ContextAccumulator(max_context_tokens=budget)

    def _resume_cursor_from_evidence(
        self,
        *,
        run_id: str,
        route_identity: RouteIdentity,
        evidence_writer: AtomicEvidenceWriter,
        current_state: RunState,
    ) -> _ExecutionCursor:
        if not hasattr(evidence_writer, "evidence_timeline"):
            raise RuntimeBoundaryError(
                "deterministic execution resume requires an evidence writer with evidence_timeline()"
            )
        timeline = tuple(evidence_writer.evidence_timeline(run_id))
        if not timeline:
            raise RuntimeLifecycleError(
                f"runtime.execution_resume_missing_evidence:{run_id}"
            )
        last_row = timeline[-1]
        return _ExecutionCursor(
            route_identity=route_identity,
            transition_seq=last_row.transition_seq + 1,
            next_evidence_seq=last_row.evidence_seq + 1,
            current_state=current_state,
        )

    def _execute_admitted_run(
        self,
        *,
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        evidence_writer: AtomicEvidenceWriter,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        operator_frame_repository: OperatorFrameRepository,
        run_state_reader: RunStateReader | None,
        cancel_signal: RunCancellationSignal,
        max_parallel_nodes: int,
        accumulate_context: bool,
        max_context_tokens: int | None,
    ) -> RunExecutionResult:
        node_results: list[NodeExecutionRecord] = []
        queue_transition = LifecycleTransition(
            route_identity=cursor.identity_for_current_transition(),
            from_state=cursor.current_state,
            to_state=RunState.QUEUED,
            reason_code="runtime.execution_ready",
            evidence_seq=cursor.next_evidence_seq,
            event_type=WORKFLOW_QUEUED_EVENT_TYPE,
            receipt_type=WORKFLOW_QUEUE_RECEIPT_TYPE,
            occurred_at=_now(),
        )
        queue_result = self.advance_run(
            transition=queue_transition,
            evidence_writer=evidence_writer,
        )
        cursor.advance(result=queue_result, new_state=RunState.QUEUED)
        if cancel_signal.cancel_requested() or (
            _current_run_state(
                run_id=intake_outcome.run_id,
                run_state_reader=run_state_reader,
            )
            == RunState.CANCELLED.value
        ):
            cancel_signal.close()
            return self._cancel_run(
                request=request,
                intake_outcome=intake_outcome,
                writer=writer,
                cursor=cursor,
                node_results=node_results,
                cancel_reason=_FailureReason(
                    reason_code="workflow_cancelled",
                    details={
                        "run_id": intake_outcome.run_id,
                        "pending_node_ids": (),
                        "completed_node_ids": (),
                    },
                ),
            )

        start_transition = LifecycleTransition(
            route_identity=cursor.identity_for_current_transition(),
            from_state=cursor.current_state,
            to_state=RunState.RUNNING,
            reason_code="runtime.execution_started",
            evidence_seq=cursor.next_evidence_seq,
            event_type=WORKFLOW_STARTED_EVENT_TYPE,
            receipt_type=WORKFLOW_START_RECEIPT_TYPE,
            occurred_at=_now(),
        )
        start_result = self.advance_run(
            transition=start_transition,
            evidence_writer=evidence_writer,
        )
        cursor.advance(result=start_result, new_state=RunState.RUNNING)

        completed_nodes: dict[str, NodeExecutionRecord] = {}
        pending_nodes = {
            node.node_id: node
            for node in _node_order(request)
            if node.template_owner_node_id is None
        }
        inbound_edges = _inbound_edges(request=request)
        execution_order: list[str] = []
        context_accumulator = self._context_accumulator_for_request(
            request=request,
            pending_nodes=pending_nodes,
            accumulate_context=accumulate_context,
            max_context_tokens=max_context_tokens,
        )
        execution_boundary_ref = _execution_boundary_ref(intake_outcome=intake_outcome)

        failure_reason = self._execute_graph(
            request=request,
            intake_outcome=intake_outcome,
            operator_frame_repository=operator_frame_repository,
            run_state_reader=run_state_reader,
            writer=writer,
            cursor=cursor,
            pending_nodes=pending_nodes,
            inbound_edges=inbound_edges,
            completed_nodes=completed_nodes,
            node_results=node_results,
            execution_order=execution_order,
            execution_boundary_ref=execution_boundary_ref,
            max_parallel_nodes=max_parallel_nodes,
            context_accumulator=context_accumulator,
            cancel_signal=cancel_signal,
        )
        if failure_reason is not None:
            if failure_reason.reason_code == "workflow_cancelled":
                cancel_signal.close()
                return self._cancel_run(
                    request=request,
                    intake_outcome=intake_outcome,
                    writer=writer,
                    cursor=cursor,
                    node_results=node_results,
                    cancel_reason=failure_reason,
                )
            cancel_signal.close()
            return self._fail_run(
                request=request,
                intake_outcome=intake_outcome,
                writer=writer,
                cursor=cursor,
                node_results=node_results,
                failure_reason=failure_reason,
            )

        has_failure = any(nr.status == "failed" for nr in node_results)
        if has_failure:
            first_failure = next(nr for nr in node_results if nr.status == "failed")
            cancel_signal.close()
            return self._fail_run(
                request=request,
                intake_outcome=intake_outcome,
                writer=writer,
                cursor=cursor,
                node_results=node_results,
                failure_reason=_FailureReason(
                    reason_code=first_failure.failure_code or "adapter.command_failed",
                    details={
                        "node_id": first_failure.node_id,
                        "task_name": first_failure.task_name,
                        "pending_node_ids": (),
                        "completed_node_ids": tuple(sorted(completed_nodes)),
                    },
                ),
            )

        if cancel_signal.cancel_requested() or (
            _current_run_state(
                run_id=intake_outcome.run_id,
                run_state_reader=run_state_reader,
            )
            == RunState.CANCELLED.value
        ):
            cancel_signal.close()
            return self._cancel_run(
                request=request,
                intake_outcome=intake_outcome,
                writer=writer,
                cursor=cursor,
                node_results=node_results,
                cancel_reason=_FailureReason(
                    reason_code="workflow_cancelled",
                    details={
                        "run_id": intake_outcome.run_id,
                        "pending_node_ids": (),
                        "completed_node_ids": tuple(sorted(completed_nodes)),
                    },
                ),
            )

        success_transition = LifecycleTransition(
            route_identity=cursor.identity_for_current_transition(),
            from_state=cursor.current_state,
            to_state=RunState.SUCCEEDED,
            reason_code="runtime.workflow_succeeded",
            evidence_seq=cursor.next_evidence_seq,
            event_type=WORKFLOW_SUCCEEDED_EVENT_TYPE,
            receipt_type=WORKFLOW_COMPLETION_RECEIPT_TYPE,
            occurred_at=_now(),
        )
        success_result = self.advance_run(
            transition=success_transition,
            evidence_writer=evidence_writer,
        )
        cursor.advance(result=success_result, new_state=RunState.SUCCEEDED)
        cancel_signal.close()
        return RunExecutionResult(
            workflow_id=request.workflow_id,
            run_id=intake_outcome.run_id,
            request_id=request.request_id,
            current_state=RunState.SUCCEEDED,
            terminal_reason_code="runtime.workflow_succeeded",
            node_order=tuple(execution_order),
            node_results=tuple(node_results),
            admitted_definition_ref=intake_outcome.admitted_definition_ref,
            admitted_definition_hash=intake_outcome.admitted_definition_hash,
        )

    def execute_deterministic_path(
        self,
        *,
        intake_outcome: WorkflowIntakeOutcome,
        evidence_writer: AtomicEvidenceWriter,
        max_parallel_nodes: int = 4,
        accumulate_context: bool = True,
        max_context_tokens: int | None = None,
    ) -> RunExecutionResult:
        writer = _validate_execution_writer(evidence_writer)
        operator_frame_repository = _operator_frame_repository_for_execution(
            evidence_writer=evidence_writer,
            default_repository=self._default_operator_frames,
        )
        run_state_reader = _run_state_reader_for_execution(
            evidence_writer=evidence_writer,
            evidence_reader=self._evidence_reader,
        )
        if intake_outcome.current_state not in {
            RunState.CLAIM_ACCEPTED,
            RunState.CLAIM_REJECTED,
        }:
            raise RuntimeLifecycleError(
                f"runtime.execution_invalid_start:{intake_outcome.current_state.value}"
            )
        cancel_signal = _run_cancellation_signal_for_execution(
            run_id=intake_outcome.run_id,
            evidence_writer=evidence_writer,
            evidence_reader=self._evidence_reader,
            run_state_reader=run_state_reader,
        )

        request = intake_outcome.workflow_request
        cursor = _ExecutionCursor(route_identity=intake_outcome.route_identity)
        node_results: list[NodeExecutionRecord] = []
        operator_frame_repository.clear_for_run(run_id=intake_outcome.run_id)

        submission_result = evidence_writer.commit_submission(
            route_identity=cursor.identity_for_current_transition(),
            request_payload=_workflow_request_payload(request),
            admitted_definition_ref=(
                intake_outcome.admitted_definition_ref or request.workflow_definition_id
            ),
            admitted_definition_hash=(
                intake_outcome.admitted_definition_hash or request.definition_hash
            ),
        )
        cursor.advance(result=submission_result, new_state=RunState.CLAIM_RECEIVED)

        admission_state = intake_outcome.current_state
        admission_event_type = (
            CLAIM_VALIDATED_EVENT_TYPE
            if intake_outcome.admission_decision.decision is AdmissionDecisionKind.ADMIT
            else CLAIM_REJECTED_EVENT_TYPE
        )
        admission_status = admission_state.value
        admission_reason_code = intake_outcome.admission_decision.reason_code
        admission_proof = build_transition_proof(
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
            event_type=admission_event_type,
            receipt_type=CLAIM_VALIDATION_RECEIPT_TYPE,
            reason_code=admission_reason_code,
            evidence_seq=cursor.next_evidence_seq,
            occurred_at=_now(),
            started_at=intake_outcome.admission_decision.decided_at,
            finished_at=intake_outcome.admission_decision.decided_at,
            executor_type="runtime.intake",
            status=admission_status,
            payload={
                "from_state": RunState.CLAIM_RECEIVED.value,
                "to_state": admission_state.value,
                "validation_result_ref": intake_outcome.validation_result.validation_result_ref,
                "authority_context_ref": intake_outcome.admission_decision.authority_context_ref,
                "admission_decision_id": intake_outcome.admission_decision.admission_decision_id,
            },
            inputs={
                "validation_result_ref": intake_outcome.validation_result.validation_result_ref,
                "request_digest": intake_outcome.request_digest,
                "authority_context_ref": intake_outcome.admission_decision.authority_context_ref,
            },
            outputs={
                "admission_decision_id": intake_outcome.admission_decision.admission_decision_id,
                "to_state": admission_state.value,
            },
            decision_refs=_decision_refs_for_admission(intake_outcome),
            failure_code=(
                admission_reason_code
                if admission_state is RunState.CLAIM_REJECTED
                else None
            ),
        )
        admission_result = writer.append_transition_proof(admission_proof)
        cursor.advance(result=admission_result, new_state=admission_state)

        if admission_state is RunState.CLAIM_REJECTED:
            cancel_signal.close()
            return RunExecutionResult(
                workflow_id=request.workflow_id,
                run_id=intake_outcome.run_id,
                request_id=request.request_id,
                current_state=RunState.CLAIM_REJECTED,
                terminal_reason_code=admission_reason_code,
                node_order=(),
                node_results=(),
                admitted_definition_ref=intake_outcome.admitted_definition_ref,
                admitted_definition_hash=intake_outcome.admitted_definition_hash,
            )
        return self._execute_admitted_run(
            request=request,
            intake_outcome=intake_outcome,
            evidence_writer=evidence_writer,
            writer=writer,
            cursor=cursor,
            operator_frame_repository=operator_frame_repository,
            run_state_reader=run_state_reader,
            cancel_signal=cancel_signal,
            max_parallel_nodes=max_parallel_nodes,
            accumulate_context=accumulate_context,
            max_context_tokens=max_context_tokens,
        )

    def execute_admitted_deterministic_path(
        self,
        *,
        intake_outcome: WorkflowIntakeOutcome,
        evidence_writer: AtomicEvidenceWriter,
        max_parallel_nodes: int = 4,
        accumulate_context: bool = True,
        max_context_tokens: int | None = None,
    ) -> RunExecutionResult:
        writer = _validate_execution_writer(evidence_writer)
        operator_frame_repository = _operator_frame_repository_for_execution(
            evidence_writer=evidence_writer,
            default_repository=self._default_operator_frames,
        )
        run_state_reader = _run_state_reader_for_execution(
            evidence_writer=evidence_writer,
            evidence_reader=self._evidence_reader,
        )
        if intake_outcome.current_state is not RunState.CLAIM_ACCEPTED:
            raise RuntimeLifecycleError(
                f"runtime.execution_invalid_resume:{intake_outcome.current_state.value}"
            )
        persisted_state = _current_run_state(
            run_id=intake_outcome.run_id,
            run_state_reader=run_state_reader,
        )
        if persisted_state != RunState.CLAIM_ACCEPTED.value:
            raise RuntimeLifecycleError(
                f"runtime.execution_resume_expected_claim_accepted:{persisted_state or 'missing'}"
            )
        cancel_signal = _run_cancellation_signal_for_execution(
            run_id=intake_outcome.run_id,
            evidence_writer=evidence_writer,
            evidence_reader=self._evidence_reader,
            run_state_reader=run_state_reader,
        )
        request = intake_outcome.workflow_request
        cursor = self._resume_cursor_from_evidence(
            run_id=intake_outcome.run_id,
            route_identity=intake_outcome.route_identity,
            evidence_writer=evidence_writer,
            current_state=RunState.CLAIM_ACCEPTED,
        )
        operator_frame_repository.clear_for_run(run_id=intake_outcome.run_id)
        return self._execute_admitted_run(
            request=request,
            intake_outcome=intake_outcome,
            evidence_writer=evidence_writer,
            writer=writer,
            cursor=cursor,
            operator_frame_repository=operator_frame_repository,
            run_state_reader=run_state_reader,
            cancel_signal=cancel_signal,
            max_parallel_nodes=max_parallel_nodes,
            accumulate_context=accumulate_context,
            max_context_tokens=max_context_tokens,
        )

    def _cancel_run(
        self,
        *,
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        node_results: Sequence[NodeExecutionRecord],
        cancel_reason: _FailureReason,
    ) -> RunExecutionResult:
        cancel_transition = LifecycleTransition(
            route_identity=cursor.identity_for_current_transition(),
            from_state=cursor.current_state,
            to_state=RunState.CANCELLED,
            reason_code=cancel_reason.reason_code,
            evidence_seq=cursor.next_evidence_seq,
            event_type=WORKFLOW_CANCELLED_EVENT_TYPE,
            receipt_type=WORKFLOW_CANCELLED_RECEIPT_TYPE,
            occurred_at=_now(),
        )
        validate_transition(cancel_transition)
        cancelled_at = cancel_transition.occurred_at
        cancel_proof = build_transition_proof(
            route_identity=cancel_transition.route_identity,
            transition_seq=cancel_transition.route_identity.transition_seq,
            event_id=_event_id(
                run_id=intake_outcome.run_id,
                evidence_seq=cursor.next_evidence_seq,
            ),
            receipt_id=_receipt_id(
                run_id=intake_outcome.run_id,
                evidence_seq=cursor.next_evidence_seq + 1,
            ),
            event_type=WORKFLOW_CANCELLED_EVENT_TYPE,
            receipt_type=WORKFLOW_CANCELLED_RECEIPT_TYPE,
            reason_code=cancel_reason.reason_code,
            evidence_seq=cursor.next_evidence_seq,
            occurred_at=cancelled_at,
            started_at=cancelled_at,
            finished_at=cancelled_at,
            executor_type="runtime.transition",
            status=RunState.CANCELLED.value,
            payload={
                "from_state": cursor.current_state.value,
                "to_state": RunState.CANCELLED.value,
                "cancel_reason": {
                    "reason_code": cancel_reason.reason_code,
                    **dict(cancel_reason.details),
                },
            },
            inputs={
                "from_state": cursor.current_state.value,
                "to_state": RunState.CANCELLED.value,
                "cancel_reason": {
                    "reason_code": cancel_reason.reason_code,
                    **dict(cancel_reason.details),
                },
            },
            outputs={
                "to_state": RunState.CANCELLED.value,
                "terminal_reason_code": cancel_reason.reason_code,
                "node_order": tuple(record.node_id for record in node_results),
            },
            failure_code=cancel_reason.reason_code,
        )
        cancel_result = writer.append_transition_proof(cancel_proof)
        cursor.advance(result=cancel_result, new_state=RunState.CANCELLED)
        return RunExecutionResult(
            workflow_id=request.workflow_id,
            run_id=intake_outcome.run_id,
            request_id=request.request_id,
            current_state=RunState.CANCELLED,
            terminal_reason_code=cancel_reason.reason_code,
            node_order=tuple(record.node_id for record in node_results),
            node_results=tuple(node_results),
            admitted_definition_ref=intake_outcome.admitted_definition_ref,
            admitted_definition_hash=intake_outcome.admitted_definition_hash,
        )

    def _fail_run(
        self,
        *,
        request: WorkflowRequest,
        intake_outcome: WorkflowIntakeOutcome,
        writer: TransitionProofWriter,
        cursor: _ExecutionCursor,
        node_results: Sequence[NodeExecutionRecord],
        failure_reason: _FailureReason,
    ) -> RunExecutionResult:
        failure_transition = LifecycleTransition(
            route_identity=cursor.identity_for_current_transition(),
            from_state=cursor.current_state,
            to_state=RunState.FAILED,
            reason_code=failure_reason.reason_code,
            evidence_seq=cursor.next_evidence_seq,
            event_type=WORKFLOW_FAILED_EVENT_TYPE,
            receipt_type=WORKFLOW_COMPLETION_RECEIPT_TYPE,
            occurred_at=_now(),
        )
        validate_transition(failure_transition)
        failure_occurred_at = failure_transition.occurred_at
        failure_proof = build_transition_proof(
            route_identity=failure_transition.route_identity,
            transition_seq=failure_transition.route_identity.transition_seq,
            event_id=_event_id(
                run_id=intake_outcome.run_id,
                evidence_seq=cursor.next_evidence_seq,
            ),
            receipt_id=_receipt_id(
                run_id=intake_outcome.run_id,
                evidence_seq=cursor.next_evidence_seq + 1,
            ),
            event_type=WORKFLOW_FAILED_EVENT_TYPE,
            receipt_type=WORKFLOW_COMPLETION_RECEIPT_TYPE,
            reason_code=failure_reason.reason_code,
            evidence_seq=cursor.next_evidence_seq,
            occurred_at=failure_occurred_at,
            started_at=failure_occurred_at,
            finished_at=failure_occurred_at,
            executor_type="runtime.transition",
            status=RunState.FAILED.value,
            payload={
                "from_state": cursor.current_state.value,
                "to_state": RunState.FAILED.value,
                "failure_reason": {
                    "reason_code": failure_reason.reason_code,
                    **dict(failure_reason.details),
                },
            },
            inputs={
                "from_state": cursor.current_state.value,
                "to_state": RunState.FAILED.value,
                "failure_reason": {
                    "reason_code": failure_reason.reason_code,
                    **dict(failure_reason.details),
                },
            },
            outputs={
                "to_state": RunState.FAILED.value,
                "terminal_reason_code": failure_reason.reason_code,
                "node_order": tuple(record.node_id for record in node_results),
            },
            failure_code=failure_reason.reason_code,
        )
        failure_result = writer.append_transition_proof(failure_proof)
        cursor.advance(result=failure_result, new_state=RunState.FAILED)
        return RunExecutionResult(
            workflow_id=request.workflow_id,
            run_id=intake_outcome.run_id,
            request_id=request.request_id,
            current_state=RunState.FAILED,
            terminal_reason_code=failure_reason.reason_code,
            node_order=tuple(record.node_id for record in node_results),
            node_results=tuple(node_results),
            admitted_definition_ref=intake_outcome.admitted_definition_ref,
            admitted_definition_hash=intake_outcome.admitted_definition_hash,
        )

    def _canonical_evidence(self, *, run_id: str) -> tuple[EvidenceRow, ...]:
        if self._evidence_reader is None:
            raise RuntimeBoundaryError(
                "inspect/replay require an evidence reader with evidence_timeline()"
            )
        return tuple(self._evidence_reader.evidence_timeline(run_id))

    def _inspection_operator_frame_snapshot(
        self,
        *,
        run_id: str,
    ) -> tuple[str, tuple[OperatorFrameReadModel, ...]]:
        repository, _source = self._inspection_operator_frame_repository(run_id=run_id)
        if repository is None:
            return _source, ()
        try:
            frames = repository.list_for_run(run_id=run_id)
        except Exception:
            return "unavailable", ()
        return _source, tuple(_operator_frame_read_model(frame) for frame in frames)

    def _inspection_operator_frame_repository(
        self,
        *,
        run_id: str,
    ) -> tuple[OperatorFrameRepository | None, str]:
        del run_id
        repository = (
            _bound_operator_frame_repository(self._evidence_reader)
            if self._evidence_reader is not None
            else None
        )
        if repository is not None:
            return repository, "canonical_operator_frames"
        if self._operator_frames_explicitly_bound:
            return self._default_operator_frames, "explicit_operator_frames"
        return None, "missing"


__all__ = [
    "CanonicalEvidenceReader",
    "NodeExecutionRecord",
    "RunExecutionResult",
    "RuntimeOrchestrator",
    "TransitionProofWriter",
]
