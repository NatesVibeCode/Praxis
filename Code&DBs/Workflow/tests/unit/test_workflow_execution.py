from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.workflow._workflow_execution import (
    WorkflowExecutionContext,
    execute_workflow_request,
    plan_workflow_request,
)
from runtime.domain import RunState


class _FakePlanner:
    def __init__(self, *, registry, intake_outcome) -> None:
        self.registry = registry
        self._intake_outcome = intake_outcome

    def plan(self, *, request):
        return self._intake_outcome


class _FakeLoadBalancer:
    def __init__(self, acquired: bool) -> None:
        self.acquired = acquired
        self.providers = []

    def slot(self, provider_slug: str):
        self.providers.append(provider_slug)
        acquired = self.acquired

        class _Slot:
            def __enter__(self_inner):
                return acquired

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        return _Slot()


class _FakeFuture:
    def __init__(self, *, result=None, error=None) -> None:
        self._result = result
        self._error = error

    def result(self, timeout=None):
        if self._error is not None:
            raise self._error
        return self._result


class _FakeThreadPoolExecutor:
    def __init__(self, *, future, sink=None, max_workers=None) -> None:
        self.future = future
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, **kwargs):
        if self.sink is not None:
            self.sink.append((fn, kwargs))
        return self.future


def _context() -> WorkflowExecutionContext:
    return WorkflowExecutionContext(
        provider_slug="anthropic",
        model_slug="claude-test",
        adapter_type="cli_llm",
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        start_ns=0,
    )


def test_plan_workflow_request_returns_rejection_result(monkeypatch):
    from runtime.workflow import _workflow_execution as execution_module

    intake_outcome = SimpleNamespace(
        admission_state=RunState.CLAIM_REJECTED,
        admission_decision=SimpleNamespace(reason_code="scope.denied"),
        run_id="run_rejected",
    )
    monkeypatch.setattr(
        execution_module,
        "WorkflowIntakePlanner",
        lambda *, registry: _FakePlanner(registry=registry, intake_outcome=intake_outcome),
    )

    planned, failure = plan_workflow_request(
        request=SimpleNamespace(),
        registry=SimpleNamespace(),
        context=_context(),
    )

    assert planned is None
    assert failure.run_id == "run_rejected"
    assert failure.reason_code == "scope.denied"
    assert failure.failure_code == "intake.rejected"


def test_execute_workflow_request_returns_capacity_failure(monkeypatch):
    from runtime.workflow import _workflow_execution as execution_module

    load_balancer = _FakeLoadBalancer(acquired=False)
    monkeypatch.setattr(execution_module._workflow_caps, "LOAD_BALANCER", load_balancer)

    execution_result, failure = execute_workflow_request(
        intake_outcome=SimpleNamespace(run_id="run_capacity"),
        adapter_registry=SimpleNamespace(),
        evidence_writer=SimpleNamespace(evidence_timeline=lambda run_id: []),
        context=_context(),
        timeout=300,
    )

    assert execution_result is None
    assert load_balancer.providers == ["anthropic"]
    assert failure.reason_code == "provider.capacity"
    assert failure.failure_code == "provider.capacity"
    assert failure.outputs == {"error": "Provider at capacity: anthropic"}


def test_execute_workflow_request_counts_failure_evidence_on_timeout(monkeypatch):
    from runtime.workflow import _workflow_execution as execution_module

    monkeypatch.setattr(execution_module._workflow_caps, "LOAD_BALANCER", None)
    monkeypatch.setattr(
        execution_module,
        "RuntimeOrchestrator",
        lambda *, adapter_registry, evidence_reader: SimpleNamespace(
            execute_deterministic_path=lambda **kwargs: None
        ),
    )
    monkeypatch.setattr(
        execution_module,
        "ThreadPoolExecutor",
        lambda max_workers: _FakeThreadPoolExecutor(
            future=_FakeFuture(error=execution_module.FuturesTimeoutError())
        ),
    )

    execution_result, failure = execute_workflow_request(
        intake_outcome=SimpleNamespace(run_id="run_timeout"),
        adapter_registry=SimpleNamespace(),
        evidence_writer=SimpleNamespace(evidence_timeline=lambda run_id: [1, 2, 3]),
        context=_context(),
        timeout=300,
        count_evidence_on_failure=True,
    )

    assert execution_result is None
    assert failure.reason_code == "workflow.execution_timeout"
    assert failure.failure_code == "workflow.timeout"
    assert failure.evidence_count == 3


def test_execute_workflow_request_passes_max_context_tokens(monkeypatch):
    from runtime.workflow import _workflow_execution as execution_module

    submissions = []
    monkeypatch.setattr(execution_module._workflow_caps, "LOAD_BALANCER", None)
    monkeypatch.setattr(
        execution_module,
        "RuntimeOrchestrator",
        lambda *, adapter_registry, evidence_reader: SimpleNamespace(
            execute_deterministic_path=lambda **kwargs: "ok"
        ),
    )
    monkeypatch.setattr(
        execution_module,
        "ThreadPoolExecutor",
        lambda max_workers: _FakeThreadPoolExecutor(
            future=_FakeFuture(result="execution"),
            sink=submissions,
        ),
    )

    execution_result, failure = execute_workflow_request(
        intake_outcome=SimpleNamespace(run_id="run_ok"),
        adapter_registry=SimpleNamespace(),
        evidence_writer=SimpleNamespace(evidence_timeline=lambda run_id: []),
        context=_context(),
        timeout=300,
        max_context_tokens=2048,
    )

    assert failure is None
    assert execution_result == "execution"
    assert submissions[0][1]["max_context_tokens"] == 2048
