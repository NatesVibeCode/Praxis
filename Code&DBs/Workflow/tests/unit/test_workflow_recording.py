from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.workflow._recording import (
    emit_workflow_finished,
    emit_workflow_started,
    record_workflow_result,
)
from runtime.workflow.orchestrator import WorkflowResult, WorkflowSpec


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def __call__(self, *args, **kwargs) -> None:
        self.calls.append((args, kwargs))


class _ReviewTracker(_Recorder):
    def record_review(self, result) -> None:
        self.calls.append(((result,), {}))


class _CapabilityTracker(_Recorder):
    def record_outcome(self, result, *, capabilities) -> None:
        self.calls.append(((result,), {"capabilities": capabilities}))


class _RouteOutcomes:
    def __init__(self) -> None:
        self.outcomes = []

    def record_outcome(self, outcome) -> None:
        self.outcomes.append(outcome)


class _CircuitBreakers:
    def __init__(self) -> None:
        self.calls = []

    def record_outcome(self, provider_slug, succeeded, failure_code=None) -> None:
        self.calls.append((provider_slug, succeeded, failure_code))


class _WorkflowHistory:
    def __init__(self) -> None:
        self.results = []

    def record_workflow(self, result) -> None:
        self.results.append(result)


class _CostTracker:
    def __init__(self) -> None:
        self.results = []

    def record_cost(self, result) -> None:
        self.results.append(result)


class _TrustScorer:
    def __init__(self) -> None:
        self.calls = []

    def update(self, provider_slug, model_slug, succeeded) -> None:
        self.calls.append((provider_slug, model_slug, succeeded))


class _MetricsView:
    def __init__(self) -> None:
        self.results = []

    def record_workflow(self, result) -> None:
        self.results.append(result)


class _ObservabilityHub:
    def __init__(self) -> None:
        self.receipts = []

    def ingest_receipt(self, receipt) -> None:
        self.receipts.append(receipt)


def _result(**overrides) -> WorkflowResult:
    defaults = dict(
        run_id="run_123",
        status="succeeded",
        reason_code="ok",
        completion="done",
        outputs={"cost_usd": 1.25},
        evidence_count=4,
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 8, 12, 0, 5, tzinfo=timezone.utc),
        latency_ms=5000,
        provider_slug="anthropic",
        model_slug="claude-test",
        adapter_type="cli_llm",
        failure_code=None,
        attempts=1,
        label="job-a",
        task_type=None,
        capabilities=None,
        author_model="anthropic/claude-test",
        reviews_workflow_id=None,
        review_target_modules=None,
    )
    defaults.update(overrides)
    return WorkflowResult(**defaults)


def test_record_workflow_result_fans_out_to_platform_subsystems(monkeypatch):
    from runtime.workflow import _recording

    route_outcomes = _RouteOutcomes()
    circuit_breakers = _CircuitBreakers()
    history = _WorkflowHistory()
    cost_tracker = _CostTracker()
    trust_scorer = _TrustScorer()
    metrics_view = _MetricsView()
    receipt_writer = _Recorder()
    notifier = _Recorder()
    obs_hub = _ObservabilityHub()
    review_tracker = _ReviewTracker()

    monkeypatch.setattr(_recording._workflow_caps, "ROUTE_OUTCOMES", route_outcomes)
    monkeypatch.setattr(_recording._workflow_caps, "CIRCUIT_BREAKERS", circuit_breakers)
    monkeypatch.setattr(_recording._workflow_caps, "WORKFLOW_HISTORY", history)
    monkeypatch.setattr(_recording._workflow_caps, "COST_TRACKER", cost_tracker)
    monkeypatch.setattr(_recording._workflow_caps, "TRUST_SCORER", trust_scorer)
    monkeypatch.setattr(_recording._workflow_caps, "WORKFLOW_METRICS_VIEW", metrics_view)
    monkeypatch.setattr(
        _recording._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(
            receipt_writer=receipt_writer,
            completion_notifier=notifier,
            obs_hub=lambda: obs_hub,
            failure_classifier=None,
            event_logger=None,
            event_type_started="workflow.started",
            event_type_completed="workflow.completed",
            event_type_failed="workflow.failed",
        ),
    )

    import runtime.review_tracker as review_tracker_module

    monkeypatch.setattr(review_tracker_module, "get_review_tracker", lambda: review_tracker)

    result = _result()
    record_workflow_result(result)

    assert len(route_outcomes.outcomes) == 1
    assert route_outcomes.outcomes[0].provider_slug == "anthropic"
    assert circuit_breakers.calls == [("anthropic", True, None)]
    assert history.results == [result]
    assert cost_tracker.results == [result]
    assert trust_scorer.calls == [("anthropic", "claude-test", True)]
    assert receipt_writer.calls == [((result,), {})]
    assert notifier.calls == [((result,), {})]
    assert metrics_view.results == [result]
    assert review_tracker.calls == [((result,), {})]
    assert obs_hub.receipts == [
        {
            "receipt_id": "receipt:run_123:job-a:1",
            "workflow_id": "run_123",
            "agent_slug": "anthropic/claude-test",
            "provider_slug": "anthropic",
            "model_slug": "claude-test",
            "status": "succeeded",
            "cost": 1.25,
            "latency_seconds": 5.0,
            "job_label": "job-a",
            "label": "job-a",
            "node_id": "job-a",
            "attempt_no": 1,
            "timestamp": "2026-04-08T12:00:05+00:00",
            "failure_code": None,
            "run_id": "run_123",
        }
    ]


def test_record_workflow_result_handles_failure_feedback_and_auto_review(monkeypatch):
    from runtime.workflow import _recording

    capability_tracker = _CapabilityTracker()
    queue_auto_review = _Recorder()
    obs_hub = _ObservabilityHub()
    classification = SimpleNamespace(category=SimpleNamespace(value="rate_limited"))

    monkeypatch.setattr(
        _recording._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(
            receipt_writer=None,
            completion_notifier=None,
            obs_hub=lambda: obs_hub,
            failure_classifier=lambda failure_code, outputs: classification,
            event_logger=None,
            event_type_started="workflow.started",
            event_type_completed="workflow.completed",
            event_type_failed="workflow.failed",
        ),
    )
    monkeypatch.setattr(_recording._workflow_caps, "ROUTE_OUTCOMES", _RouteOutcomes())
    monkeypatch.setattr(_recording._workflow_caps, "CIRCUIT_BREAKERS", None)
    monkeypatch.setattr(_recording._workflow_caps, "WORKFLOW_HISTORY", None)
    monkeypatch.setattr(_recording._workflow_caps, "COST_TRACKER", None)
    monkeypatch.setattr(_recording._workflow_caps, "TRUST_SCORER", None)
    monkeypatch.setattr(_recording._workflow_caps, "WORKFLOW_METRICS_VIEW", None)

    import runtime.capability_feedback as capability_feedback_module
    import runtime.review_tracker as review_tracker_module
    import runtime.auto_review as auto_review_module
    import storage.postgres as postgres_module

    monkeypatch.setattr(capability_feedback_module, "get_capability_tracker", lambda: capability_tracker)
    monkeypatch.setattr(review_tracker_module, "get_review_tracker", lambda: _ReviewTracker())
    monkeypatch.setattr(auto_review_module, "queue_auto_review", queue_auto_review)
    monkeypatch.setattr(postgres_module, "get_workflow_pool", lambda: "pool")
    monkeypatch.setattr(postgres_module, "SyncPostgresConnection", lambda pool: f"conn:{pool}")

    result = _result(
        status="failed",
        reason_code="workflow.failed",
        completion=None,
        outputs={"error": "timeout"},
        failure_code="workflow.timeout",
        capabilities=["ops"],
    )
    spec = WorkflowSpec(prompt="test", skip_auto_review=False, capabilities=["ops"])

    record_workflow_result(result, spec=spec)

    assert capability_tracker.calls == [((result,), {"capabilities": ["ops"]})]
    assert queue_auto_review.calls == [((result,), {"conn": "conn:pool"})]
    assert obs_hub.receipts == [
        {
            "receipt_id": "receipt:run_123:job-a:1",
            "workflow_id": "run_123",
            "agent_slug": "anthropic/claude-test",
            "provider_slug": "anthropic",
            "model_slug": "claude-test",
            "status": "failed",
            "cost": 0.0,
            "latency_seconds": 5.0,
            "job_label": "job-a",
            "label": "job-a",
            "node_id": "job-a",
            "attempt_no": 1,
            "timestamp": "2026-04-08T12:00:05+00:00",
            "failure_code": "workflow.timeout",
            "failure_category": "rate_limited",
            "run_id": "run_123",
        }
    ]


def test_record_workflow_result_records_task_type_feedback(monkeypatch):
    from runtime.workflow import _recording

    calls = []
    monkeypatch.setattr(_recording._workflow_caps, "ROUTE_OUTCOMES", _RouteOutcomes())
    monkeypatch.setattr(_recording._workflow_caps, "CIRCUIT_BREAKERS", None)
    monkeypatch.setattr(_recording._workflow_caps, "WORKFLOW_HISTORY", None)
    monkeypatch.setattr(_recording._workflow_caps, "COST_TRACKER", None)
    monkeypatch.setattr(_recording._workflow_caps, "TRUST_SCORER", None)
    monkeypatch.setattr(_recording._workflow_caps, "WORKFLOW_METRICS_VIEW", None)
    monkeypatch.setattr(
        _recording._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(
            receipt_writer=None,
            completion_notifier=None,
            obs_hub=None,
            failure_classifier=None,
            event_logger=None,
            event_type_started="workflow.started",
            event_type_completed="workflow.completed",
            event_type_failed="workflow.failed",
        ),
    )

    import runtime.review_tracker as review_tracker_module

    monkeypatch.setattr(review_tracker_module, "get_review_tracker", lambda: _ReviewTracker())
    monkeypatch.setattr(
        _recording,
        "_record_task_type_route_feedback",
        lambda *, result, spec: calls.append(
            (
                (spec.task_type, result.provider_slug, result.model_slug),
                {"succeeded": True, "failure_code": result.failure_code},
            )
        ),
    )

    spec = WorkflowSpec(prompt="test", task_type="build")
    result = _result(provider_slug="openai", model_slug="gpt-5.4")

    record_workflow_result(result, spec=spec)

    assert calls == [
        (
            ("build", "openai", "gpt-5.4"),
            {"succeeded": True, "failure_code": None},
        )
    ]


def test_emit_workflow_events_use_capability_event_logger(monkeypatch):
    from runtime.workflow import _recording

    event_logger = _Recorder()
    monkeypatch.setattr(
        _recording._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(
            event_logger=event_logger,
            event_type_started="workflow.started",
            event_type_completed="workflow.completed",
            event_type_failed="workflow.failed",
        ),
    )

    spec = WorkflowSpec(prompt="test", provider_slug="openai", model_slug="gpt-5.4", label="job-a")
    success = _result(run_id="run_ok", provider_slug="openai", model_slug="gpt-5.4")
    failed = _result(
        run_id="run_fail",
        status="failed",
        reason_code="workflow.failed",
        completion=None,
        provider_slug="openai",
        model_slug="gpt-5.4",
        failure_code="workflow.timeout",
        attempts=2,
    )

    emit_workflow_started(spec=spec, run_id=success.run_id)
    emit_workflow_finished(spec=spec, result=success)
    emit_workflow_finished(spec=spec, result=failed)

    assert event_logger.calls == [
        (
            ("workflow.started",),
            {
                "source": "workflow.runtime",
                "run_id": "run_ok",
                "provider": "openai",
                "model": "gpt-5.4",
                "payload": {
                    "run_id": "run_ok",
                    "adapter_type": "cli_llm",
                    "timeout": 300,
                    "tier": None,
                    "label": "job-a",
                },
            },
        ),
        (
            ("workflow.completed",),
            {
                "source": "workflow.runtime",
                "run_id": "run_ok",
                "provider": "openai",
                "model": "gpt-5.4",
                "payload": {
                    "status": "succeeded",
                    "reason_code": "ok",
                    "latency_ms": 5000,
                    "attempts": 1,
                    "evidence_count": 4,
                    "label": "job-a",
                },
            },
        ),
        (
            ("workflow.failed",),
            {
                "source": "workflow.runtime",
                "run_id": "run_fail",
                "provider": "openai",
                "model": "gpt-5.4",
                "payload": {
                    "status": "failed",
                    "reason_code": "workflow.failed",
                    "latency_ms": 5000,
                    "attempts": 2,
                    "evidence_count": 4,
                    "label": "job-a",
                    "failure_code": "workflow.timeout",
                },
            },
        ),
    ]
