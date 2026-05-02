from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.workflow._workflow_policy import (
    apply_workflow_preflight,
    run_workflow_with_retry,
)
from runtime.result_cache import CacheKey
from runtime.workflow._workflow_execution import WorkflowExecutionContext
from runtime.workflow.orchestrator import WorkflowResult, WorkflowSpec


class _FakeCircuitBreakers:
    def __init__(self, allowed: bool) -> None:
        self.allowed = allowed
        self.providers = []

    def allow_request(self, provider_slug: str) -> bool:
        self.providers.append(provider_slug)
        return self.allowed


class _ExplodingCircuitBreakers:
    def allow_request(self, provider_slug: str) -> bool:
        raise RuntimeError(f"{provider_slug} override lookup failed")


class _FakeCache:
    def __init__(self, result=None) -> None:
        self.result = result
        self.keys = []
        self.put_calls = []

    def compute_key(self, spec) -> str:
        self.keys.append(spec.prompt)
        return f"key:{spec.prompt}"

    def get(self, key: str):
        return self.result

    def put(self, key: str, result, *, ttl_hours: float) -> None:
        self.put_calls.append((key, result, ttl_hours))


def _context() -> WorkflowExecutionContext:
    return WorkflowExecutionContext(
        provider_slug="anthropic",
        model_slug="claude-test",
        adapter_type="cli_llm",
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        start_ns=0,
    )


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
        capabilities=None,
        author_model="anthropic/claude-test",
        reviews_workflow_id=None,
        review_target_modules=None,
    )
    defaults.update(overrides)
    return WorkflowResult(**defaults)


def test_workflow_spec_construction_does_not_resolve_registry_defaults(monkeypatch):
    from runtime.workflow import orchestrator as orchestrator_module

    monkeypatch.setattr(
        orchestrator_module,
        "default_provider_slug",
        lambda: (_ for _ in ()).throw(AssertionError("provider registry touched")),
    )
    monkeypatch.setattr(
        orchestrator_module,
        "resolve_default_adapter_type",
        lambda _provider_slug=None: (_ for _ in ()).throw(AssertionError("adapter registry touched")),
    )

    spec = WorkflowSpec(prompt="test")

    assert spec.provider_slug is None
    assert spec.adapter_type is None


def test_apply_workflow_preflight_returns_circuit_breaker_failure(monkeypatch):
    from runtime.workflow import _workflow_policy as policy_module

    circuit_breakers = _FakeCircuitBreakers(allowed=False)
    monkeypatch.setattr(policy_module._workflow_caps, "CIRCUIT_BREAKERS", circuit_breakers)
    monkeypatch.setattr(
        policy_module._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(result_cache=None),
    )

    result = apply_workflow_preflight(
        WorkflowSpec(prompt="test", provider_slug="openai"),
        context=_context(),
        run_id_factory=lambda: "run_open",
    )

    assert circuit_breakers.providers == ["openai"]
    assert result.run_id == "run_open"
    assert result.reason_code == "circuit_breaker.open"
    assert result.failure_code == "rate_limited"


def test_apply_workflow_preflight_fails_closed_when_circuit_authority_missing(monkeypatch):
    from runtime.workflow import _workflow_policy as policy_module

    monkeypatch.setattr(policy_module._workflow_caps, "CIRCUIT_BREAKERS", None)
    monkeypatch.setattr(
        policy_module._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(result_cache=None),
    )

    result = apply_workflow_preflight(
        WorkflowSpec(prompt="test", provider_slug="openai"),
        context=_context(),
        run_id_factory=lambda: "run_closed",
    )

    assert result.run_id == "run_closed"
    assert result.reason_code == "circuit_breaker.unavailable"
    assert result.failure_code == "circuit_breaker.unavailable"
    assert "Circuit breaker authority unavailable" in result.outputs["error"]


def test_apply_workflow_preflight_fails_closed_when_circuit_check_errors(monkeypatch):
    from runtime.workflow import _workflow_policy as policy_module

    monkeypatch.setattr(
        policy_module._workflow_caps,
        "CIRCUIT_BREAKERS",
        _ExplodingCircuitBreakers(),
    )
    monkeypatch.setattr(
        policy_module._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(result_cache=None),
    )

    result = apply_workflow_preflight(
        WorkflowSpec(prompt="test", provider_slug="openai"),
        context=_context(),
        run_id_factory=lambda: "run_closed",
    )

    assert result.run_id == "run_closed"
    assert result.reason_code == "circuit_breaker.unavailable"
    assert result.failure_code == "circuit_breaker.unavailable"
    assert "override lookup failed" in result.outputs["error"]


def test_apply_workflow_preflight_returns_cached_result_with_marker(monkeypatch):
    from runtime.workflow import _workflow_policy as policy_module

    cached = _result(run_id="cached_run", outputs={"value": 7})
    cache = _FakeCache(result=cached)
    monkeypatch.setattr(policy_module._workflow_caps, "CIRCUIT_BREAKERS", None)
    monkeypatch.setattr(
        policy_module._workflow_caps,
        "WORKFLOW_CAPABILITIES",
        SimpleNamespace(result_cache=lambda: cache),
    )

    result = apply_workflow_preflight(
        WorkflowSpec(prompt="cache me", use_cache=True),
        context=_context(),
        run_id_factory=lambda: "unused",
    )

    assert cache.keys == ["cache me"]
    assert result.run_id == "cached_run"
    assert result.outputs == {"value": 7, "cache_hit": True}


def test_result_cache_key_includes_execution_authority_inputs():
    base = WorkflowSpec(
        prompt="same prompt",
        provider_slug="anthropic",
        model_slug="claude-test",
        system_prompt="same system",
        use_cache=True,
    )
    base_key = CacheKey.compute(base)

    variants = [
        WorkflowSpec(
            prompt=base.prompt,
            provider_slug=base.provider_slug,
            model_slug=base.model_slug,
            system_prompt=base.system_prompt,
            workspace_ref="workspace:other",
            use_cache=True,
        ),
        WorkflowSpec(
            prompt=base.prompt,
            provider_slug=base.provider_slug,
            model_slug=base.model_slug,
            system_prompt=base.system_prompt,
            runtime_profile_ref="runtime:other",
            use_cache=True,
        ),
        WorkflowSpec(
            prompt=base.prompt,
            provider_slug=base.provider_slug,
            model_slug=base.model_slug,
            system_prompt=base.system_prompt,
            allowed_tools=["praxis_submit", "praxis_bugs"],
            use_cache=True,
        ),
        WorkflowSpec(
            prompt=base.prompt,
            provider_slug=base.provider_slug,
            model_slug=base.model_slug,
            system_prompt=base.system_prompt,
            output_schema={"type": "object", "required": ["answer"]},
            use_cache=True,
        ),
        WorkflowSpec(
            prompt=base.prompt,
            provider_slug=base.provider_slug,
            model_slug=base.model_slug,
            system_prompt=base.system_prompt,
            task_type="architecture_review",
            use_cache=True,
        ),
        WorkflowSpec(
            prompt=base.prompt,
            provider_slug=base.provider_slug,
            model_slug=base.model_slug,
            system_prompt=base.system_prompt,
            scope_write=["runtime/result_cache.py"],
            use_cache=True,
        ),
    ]

    assert all(CacheKey.compute(variant) != base_key for variant in variants)


def test_result_cache_key_is_stable_for_equivalent_structured_specs():
    left = WorkflowSpec(
        prompt="same prompt",
        provider_slug="anthropic",
        model_slug="claude-test",
        use_cache=True,
        output_schema={"required": ["answer"], "type": "object"},
        context_sections=[
            {"title": "Facts", "body": "Use the durable authority."},
        ],
    )
    right = WorkflowSpec(
        prompt="same prompt",
        provider_slug="anthropic",
        model_slug="claude-test",
        use_cache=True,
        output_schema={"type": "object", "required": ["answer"]},
        context_sections=[
            {"body": "Use the durable authority.", "title": "Facts"},
        ],
    )

    assert CacheKey.compute(left) == CacheKey.compute(right)


def test_workflow_with_retry_retries_then_escalates_and_records(monkeypatch):
    from runtime.workflow import _workflow_policy as policy_module

    results = [
        _result(run_id="run_1", status="failed", reason_code="retry", completion=None, outputs={}, failure_code="workflow.timeout"),
        _result(run_id="run_2", status="failed", reason_code="retry", completion=None, outputs={}, failure_code="workflow.timeout"),
        _result(run_id="run_3", status="succeeded", reason_code="ok", outputs={"done": True}, failure_code=None),
    ]
    dispatched_tiers = []
    started = []
    finished = []
    recorded = []
    sleeps = []

    def _dispatch_once(spec: WorkflowSpec) -> WorkflowResult:
        dispatched_tiers.append((spec.tier, spec.model_slug))
        return results.pop(0)

    spec = WorkflowSpec(prompt="x", tier="economy", max_retries=1, model_slug="claude-sonnet")
    result = run_workflow_with_retry(
        spec,
        dispatch_once=_dispatch_once,
        emit_started=lambda **kwargs: started.append(kwargs),
        emit_finished=lambda **kwargs: finished.append(kwargs),
        record_result=lambda result, *, spec: recorded.append((result, spec)),
        sleep_fn=sleeps.append,
    )

    assert dispatched_tiers == [
        ("economy", "claude-sonnet"),
        ("economy", "claude-sonnet"),
        ("mid", None),
    ]
    assert sleeps == [1.0]
    assert result.run_id == "run_3"
    assert result.attempts == 3
    assert started == [{"spec": spec, "run_id": "run_1"}]
    assert finished == [{"spec": spec, "result": result}]
    assert recorded == [(result, spec)]


def test_workflow_with_retry_uses_adapter_http_error_contract(monkeypatch):
    from runtime.workflow import _workflow_policy as policy_module

    results = [
        _result(
            run_id="run_http_1",
            status="failed",
            reason_code="retry",
            completion=None,
            outputs={
                "status_code": 429,
                "stderr": "HTTP 429 rate limit exceeded",
            },
            failure_code="adapter.http_error",
        ),
        _result(
            run_id="run_http_2",
            status="succeeded",
            reason_code="ok",
            outputs={"done": True},
            failure_code=None,
        ),
    ]
    dispatched = []

    def _dispatch_once(spec: WorkflowSpec) -> WorkflowResult:
        dispatched.append((spec.tier, spec.model_slug))
        return results.pop(0)

    spec = WorkflowSpec(prompt="x", max_retries=1)
    result = run_workflow_with_retry(
        spec,
        dispatch_once=_dispatch_once,
        emit_started=lambda **kwargs: None,
        emit_finished=lambda **kwargs: None,
        record_result=lambda result, *, spec: None,
        sleep_fn=lambda seconds: None,
    )

    assert dispatched == [(None, None), (None, None)]
    assert result.run_id == "run_http_2"
    assert result.attempts == 2
