"""Tests for auto_retry and retry_context modules."""

import sys
import importlib
import pytest

# The runtime/__init__.py imports modules that use slots=True (Python 3.10+).
# We need to import our specific modules directly to avoid that.
import importlib.util
import pathlib

def _import_module(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_BASE = str(pathlib.Path(__file__).resolve().parents[2])
auto_retry = _import_module("runtime.auto_retry", f"{_BASE}/runtime/auto_retry.py")
retry_context = _import_module("runtime.retry_context", f"{_BASE}/runtime/retry_context.py")
failure_classifier = _import_module("runtime.failure_classifier", f"{_BASE}/runtime/failure_classifier.py")
retry_orchestrator = _import_module("runtime.retry_orchestrator", f"{_BASE}/runtime/retry_orchestrator.py")

AutoRetryManager = auto_retry.AutoRetryManager
FailureCategory = auto_retry.FailureCategory
RetryClassification = auto_retry.RetryClassification
RetryDecision = auto_retry.RetryDecision
RetryPolicy = auto_retry.RetryPolicy
RetryContextBuilder = retry_context.RetryContextBuilder
FailureClassification = failure_classifier.FailureClassification
OrchestratorFailureCategory = failure_classifier.FailureCategory


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture
def manager() -> AutoRetryManager:
    policy = RetryPolicy(max_retries=3, escalate_after=2, backoff_seconds=(5, 15, 60))
    return AutoRetryManager(policy)


@pytest.fixture
def ctx_builder() -> RetryContextBuilder:
    return RetryContextBuilder()


# ── Classification tests ──────────────────────────────────────────────────

class TestClassification:

    def test_exit_code_124_is_transient(self, manager: AutoRetryManager):
        c = manager.classify("timeout", "", exit_code=124)
        assert c.category == FailureCategory.TRANSIENT
        assert c.retryable is True

    def test_timeout_in_stderr_is_transient(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout after 30s")
        assert c.category == FailureCategory.TRANSIENT

    def test_rate_limit_is_transient(self, manager: AutoRetryManager):
        c = manager.classify("err", "HTTP 429 rate limit exceeded")
        assert c.category == FailureCategory.TRANSIENT
        assert c.retryable is True

    def test_idle_timeout_is_transient(self, manager: AutoRetryManager):
        c = manager.classify("err", "Process killed: idle timeout reached")
        assert c.category == FailureCategory.TRANSIENT

    def test_scope_violation_is_non_retryable(self, manager: AutoRetryManager):
        c = manager.classify("err", "scope violation: wrote to /etc/passwd")
        assert c.category == FailureCategory.NON_RETRYABLE
        assert c.retryable is False
        assert c.suggested_action == "halt"

    def test_provider_disabled_is_scope_violation_and_non_retryable(self):
        c = failure_classifier.classify_failure("provider_disabled")
        assert c.category == OrchestratorFailureCategory.SCOPE_VIOLATION
        assert c.is_retryable is False
        assert c.is_transient is False

    def test_isolation_breach_is_non_retryable(self, manager: AutoRetryManager):
        c = manager.classify("err", "isolation breach detected in sandbox")
        assert c.category == FailureCategory.NON_RETRYABLE
        assert c.retryable is False

    def test_prompt_injection_is_non_retryable(self, manager: AutoRetryManager):
        c = manager.classify("err", "prompt injection detected in output")
        assert c.category == FailureCategory.NON_RETRYABLE

    def test_json_parse_error_is_triageable(self, manager: AutoRetryManager):
        c = manager.classify("err", "Failed to json parse response body")
        assert c.category == FailureCategory.TRIAGEABLE
        assert c.retryable is True
        assert c.suggested_action == "retry_with_context"

    def test_test_failure_is_triageable(self, manager: AutoRetryManager):
        c = manager.classify("err", "tests/test_foo.py::test_bar FAILED")
        assert c.category == FailureCategory.TRIAGEABLE

    def test_assertion_error_is_triageable(self, manager: AutoRetryManager):
        c = manager.classify("err", "AssertionError: expected 1 got 2")
        assert c.category == FailureCategory.TRIAGEABLE

    def test_unknown_failure_treated_as_unknown(self, manager: AutoRetryManager):
        c = manager.classify("weird_code", "something unexpected happened")
        assert c.category == FailureCategory.UNKNOWN
        assert c.retryable is True

    def test_adapter_http_error_with_status_code_is_rate_limit(self):
        c = failure_classifier.classify_failure(
            "adapter.http_error",
            outputs={"status_code": 429, "stderr": "HTTP 429 rate limit exceeded"},
        )
        assert c.category == OrchestratorFailureCategory.RATE_LIMIT
        assert c.is_retryable is True

    def test_cli_unexpected_argument_is_input_error_not_provider_error(self):
        c = failure_classifier.classify_failure(
            "cli_adapter.nonzero_exit",
            outputs={
                "stderr": "error: unexpected argument '--read-dir' found\nUsage: codex exec --model <MODEL> --add-dir <DIR> [PROMPT]",
                "exit_code": 2,
            },
        )
        assert c.category == OrchestratorFailureCategory.INPUT_ERROR
        assert c.is_retryable is False

    def test_cli_missing_prompt_contract_is_input_error_not_provider_error(self):
        c = failure_classifier.classify_failure(
            "cli_adapter.nonzero_exit",
            outputs={
                "stderr": "Error: Input must be provided either through stdin or as a prompt argument when using --print",
                "exit_code": 1,
            },
        )
        assert c.category == OrchestratorFailureCategory.INPUT_ERROR
        assert c.is_retryable is False

    def test_missing_workflow_submission_is_non_retryable_infrastructure(self):
        c = failure_classifier.classify_failure("workflow_submission.required_missing")

        assert c.category == OrchestratorFailureCategory.INFRASTRUCTURE
        assert c.is_retryable is False
        assert "sealed workflow_job_submissions row" in c.recommended_action

    def test_host_resource_capacity_is_transient_timeout_not_provider_failover(self):
        c = failure_classifier.classify_failure("host_resource_capacity")

        assert c.category == OrchestratorFailureCategory.TIMEOUT
        assert c.is_retryable is True
        assert c.is_transient is True

    def test_host_resource_admission_unavailable_is_transient_timeout(self):
        c = failure_classifier.classify_failure("host_resource_admission_unavailable")

        assert c.category == OrchestratorFailureCategory.TIMEOUT
        assert c.is_retryable is True
        assert c.is_transient is True


# ── Retry decision tests ─────────────────────────────────────────────────

class TestRetryDecision:

    def test_respects_max_retries(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout")
        # Exhaust all retries
        for _ in range(3):
            manager.record_attempt("job-A", c)
        decision = manager.should_retry("job-A", c)
        assert decision.retry is False
        assert decision.action == "skip"

    def test_allows_retry_within_limit(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout")
        decision = manager.should_retry("job-B", c)
        assert decision.retry is True
        assert decision.attempt_number == 1

    def test_non_retryable_immediately_false(self, manager: AutoRetryManager):
        c = manager.classify("err", "scope violation detected")
        decision = manager.should_retry("job-C", c)
        assert decision.retry is False
        assert decision.action == "halt"

    def test_escalation_triggers_after_threshold(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout")
        # Record 2 attempts (escalate_after=2)
        manager.record_attempt("job-D", c)
        manager.record_attempt("job-D", c)
        decision = manager.should_retry("job-D", c)
        assert decision.retry is True
        assert decision.escalate_tier is True

    def test_no_escalation_before_threshold(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout")
        manager.record_attempt("job-E", c)
        decision = manager.should_retry("job-E", c)
        assert decision.escalate_tier is False

    def test_backoff_timing(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout")
        # First attempt
        d1 = manager.should_retry("job-F", c)
        assert d1.wait_seconds == 5
        manager.record_attempt("job-F", c)
        # Second attempt
        d2 = manager.should_retry("job-F", c)
        assert d2.wait_seconds == 15
        manager.record_attempt("job-F", c)
        # Third attempt
        d3 = manager.should_retry("job-F", c)
        assert d3.wait_seconds == 60

    def test_escalation_changes_action(self, manager: AutoRetryManager):
        """After escalate_after, retry_same_agent becomes retry_escalated_agent."""
        c = manager.classify("err", "Connection timeout")  # suggests retry_same_agent
        assert c.suggested_action == "retry_same_agent"
        manager.record_attempt("job-G", c)
        manager.record_attempt("job-G", c)
        decision = manager.should_retry("job-G", c)
        assert decision.action == "retry_escalated_agent"


# ── Attempt tracking tests ───────────────────────────────────────────────

class TestAttemptTracking:

    def test_tracks_across_multiple_failures(self, manager: AutoRetryManager):
        c1 = manager.classify("err", "Connection timeout")
        c2 = manager.classify("err", "Failed to json parse output")
        manager.record_attempt("job-H", c1)
        manager.record_attempt("job-H", c2)
        assert manager.attempt_count("job-H") == 2

    def test_separate_jobs_tracked_independently(self, manager: AutoRetryManager):
        c = manager.classify("err", "Connection timeout")
        manager.record_attempt("job-I", c)
        manager.record_attempt("job-I", c)
        manager.record_attempt("job-J", c)
        assert manager.attempt_count("job-I") == 2
        assert manager.attempt_count("job-J") == 1

    def test_unknown_job_has_zero_attempts(self, manager: AutoRetryManager):
        assert manager.attempt_count("never-seen") == 0


# ── Retry context builder tests ──────────────────────────────────────────

class TestRetryContextBuilder:

    def test_produces_header(self, ctx_builder: RetryContextBuilder):
        block = ctx_builder.build("job-X", "timeout", "Connection timeout")
        assert "PREVIOUS ATTEMPT FAILED" in block
        assert "job-X" in block

    def test_transient_guidance(self, ctx_builder: RetryContextBuilder):
        block = ctx_builder.build("j", "timeout", "Connection timeout")
        assert "transient failure" in block.lower()
        assert "Retry the same approach" in block

    def test_triageable_guidance(self, ctx_builder: RetryContextBuilder):
        block = ctx_builder.build("j", "parse", "json parse error on line 5")
        assert "Previous output had issues" in block
        assert "Adjust your approach" in block

    def test_unknown_guidance(self, ctx_builder: RetryContextBuilder):
        block = ctx_builder.build("j", "mystery", "something weird")
        assert "unclear reasons" in block
        assert "different strategy" in block

    def test_stderr_excerpt_included(self, ctx_builder: RetryContextBuilder):
        stderr = "line1\nline2\nerror: bad thing\nline4"
        block = ctx_builder.build("j", "mystery", stderr)
        assert "stderr excerpt" in block
        assert "error: bad thing" in block

    def test_stderr_truncated_to_50_lines(self, ctx_builder: RetryContextBuilder):
        stderr = "\n".join(f"log line {i}" for i in range(100))
        block = ctx_builder.build("j", "mystery", stderr)
        # Should contain last 50 lines, not first
        assert "log line 99" in block
        assert "log line 49" not in block


# ── Key error extraction tests ───────────────────────────────────────────

class TestKeyErrorExtraction:

    def test_extracts_error_line(self, ctx_builder: RetryContextBuilder):
        stderr = "loading config\nError: missing field 'name'\ndone"
        result = ctx_builder.extract_key_error(stderr)
        assert "Error: missing field" in result

    def test_extracts_exception_line(self, ctx_builder: RetryContextBuilder):
        stderr = "starting\nValueError: invalid literal\ncleaning up"
        result = ctx_builder.extract_key_error(stderr)
        assert "ValueError" in result

    def test_extracts_failed_line(self, ctx_builder: RetryContextBuilder):
        stderr = "running tests\ntest_foo.py::test_bar FAILED\n2 passed"
        result = ctx_builder.extract_key_error(stderr)
        assert "FAILED" in result

    def test_fallback_to_last_line(self, ctx_builder: RetryContextBuilder):
        stderr = "just some output\nnothing special here"
        result = ctx_builder.extract_key_error(stderr)
        assert result == "nothing special here"

    def test_empty_stderr_returns_placeholder(self, ctx_builder: RetryContextBuilder):
        result = ctx_builder.extract_key_error("")
        assert "no error details" in result

    def test_realistic_traceback(self, ctx_builder: RetryContextBuilder):
        stderr = """Traceback (most recent call last):
  File "run.py", line 42, in main
    result = process(data)
  File "run.py", line 10, in process
    raise RuntimeError("output format invalid")
RuntimeError: output format invalid"""
        result = ctx_builder.extract_key_error(stderr)
        assert "Traceback" in result or "Error" in result.lower()


class TestRetryOrchestrator:

    def test_rate_limit_failover_wins_over_same_model_retry(self):
        decision = retry_orchestrator.decide(
            error_code="rate_limited",
            stderr="HTTP 429 rate limit exceeded",
            attempt=1,
            max_attempts=3,
            failover_chain=["google/gemini-3.1-pro-preview", "openai/gpt-5.4"],
            resolved_agent="google/gemini-3.1-pro-preview",
        )

        assert decision.action == "failover"
        assert decision.next_agent == "openai/gpt-5.4"
        assert decision.should_requeue is True

    def test_non_retryable_preclassification_forces_terminal_failure(self):
        pre_classified = FailureClassification(
            category=OrchestratorFailureCategory.CREDENTIAL_ERROR,
            is_retryable=False,
            is_transient=False,
            recommended_action="Fix credentials before retrying.",
            severity="medium",
        )

        decision = retry_orchestrator.decide(
            error_code="rate_limited",
            stderr="HTTP 429 rate limit exceeded",
            attempt=1,
            max_attempts=3,
            failover_chain=["google/gemini-3.1-pro-preview", "openai/gpt-5.4"],
            resolved_agent="google/gemini-3.1-pro-preview",
            pre_classified=pre_classified,
        )

        assert decision.action == "fail"
        assert decision.next_agent is None
        assert decision.should_requeue is False

    def test_adapter_http_error_rate_limit_fails_over(self):
        pre_classified = failure_classifier.classify_failure(
            "adapter.http_error",
            outputs={"status_code": 429, "stderr": "HTTP 429 rate limit exceeded"},
        )

        decision = retry_orchestrator.decide(
            error_code="adapter.http_error",
            stderr="HTTP 429 rate limit exceeded",
            attempt=1,
            max_attempts=3,
            failover_chain=["google/gemini-3.1-pro-preview", "openai/gpt-5.4"],
            resolved_agent="google/gemini-3.1-pro-preview",
            pre_classified=pre_classified,
        )

        assert decision.action == "failover"
        assert decision.next_agent == "openai/gpt-5.4"
        assert decision.should_requeue is True

    def test_host_resource_capacity_retries_same_agent(self):
        pre_classified = failure_classifier.classify_failure("host_resource_capacity")

        decision = retry_orchestrator.decide(
            error_code="host_resource_capacity",
            stderr="Host resource at capacity",
            attempt=1,
            max_attempts=3,
            failover_chain=["google/gemini-3.1-pro-preview", "openai/gpt-5.4"],
            resolved_agent="google/gemini-3.1-pro-preview",
            pre_classified=pre_classified,
        )

        assert decision.action == "retry_same"
        assert decision.next_agent == "google/gemini-3.1-pro-preview"
        assert decision.should_requeue is True
