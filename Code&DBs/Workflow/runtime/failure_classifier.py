"""Failure classification — categorize dispatch failures by type and retryability.

Provides structured failure analysis for diagnostics, circuit breaking, and
retry decision-making. Supports pattern-matching against failure codes from
adapters, LLM clients, and the dispatch engine itself.

Usage:
    >>> classification = classify_failure("llm_client.http_error", outputs={"status_code": 429})
    >>> print(classification.category)
    FailureCategory.RATE_LIMIT
    >>> print(classification.is_retryable)
    True
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any


class FailureCategory(str, Enum):
    """Priority-ordered failure categories for dispatch failures."""

    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    PROVIDER_ERROR = "provider_error"
    NETWORK_ERROR = "network_error"
    INPUT_ERROR = "input_error"
    CREDENTIAL_ERROR = "credential_error"
    CONTEXT_OVERFLOW = "context_overflow"
    PARSE_ERROR = "parse_error"
    MODEL_ERROR = "model_error"
    SANDBOX_ERROR = "sandbox_error"
    SCOPE_VIOLATION = "scope_violation"
    VERIFICATION_FAILED = "verification_failed"
    INFRASTRUCTURE = "infrastructure"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FailureClassification:
    """Structured failure classification and metadata.

    Attributes
    ----------
    category : FailureCategory
        The failure type.
    is_retryable : bool
        Whether a retry is likely to succeed.
    is_transient : bool
        Whether the failure is temporary vs permanent.
    recommended_action : str
        Human-readable next step.
    severity : str
        "critical", "high", "medium", or "low".
    """

    category: FailureCategory
    is_retryable: bool
    is_transient: bool
    recommended_action: str
    severity: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict."""
        return {
            "category": self.category.value,
            "is_retryable": self.is_retryable,
            "is_transient": self.is_transient,
            "recommended_action": self.recommended_action,
            "severity": self.severity,
        }


# ---------------------------------------------------------------------------
# Pattern matchers (priority-ordered)
# ---------------------------------------------------------------------------

def _classify_signal(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a signal-based termination or known exit code."""
    if failure_code == "exit_130":
        # Exit code 130 = 128 + SIGINT(2) — user cancel or orchestrator interrupt
        return FailureClassification(
            category=FailureCategory.INFRASTRUCTURE,
            is_retryable=True,
            is_transient=True,
            recommended_action="Process was interrupted (SIGINT). Retry the job.",
            severity="medium",
        )
    if failure_code == "exit_124":
        # Exit code 124 = GNU timeout command — process timed out
        return FailureClassification(
            category=FailureCategory.TIMEOUT,
            is_retryable=True,
            is_transient=True,
            recommended_action="Process timed out (exit 124). Retry with longer timeout or simpler prompt.",
            severity="high",
        )
    if failure_code == "setup_failure":
        return FailureClassification(
            category=FailureCategory.INFRASTRUCTURE,
            is_retryable=True,
            is_transient=True,
            recommended_action="Setup/initialization failed. Check environment and retry.",
            severity="high",
        )
    if failure_code == "google_auth_not_configured":
        return FailureClassification(
            category=FailureCategory.CREDENTIAL_ERROR,
            is_retryable=False,
            is_transient=False,
            recommended_action="Configure GOOGLE_OAUTH_TOKEN to enable Google integrations.",
            severity="medium",
        )
    if failure_code == "tool_use_error":
        return FailureClassification(
            category=FailureCategory.SANDBOX_ERROR,
            is_retryable=True,
            is_transient=True,
            recommended_action="Agent tool execution failed. Retry with refined prompt.",
            severity="medium",
        )
    if outputs and isinstance(outputs, dict):
        exit_code = outputs.get("exit_code")
        if exit_code == 130:
            return FailureClassification(
                category=FailureCategory.INFRASTRUCTURE,
                is_retryable=True,
                is_transient=True,
                recommended_action="Process was interrupted (SIGINT). Retry the job.",
                severity="medium",
            )
        if exit_code == 124:
            return FailureClassification(
                category=FailureCategory.TIMEOUT,
                is_retryable=True,
                is_transient=True,
                recommended_action="Process timed out (exit 124). Retry with longer timeout or simpler prompt.",
                severity="high",
            )
    return None


def _classify_timeout(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a timeout."""
    if failure_code in (
        "dispatch.timeout",
        "cli_adapter.timeout",
        "adapter.timeout",
        "llm_client.timeout",
        "workflow.timeout",
        "timeout",
    ):
        return FailureClassification(
            category=FailureCategory.TIMEOUT,
            is_retryable=True,
            is_transient=True,
            recommended_action="Retry with --tier auto to fail over to an alternate provider.",
            severity="high",
        )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        if re.search(r"timed?\s*out|timeout|etimedout", stderr, re.IGNORECASE):
            return FailureClassification(
                category=FailureCategory.TIMEOUT,
                is_retryable=True,
                is_transient=True,
                recommended_action="Retry with --tier auto to fail over to an alternate provider.",
                severity="high",
            )
    return None


def _classify_rate_limit(
    failure_code: str,
    *,
    outputs: dict | None = None,
) -> FailureClassification | None:
    """Check if failure is rate limiting."""
    # Direct rate-limit codes from worker/queue_worker error classification
    if failure_code in ("rate_limit", "rate_limited", "quota_exceeded"):
        return FailureClassification(
            category=FailureCategory.RATE_LIMIT,
            is_retryable=True,
            is_transient=True,
            recommended_action="Rate limited. Failover to next provider in chain.",
            severity="high",
        )

    if failure_code in ("route.unhealthy", "circuit.open"):
        return FailureClassification(
            category=FailureCategory.RATE_LIMIT,
            is_retryable=True,
            is_transient=True,
            recommended_action="Retry with --tier auto to fail over to an alternate provider.",
            severity="high",
        )

    if failure_code in ("llm_client.http_error", "adapter.http_error"):
        status_code = None
        if outputs and isinstance(outputs, dict):
            status_code = outputs.get("status_code")
        if status_code == 408:
            return FailureClassification(
                category=FailureCategory.TIMEOUT,
                is_retryable=True,
                is_transient=True,
                recommended_action="Request timed out. Retry with a longer timeout or simpler prompt.",
                severity="high",
            )
        if status_code == 429:
            return FailureClassification(
                category=FailureCategory.RATE_LIMIT,
                is_retryable=True,
                is_transient=True,
                recommended_action="Rate limited. Retry with exponential backoff.",
                severity="high",
            )

    # Check for rate limit patterns in cli_adapter.nonzero_exit stderr
    if failure_code == "cli_adapter.nonzero_exit":
        if outputs and isinstance(outputs, dict):
            stderr = outputs.get("stderr", "")
            if isinstance(stderr, str) and _has_rate_limit_pattern(stderr):
                return FailureClassification(
                    category=FailureCategory.RATE_LIMIT,
                    is_retryable=True,
                    is_transient=True,
                    recommended_action="Rate limited. Retry with exponential backoff.",
                    severity="high",
                )

    return None


def _classify_provider_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a provider-side error."""
    if failure_code in ("llm_client.http_error", "adapter.http_error"):
        if outputs and isinstance(outputs, dict):
            status_code = outputs.get("status_code")
            if status_code == 429:
                return None
        # 5xx status codes (and some 4xx like 503) are provider errors
        # Already handled 429 in rate_limit check, so this catches 5xx
        return FailureClassification(
            category=FailureCategory.PROVIDER_ERROR,
            is_retryable=True,
            is_transient=True,
            recommended_action="Provider error. Retry with --tier auto to fail over.",
            severity="high",
        )

    # Check for provider errors in cli_adapter.nonzero_exit
    if failure_code == "cli_adapter.nonzero_exit":
        if outputs and isinstance(outputs, dict):
            stderr = str(outputs.get("stderr", "")).lower()
            # Check for tool-use errors (CLI tool execution failures)
            if "tool" in stderr and ("error" in stderr or "failed" in stderr or "rejected" in stderr):
                return FailureClassification(
                    category=FailureCategory.SANDBOX_ERROR,
                    is_retryable=True,
                    is_transient=True,
                    recommended_action="Tool execution failed. Retry with refined prompt.",
                    severity="medium",
                )
            # Check for network errors buried in stderr
            if _has_network_error_pattern(stderr):
                return FailureClassification(
                    category=FailureCategory.NETWORK_ERROR,
                    is_retryable=True,
                    is_transient=True,
                    recommended_action="Network error. Retry with exponential backoff.",
                    severity="medium",
                )
            # Check for provider server errors in stderr
            if _has_provider_error_pattern(stderr):
                return FailureClassification(
                    category=FailureCategory.PROVIDER_ERROR,
                    is_retryable=True,
                    is_transient=True,
                    recommended_action="Provider error. Retry with --tier auto to fail over.",
                    severity="high",
                )
        # Default: CLI nonzero exit without specific error pattern is a sandbox/tooling
        # failure (our system), NOT a provider error. Don't blame the model.
        return FailureClassification(
            category=FailureCategory.SANDBOX_ERROR,
            is_retryable=True,
            is_transient=True,
            recommended_action="CLI process failed. Check logs and retry.",
            severity="medium",
        )

    if failure_code in ("dispatch.crash", "dispatch.thread_error"):
        return FailureClassification(
            category=FailureCategory.PROVIDER_ERROR,
            is_retryable=True,
            is_transient=True,
            recommended_action="Internal provider crash. Retry with --tier auto to fail over.",
            severity="critical",
        )

    return None


def _classify_network_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a network error."""
    if failure_code in ("llm_client.network_error", "adapter.network_error", "connection_error", "network_error"):
        return FailureClassification(
            category=FailureCategory.NETWORK_ERROR,
            is_retryable=True,
            is_transient=True,
            recommended_action="Network error. Retry with exponential backoff.",
            severity="medium",
        )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        if _has_network_error_pattern(stderr):
            return FailureClassification(
                category=FailureCategory.NETWORK_ERROR,
                is_retryable=True,
                is_transient=True,
                recommended_action="Network error. Retry with exponential backoff.",
                severity="medium",
            )
    return None


def _classify_input_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is due to invalid input."""
    if failure_code in (
        "adapter.input_invalid",
        "adapter.transport_unsupported",
        "adapter.model_required",
        "adapter.endpoint_unavailable",
        "api_task.url_required",
        "api_task.method_invalid",
        "api_task.body_serialization_error",
        "intake.rejected",
    ):
        return FailureClassification(
            category=FailureCategory.INPUT_ERROR,
            is_retryable=False,
            is_transient=False,
            recommended_action="Fix the workflow spec or input and retry.",
            severity="medium",
        )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        if _has_input_error_pattern(stderr):
            return FailureClassification(
                category=FailureCategory.INPUT_ERROR,
                is_retryable=False,
                is_transient=False,
                recommended_action="Fix the workflow spec or CLI input contract and retry.",
                severity="medium",
            )
    return None


def _classify_credential_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is due to credentials."""
    if failure_code in (
        "credential.provider_unknown",
        "credential.env_var_missing",
        "credential_error",
        "credential_invalid",
        "auth_error",
        "auth_failure",
        "permission_denied",
    ):
        return FailureClassification(
            category=FailureCategory.CREDENTIAL_ERROR,
            is_retryable=False,
            is_transient=False,
            recommended_action="Check provider credentials in environment and retry.",
            severity="high",
        )
    if failure_code in ("llm_client.http_error", "adapter.http_error"):
        if outputs and isinstance(outputs, dict):
            status_code = outputs.get("status_code")
            if status_code in (401, 403):
                return FailureClassification(
                    category=FailureCategory.CREDENTIAL_ERROR,
                    is_retryable=False,
                    is_transient=False,
                    recommended_action="Check provider credentials in environment and retry.",
                    severity="high",
                )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        if _has_auth_pattern(stderr):
            return FailureClassification(
                category=FailureCategory.CREDENTIAL_ERROR,
                is_retryable=False,
                is_transient=False,
                recommended_action="Check provider credentials in environment and retry.",
                severity="high",
            )
    return None


def _classify_verification_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a verification failure."""
    if "verification" in failure_code or failure_code == "verification.failed":
        return FailureClassification(
            category=FailureCategory.VERIFICATION_FAILED,
            is_retryable=False,
            is_transient=False,
            recommended_action="Verify the spec's registered verification bindings and expected outputs.",
            severity="medium",
        )
    return None


def _classify_scope_violation(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is due to scope or governance violations."""
    if "scope" in failure_code or "governance" in failure_code:
        return FailureClassification(
            category=FailureCategory.SCOPE_VIOLATION,
            is_retryable=False,
            is_transient=False,
            recommended_action="Update scope_read or scope_write in the spec.",
            severity="medium",
        )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        if _has_governance_pattern(stderr):
            return FailureClassification(
                category=FailureCategory.SCOPE_VIOLATION,
                is_retryable=False,
                is_transient=False,
                recommended_action="Fix governance or scope violation and retry.",
                severity="medium",
            )
    return None


def _classify_infrastructure(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is due to infrastructure issues."""
    if failure_code in (
        "dispatch.node_not_found",
        "dispatch.execution_timeout",
        "dispatch.execution_crash",
    ):
        return FailureClassification(
            category=FailureCategory.INFRASTRUCTURE,
            is_retryable=True,
            is_transient=True,
            recommended_action="Infrastructure error. Retry with --tier auto to fail over.",
            severity="high",
        )
    return None


def _classify_context_overflow(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a context-window overflow.

    Tightened to avoid false positives from platform boilerplate
    (e.g. '--- PLATFORM CONTEXT ---' in stderr).
    """
    code_lower = (failure_code or "").lower()
    # Only match failure codes that explicitly indicate context/token overflow
    for keyword in ("context_overflow", "context_length", "token_limit",
                     "too_long", "max_tokens", "context_window"):
        if keyword in code_lower:
            return FailureClassification(
                category=FailureCategory.CONTEXT_OVERFLOW,
                is_retryable=True,
                is_transient=False,
                recommended_action="Reduce prompt size or switch to a model with a larger context window.",
                severity="high",
            )
    # Check stderr / reason for specific context-window error patterns
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        reason = str(outputs.get("reason_code", "")).lower()
        for text in (stderr, reason):
            if _has_context_overflow_pattern(text):
                return FailureClassification(
                    category=FailureCategory.CONTEXT_OVERFLOW,
                    is_retryable=True,
                    is_transient=False,
                    recommended_action="Reduce prompt size or switch to a model with a larger context window.",
                    severity="high",
                )
    return None


def _classify_parse_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a downstream parse / format issue."""
    code_lower = (failure_code or "").lower()
    if failure_code in ("adapter.response_parse_error",):
        return FailureClassification(
            category=FailureCategory.PARSE_ERROR,
            is_retryable=True,
            is_transient=True,
            recommended_action="Retry — output format was malformed. Consider tightening the prompt.",
            severity="medium",
        )
    for keyword in ("parse", "decode", "format", "invalid_json", "malformed"):
        if keyword in code_lower:
            return FailureClassification(
                category=FailureCategory.PARSE_ERROR,
                is_retryable=True,
                is_transient=True,
                recommended_action="Retry — output format was malformed. Consider tightening the prompt.",
                severity="medium",
            )
    if outputs and isinstance(outputs, dict):
        reason = str(outputs.get("reason_code", "")).lower()
        for keyword in ("parse", "decode", "malformed"):
            if keyword in reason:
                return FailureClassification(
                    category=FailureCategory.PARSE_ERROR,
                    is_retryable=True,
                    is_transient=True,
                    recommended_action="Retry — output format was malformed. Consider tightening the prompt.",
                    severity="medium",
                )
    return None


def _classify_model_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a model-not-found or model-unavailable error."""
    if failure_code in ("model_not_found", "model_unavailable", "model_not_available", "agent_not_found"):
        return FailureClassification(
            category=FailureCategory.MODEL_ERROR,
            is_retryable=False,
            is_transient=False,
            recommended_action="Check model name or switch to an available model.",
            severity="high",
        )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        if "model" in stderr and ("not found" in stderr or "not available" in stderr or "not exist" in stderr):
            return FailureClassification(
                category=FailureCategory.MODEL_ERROR,
                is_retryable=False,
                is_transient=False,
                recommended_action="Check model name or switch to an available model.",
                severity="high",
            )
    return None


def _classify_sandbox_error(failure_code: str, *, outputs: dict | None = None) -> FailureClassification | None:
    """Check if failure is a sandbox, CLI tooling, or code execution error."""
    if failure_code in ("sandbox_error",):
        return FailureClassification(
            category=FailureCategory.SANDBOX_ERROR,
            is_retryable=False,
            is_transient=False,
            recommended_action="Check sandbox configuration and CLI availability.",
            severity="medium",
        )
    if outputs and isinstance(outputs, dict):
        stderr = str(outputs.get("stderr", "")).lower()
        # CLI / sandbox setup failures
        if any(kw in stderr for kw in ("show help", "show version", "--help", "no such file", "command not found")):
            return FailureClassification(
                category=FailureCategory.SANDBOX_ERROR,
                is_retryable=False,
                is_transient=False,
                recommended_action="Check sandbox configuration and CLI availability.",
                severity="medium",
            )
        # Code execution errors from generated code
        if any(kw in stderr for kw in (
            "importerror", "modulenotfounderror", "syntaxerror",
            "typeerror", "attributeerror",
        )):
            return FailureClassification(
                category=FailureCategory.SANDBOX_ERROR,
                is_retryable=True,
                is_transient=True,
                recommended_action="Generated code has errors. Retry with refined prompt.",
                severity="medium",
            )
        # Tool use failures
        if "tool" in stderr and ("error" in stderr or "failed" in stderr):
            return FailureClassification(
                category=FailureCategory.SANDBOX_ERROR,
                is_retryable=True,
                is_transient=True,
                recommended_action="Tool execution failed. Retry or check tool availability.",
                severity="medium",
            )
    return None


def _has_auth_pattern(text: str) -> bool:
    """Check if text contains common authentication error patterns."""
    patterns = [
        r"\b401\b",
        r"\b403\b",
        r"unauthorized",
        r"unauthenticated",
        r"api\s*key.*(invalid|missing|not set)",
        r"authentication\s+(cancelled|failed|error)",
        r"must\s+specify.*api_key",
        r"oauth.*(expired|invalid|refresh|cancelled)",
        r"permission.*denied",
        r"forbidden",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_network_error_pattern(text: str) -> bool:
    """Check if text contains common network error patterns."""
    patterns = [
        r"connection\s+(refused|reset|error)",
        r"\beconnreset\b",
        r"\benotfound\b",
        r"\beconnrefused\b",
        r"\betimedout\b",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_provider_error_pattern(text: str) -> bool:
    """Check if text contains common provider server error patterns."""
    patterns = [
        r"\b50[0234]\b",
        r"internal\s+server\s+error",
        r"server_error",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_governance_pattern(text: str) -> bool:
    """Check if text contains governance or scope violation patterns."""
    patterns = [
        r"secret.*detect",
        r"governance",
        r"scope.*violation",
        r"blocked.*pre.?dispatch",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_input_error_pattern(text: str) -> bool:
    """Check if stderr indicates a bad CLI contract or invalid request."""
    patterns = [
        r"unexpected\s+argument",
        r"unknown\s+option",
        r"missing\s+required\s+argument",
        r"input\s+must\s+be\s+provided.*stdin.*prompt\s+argument",
        r"invalid\s+value\s+for",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_context_overflow_pattern(text: str) -> bool:
    """Check if text contains actual context-window overflow patterns.

    Avoids false positives from boilerplate like '--- PLATFORM CONTEXT ---'.
    Requires multi-word patterns that indicate real context window errors.
    """
    patterns = [
        r"context.{0,10}(length|window|limit|overflow|exceed)",
        r"(token|input).{0,10}(limit|exceed|too.long|overflow|maximum)",
        r"max.?tokens?\s+(exceeded|limit|reached)",
        r"(prompt|request)\s+(too\s+long|exceed)",
        r"context_length_exceeded",
        r"(input|prompt)\s+is\s+too\s+(long|large)",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_rate_limit_pattern(text: str) -> bool:
    """Check if stderr contains common rate limit patterns."""
    patterns = [
        r"429\b",  # HTTP 429
        r"too\s+many\s+requests",
        r"rate\s+limit",
        r"quota",
        r"throttl",
    ]
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_failure(
    failure_code: str | None,
    *,
    outputs: dict | None = None,
) -> FailureClassification:
    """Classify a failure into a structured category.

    Applies priority-ordered pattern matching against failure codes from
    various sources (adapters, LLM clients, dispatch engine). Returns a
    FailureClassification with category, retryability, and recommended action.

    Parameters
    ----------
    failure_code : str | None
        The failure code from a workflow result. If None, returns UNKNOWN.
    outputs : dict | None
        Additional outputs that may contain context (e.g., status_code, stderr).

    Returns
    -------
    FailureClassification
        Structured classification including category, retryability, and action.
    """
    if failure_code is None:
        return FailureClassification(
            category=FailureCategory.UNKNOWN,
            is_retryable=False,
            is_transient=False,
            recommended_action="Inspect the full receipt for details; failure cause is unclear.",
            severity="low",
        )

    # Priority-ordered matchers
    matchers = [
        _classify_signal,
        _classify_timeout,
        _classify_rate_limit,
        _classify_context_overflow,
        _classify_parse_error,
        _classify_credential_error,
        _classify_model_error,
        _classify_input_error,
        _classify_verification_error,
        _classify_scope_violation,
        _classify_sandbox_error,
        _classify_network_error,
        _classify_infrastructure,
        _classify_provider_error,  # Catches remaining HTTP errors
    ]

    for matcher in matchers:
        result = matcher(failure_code, outputs=outputs)
        if result is not None:
            return result

    # Unknown failure
    return FailureClassification(
        category=FailureCategory.UNKNOWN,
        is_retryable=False,
        is_transient=False,
        recommended_action="Inspect the full receipt for details; failure cause is unclear.",
        severity="low",
    )


def classify_failure_from_stderr(
    stderr: str,
    *,
    exit_code: int = 1,
) -> FailureClassification:
    """Convenience: classify a failure from raw stderr text.

    Useful for callers that have stderr but no structured failure_code.
    Synthesizes a failure_code from stderr patterns, then delegates to
    ``classify_failure()``.

    Parameters
    ----------
    stderr : str
        Raw stderr output from a failed process.
    exit_code : int
        Process exit code (default 1).
    """
    return classify_failure(
        "cli_adapter.nonzero_exit",
        outputs={"stderr": stderr, "exit_code": exit_code},
    )
