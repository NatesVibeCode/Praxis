"""Build context blocks for retry prompts so the agent knows what went wrong."""

from __future__ import annotations

import re
from typing import Optional

import importlib
import importlib.util
import os
import sys

# Import sibling module without triggering runtime/__init__.py
def _sibling_import():
    _dir = os.path.dirname(os.path.abspath(__file__))
    spec = importlib.util.spec_from_file_location(
        "runtime.auto_retry", os.path.join(_dir, "auto_retry.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("runtime.auto_retry", mod)
    spec.loader.exec_module(mod)
    return mod

_auto_retry = _sibling_import()
FailureCategory = _auto_retry.FailureCategory
RetryClassification = _auto_retry.RetryClassification

# Patterns for extracting the most relevant error line from stderr.
_ERROR_LINE_PATTERNS = [
    re.compile(r"(?i)^.*error[:\s].*$"),
    re.compile(r"(?i)^.*exception[:\s].*$"),
    re.compile(r"(?i)^.*traceback.*$"),
    re.compile(r"(?i)^.*failed.*$"),
    re.compile(r"(?i)^.*assert.*$"),
]

_MAX_STDERR_LINES = 50


class RetryContextBuilder:
    """Assemble a context block to inject into a retry prompt."""

    def build(
        self,
        job_label: str,
        failure_code: str,
        stderr: str,
        previous_prompt: Optional[str] = None,
        classification: Optional[RetryClassification] = None,
    ) -> str:
        """Return a multi-line context string for the retry prompt."""
        # Lazy-classify if caller didn't provide one
        if classification is None:
            AutoRetryManager = _auto_retry.AutoRetryManager
            classification = AutoRetryManager().classify(failure_code, stderr)

        lines: list[str] = []
        lines.append("=== PREVIOUS ATTEMPT FAILED ===")
        lines.append(f"Job: {job_label}")
        lines.append(f"Failure code: {failure_code}")
        lines.append(f"Category: {classification.category.value}")
        lines.append("")

        # Stderr excerpt (last N lines, truncated)
        stderr_lines = stderr.strip().splitlines()
        excerpt = stderr_lines[-_MAX_STDERR_LINES:]
        if excerpt:
            lines.append("--- stderr excerpt ---")
            lines.extend(excerpt)
            lines.append("--- end stderr ---")
            lines.append("")

        # Category-specific guidance
        if classification.category == FailureCategory.TRANSIENT:
            lines.append("GUIDANCE: This was a transient failure. Retry the same approach.")
        elif classification.category == FailureCategory.TRIAGEABLE:
            key_err = self.extract_key_error(stderr)
            lines.append(
                f"GUIDANCE: Previous output had issues. Key error: {key_err}. "
                "Adjust your approach."
            )
        elif classification.category == FailureCategory.NON_RETRYABLE:
            lines.append(
                "GUIDANCE: This failure is non-retryable. Do not retry."
            )
        else:
            # UNKNOWN
            lines.append(
                "GUIDANCE: Previous attempt failed for unclear reasons. "
                "Review stderr and try a different strategy."
            )

        return "\n".join(lines)

    @staticmethod
    def extract_key_error(stderr: str) -> str:
        """Pull the most relevant single error line from stderr."""
        for line in stderr.strip().splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            for pattern in _ERROR_LINE_PATTERNS:
                if pattern.search(stripped):
                    return stripped
        # Fallback: return last non-empty line
        for line in reversed(stderr.strip().splitlines()):
            if line.strip():
                return line.strip()
        return "(no error details available)"
