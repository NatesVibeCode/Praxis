"""Unit tests for the inline first-failure-node signal surfaced in run-status.

The helper pulls a human-readable failure hint out of a job's stdout_preview
so `praxis workflow run-status` shows why the run failed without requiring a
receipts-table psql excursion. Regressions here put operators back to digging.
"""

from __future__ import annotations

import json

from runtime.workflow._status import (
    _append_first_failure_signal,
    _extract_failure_hint,
)


def test_extract_hint_from_cli_refusal_envelope() -> None:
    # Shape emitted by claude CLI when it declines to act under --print.
    refusal = json.dumps({
        "type": "result",
        "subtype": "success",
        "is_error": True,
        "result": "Not logged in · Please run /login",
        "exit_code": 1,
    })

    assert _extract_failure_hint(refusal) == "Not logged in · Please run /login"


def test_extract_hint_recurses_into_nested_cli_envelope() -> None:
    # Shape emitted by cli_llm adapter: wrapper JSON with inner completion JSON
    # encoded as a string under the `stdout` key.
    inner = json.dumps({"is_error": True, "result": "Invalid API key"})
    outer = json.dumps({"cli": "claude", "exit_code": 1, "stderr": "", "stdout": inner})

    assert _extract_failure_hint(outer) == "Invalid API key"


def test_extract_hint_falls_back_to_first_line_of_plain_text() -> None:
    raw = "traceback: something broke\nsecond line ignored\n"

    assert _extract_failure_hint(raw) == "traceback: something broke"


def test_extract_hint_returns_none_for_empty_input() -> None:
    assert _extract_failure_hint("") is None
    assert _extract_failure_hint("   \n   ") is None


def test_extract_hint_trims_overlong_snippets() -> None:
    long = "x" * 600
    raw = json.dumps({"is_error": True, "result": long})

    hint = _extract_failure_hint(raw)

    assert hint is not None
    assert hint.endswith("…")
    # Body trimmed to the configured limit (300 chars) plus the ellipsis.
    assert len(hint) == 301


def test_append_first_failure_signal_is_silent_for_non_terminal_status() -> None:
    signals: list[dict[str, object]] = []

    _append_first_failure_signal(
        signals,
        status="running",
        jobs=[{"status": "failed", "label": "step0", "last_error_code": "x"}],
    )

    assert signals == []


def test_append_first_failure_signal_surfaces_first_failed_job() -> None:
    signals: list[dict[str, object]] = []
    jobs = [
        {"status": "succeeded", "label": "step0_prep"},
        {
            "status": "failed",
            "label": "step1_generate",
            "last_error_code": "cli_adapter.not_authenticated",
            "stdout_preview": json.dumps({
                "is_error": True,
                "result": "Not logged in · Please run /login",
            }),
        },
        {
            "status": "failed",
            "label": "step2_downstream",
            "last_error_code": "should.not.appear",
        },
    ]

    _append_first_failure_signal(signals, status="failed", jobs=jobs)

    assert len(signals) == 1
    entry = signals[0]
    assert entry["type"] == "first_failed_node"
    assert entry["severity"] == "high"
    assert entry["node_id"] == "step1_generate"
    assert entry["failure_code"] == "cli_adapter.not_authenticated"
    assert entry["hint"] == "Not logged in · Please run /login"
    message = str(entry["message"])
    assert "step1_generate failed" in message
    assert "cli_adapter.not_authenticated" in message
    assert "Not logged in" in message


def test_append_first_failure_signal_handles_missing_failure_code() -> None:
    signals: list[dict[str, object]] = []
    jobs = [
        {
            "status": "failed",
            "label": "step_with_hint_only",
            "last_error_code": "",
            "stdout_preview": "boom: disk full",
        },
    ]

    _append_first_failure_signal(signals, status="failed", jobs=jobs)

    assert len(signals) == 1
    assert "failure_code" not in signals[0]
    assert signals[0]["hint"] == "boom: disk full"
    assert signals[0]["message"] == "step_with_hint_only failed: boom: disk full"


def test_append_first_failure_signal_handles_no_hint() -> None:
    signals: list[dict[str, object]] = []
    jobs = [
        {
            "status": "failed",
            "label": "step_opaque",
            "last_error_code": "adapter.something",
            "stdout_preview": "",
        },
    ]

    _append_first_failure_signal(signals, status="failed", jobs=jobs)

    assert len(signals) == 1
    entry = signals[0]
    assert entry["message"] == "step_opaque failed (adapter.something)"
    assert "hint" not in entry
