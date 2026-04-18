"""Unit tests for runtime/dataset_redactor.py."""

from __future__ import annotations

from runtime.dataset_redactor import (
    REDACTION_CLEAN,
    REDACTION_REQUIRED,
    REDACTION_SENSITIVE_BLOCKED,
    REDACTION_UNVERIFIED,
    classify_redaction,
    is_promotable_redaction,
)


def test_empty_payload_is_unverified() -> None:
    assert classify_redaction(None).status == REDACTION_UNVERIFIED
    assert classify_redaction("", "   ").status == REDACTION_UNVERIFIED
    assert classify_redaction().status == REDACTION_UNVERIFIED


def test_clean_payload_passes() -> None:
    verdict = classify_redaction(
        {"prompt": "review this diff"},
        {"verdict": "approve", "rationale": "looks fine"},
    )
    assert verdict.status == REDACTION_CLEAN
    assert verdict.blockers == ()
    assert verdict.soft_markers == ()


def test_blocker_anthropic_key_blocks() -> None:
    verdict = classify_redaction(
        {"output": "the key is sk-ant-abc1234567890ABCDEFG"}
    )
    assert verdict.status == REDACTION_SENSITIVE_BLOCKED
    assert "api_key_anthropic" in verdict.blockers


def test_authorization_header_blocks() -> None:
    verdict = classify_redaction("Authorization: Bearer abc.def.ghi-jkl")
    assert verdict.status == REDACTION_SENSITIVE_BLOCKED
    assert "authorization_header" in verdict.blockers


def test_email_is_soft_marker() -> None:
    verdict = classify_redaction({"note": "ping me at user@example.com"})
    assert verdict.status == REDACTION_REQUIRED
    assert "email_address" in verdict.soft_markers


def test_blocker_outranks_soft_marker() -> None:
    verdict = classify_redaction(
        "user@example.com plus key sk-ant-abcdefghijklmnopqrst"
    )
    assert verdict.status == REDACTION_SENSITIVE_BLOCKED


def test_is_promotable_only_when_clean() -> None:
    assert is_promotable_redaction(REDACTION_CLEAN)
    assert not is_promotable_redaction(REDACTION_REQUIRED)
    assert not is_promotable_redaction(REDACTION_SENSITIVE_BLOCKED)
    assert not is_promotable_redaction(REDACTION_UNVERIFIED)


def test_to_summary_serializable() -> None:
    summary = classify_redaction({"x": "user@example.com"}).to_summary()
    assert summary["status"] == REDACTION_REQUIRED
    assert isinstance(summary["soft_markers"], list)
    assert isinstance(summary["blockers"], list)
