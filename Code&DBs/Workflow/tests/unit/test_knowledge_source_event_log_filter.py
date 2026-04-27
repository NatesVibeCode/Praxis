"""Verifier test for BUG-4E6A2081 — knowledge source must filter
machine-generated event-log entity names from default federated search.
"""
from __future__ import annotations

from runtime.sources.knowledge_source import _is_event_log_noise


def test_hard_failure_facts_filtered():
    assert _is_event_log_noise("hard_failure: cli.workflow", "fact")
    assert _is_event_log_noise("hard_failure: anthropic/claude-opus-4-7", "fact")


def test_verification_facts_filtered():
    assert _is_event_log_noise("verification:wave5_canvas_shell", "fact")


def test_workflow_id_facts_filtered():
    assert _is_event_log_noise("workflow_3b8578ab35ff", "fact")
    assert _is_event_log_noise("workflow_97942806618e", "fact")


def test_receipt_facts_filtered():
    assert _is_event_log_noise("receipt:workflow_4540dae5cb31:133:1", "fact")


def test_real_decisions_not_filtered():
    assert not _is_event_log_noise(
        "Token budgets are not workflow execution authority", "decision"
    )
    assert not _is_event_log_noise(
        "Outcome graph links code, UI, bugs, roadmap, integrations, receipts, and external memory by typed edges",
        "decision",
    )


def test_non_fact_entity_types_not_filtered():
    """Non-fact entity_types are never filtered, even if name pattern matches."""
    assert not _is_event_log_noise("hard_failure: cli.workflow", "decision")
    assert not _is_event_log_noise("workflow_runs", "table")


def test_real_facts_with_human_names_not_filtered():
    assert not _is_event_log_noise("provider_routing", "fact")
    assert not _is_event_log_noise("source_linked_outcome_graph", "fact")


def test_event_log_pattern_constants_present():
    """Regression guard: confirm the filter list exists in source."""
    from runtime.sources import knowledge_source

    text = open(knowledge_source.__file__, encoding="utf-8").read()
    assert "_EVENT_LOG_NAME_PATTERNS" in text
    assert "hard_failure" in text
    assert "include_event_log_facts" in text


def test_opt_in_extras_flag_documented_in_handler():
    """Confirm the opt-in path is wired."""
    from runtime.sources import knowledge_source

    text = open(knowledge_source.__file__, encoding="utf-8").read()
    assert 'extras.get("include_event_log_facts"' in text
