"""Tests for runtime.prompt_generation — deterministic prompt assembly."""

import pytest
from runtime.prompt_generation import generate_job_prompt


def test_basic_prompt():
    result = generate_job_prompt(
        outcome_goal="Fix the login bug",
        task_type="debug",
    )
    assert "## Objective" in result
    assert "Fix the login bug" in result


def test_includes_scope():
    result = generate_job_prompt(
        outcome_goal="Audit the API",
        task_type="code_review",
        scope_read=["src/api/"],
        scope_write=["artifacts/"],
    )
    assert "## Scope" in result
    assert "Read: src/api/" in result
    assert "Write: artifacts/" in result


def test_includes_anti_requirements():
    result = generate_job_prompt(
        outcome_goal="Refactor utils",
        task_type="code_edit",
        anti_requirements=["Do not change the public API", "No new dependencies"],
    )
    assert "## Constraints" in result
    assert "- Do not change the public API" in result
    assert "- No new dependencies" in result


def test_includes_authoring_contract():
    result = generate_job_prompt(
        outcome_goal="Write a report",
        task_type="research",
        authoring_contract={
            "artifact_kind": "research_report",
            "required_sections": ["Summary", "Findings"],
            "notes": ["Cite all sources"],
        },
    )
    assert "## Requirements" in result
    assert "Produce: research_report" in result
    assert "Required sections: Summary, Findings" in result
    assert "- Cite all sources" in result


def test_includes_acceptance_contract():
    result = generate_job_prompt(
        outcome_goal="Build feature",
        task_type="code_generation",
        acceptance_contract={
            "verify_refs": ["pytest tests/"],
            "review": {"criteria": ["Code is clean", "Tests pass"]},
        },
    )
    assert "## Acceptance Criteria" in result
    assert "- Pass: `pytest tests/`" in result
    assert "- Code is clean" in result


def test_includes_verify_refs():
    result = generate_job_prompt(
        outcome_goal="Fix tests",
        task_type="debug",
        verify_refs=["pytest tests/unit/", "mypy src/"],
    )
    assert "## Verification" in result
    assert "- `pytest tests/unit/`" in result
    assert "- `mypy src/`" in result


def test_empty_optionals_produce_clean_output():
    result = generate_job_prompt(
        outcome_goal="Do something",
        task_type="general",
    )
    assert "## Objective" in result
    assert "## Scope" not in result
    assert "## Constraints" not in result
    assert "## Requirements" not in result
    assert "## Acceptance Criteria" not in result
    assert "## Verification" not in result


def test_structural_acceptance_sections():
    result = generate_job_prompt(
        outcome_goal="Produce artifact",
        task_type="code_generation",
        acceptance_contract={
            "structural": {
                "required_sections": ["Summary", "Details"],
                "required_fields": ["score", "confidence"],
            },
        },
    )
    assert "Must include sections: Summary, Details" in result
    assert "Must include fields: score, confidence" in result
