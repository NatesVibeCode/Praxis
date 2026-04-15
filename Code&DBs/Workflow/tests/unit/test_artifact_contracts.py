from __future__ import annotations

from runtime.workflow.artifact_contracts import evaluate_submission_acceptance
from runtime.workflow.execution_bundle import build_execution_bundle, render_execution_bundle
from runtime.workflow_spec import validate_workflow_spec


def test_validate_workflow_spec_accepts_authoring_and_acceptance_contracts() -> None:
    payload = {
        "prompt": "Draft the brief",
        "provider_slug": "anthropic",
        "adapter_type": "cli_llm",
        "output_schema": {
            "type": "object",
            "properties": {
                "cost_estimate": {
                    "type": "object",
                    "properties": {"total": {"type": "number"}},
                    "required": ["total"],
                }
            },
            "required": ["cost_estimate"],
        },
        "authoring_contract": {
            "artifact_kind": "research_brief",
            "required_sections": ["Findings", "Sources"],
            "submission_format": "json_then_render",
        },
        "acceptance_contract": {
            "structural": {
                "required_sections": ["Findings", "Sources"],
            },
            "assertions": [
                {"kind": "citations_at_least", "min": 1},
            ],
            "review": {
                "criteria": ["Answer the original question"],
                "required_decision": "approve",
            },
            "verify_refs": ["verify_ref.python.py_compile.test"],
        },
    }

    ok, errors = validate_workflow_spec(payload)

    assert ok
    assert errors == []


def test_build_execution_bundle_renders_authoring_and_acceptance_contracts() -> None:
    bundle = build_execution_bundle(
        job_label="research.brief",
        prompt="Draft the brief",
        task_type="research",
        verify_refs=["verify_ref.python.py_compile.test"],
        approval_required=True,
        approval_question="Approve the brief before drafting?",
        output_schema={
            "type": "object",
            "properties": {
                "cost_estimate": {
                    "type": "object",
                    "properties": {"total": {"type": "number"}},
                    "required": ["total"],
                }
            },
            "required": ["cost_estimate"],
        },
        authoring_contract={
            "artifact_kind": "research_brief",
            "required_sections": ["Findings", "Sources"],
            "stop_boundary": "Do not propose implementation steps.",
        },
        acceptance_contract={
            "review": {
                "criteria": ["Answer the ask", "Use evidence correctly"],
                "required_decision": "approve",
            }
        },
    )

    rendered = render_execution_bundle(bundle)

    assert bundle["authoring_contract"]["required_sections"] == ["Findings", "Sources"]
    assert bundle["acceptance_contract"]["verify_refs"] == ["verify_ref.python.py_compile.test"]
    assert bundle["acceptance_contract"]["review"]["required_decision"] == "approve"
    assert bundle["approval_required"] is True
    assert bundle["approval_question"] == "Approve the brief before drafting?"
    assert "** AUTHORING CONTRACT **" in rendered
    assert "section_scaffold" in rendered
    assert "** ACCEPTANCE CONTRACT **" in rendered
    assert "review.required_decision: approve" in rendered
    assert "** APPROVAL REQUIRED **" in rendered


def test_evaluate_submission_acceptance_tracks_pending_and_passed_states() -> None:
    acceptance_contract = {
        "structural": {
            "required_sections": ["Findings", "Sources"],
            "output_schema": {
                "type": "object",
                "properties": {
                    "cost_estimate": {
                        "type": "object",
                        "properties": {"total": {"type": "number"}},
                        "required": ["total"],
                    }
                },
                "required": ["cost_estimate"],
            },
        },
        "assertions": [
            {"kind": "citations_at_least", "min": 1},
            {"kind": "field_numeric", "path": "cost_estimate.total"},
        ],
        "verify_refs": ["verify.ref.cost"],
        "review": {
            "criteria": ["Answer the original question"],
            "required_decision": "approve",
        },
    }
    summary = """## Findings
- Numeric estimate provided.

## Sources
- [Source](https://example.com/source)

```json
{
  "cost_estimate": {
    "total": 1200
  }
}
```
"""

    pending_review_status, pending_review_report = evaluate_submission_acceptance(
        submission={
            "summary": summary,
            "verification_artifact_refs": ["receipt:verify:1"],
        },
        acceptance_contract=acceptance_contract,
    )

    assert pending_review_status == "pending_review"
    assert pending_review_report["verification"]["passed"] is True
    assert pending_review_report["hard_failures"] == []

    passed_status, passed_report = evaluate_submission_acceptance(
        submission={
            "summary": summary,
            "verification_artifact_refs": ["receipt:verify:1"],
            "latest_review": {"decision": "approve"},
        },
        acceptance_contract=acceptance_contract,
    )

    assert passed_status == "passed"
    assert passed_report["review"]["latest_decision"] == "approve"


def test_evaluate_submission_acceptance_fails_when_required_structure_is_missing() -> None:
    status, report = evaluate_submission_acceptance(
        submission={
            "summary": "## Findings\n- Missing sources section.",
        },
        acceptance_contract={
            "structural": {
                "required_sections": ["Findings", "Sources"],
            },
        },
    )

    assert status == "failed"
    assert "missing required section: Sources" in report["hard_failures"]
