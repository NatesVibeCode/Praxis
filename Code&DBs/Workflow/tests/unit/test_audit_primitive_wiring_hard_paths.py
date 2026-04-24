from __future__ import annotations

from runtime.audit_primitive import Finding
from runtime.audit_primitive_wiring import (
    _plan_host_env_var,
    _plan_path_relative,
    _plan_port_env_var,
)


def _finding(kind: str, *, surface: str, classification: str = "live_authority_bug") -> Finding:
    details: dict[str, object] = {
        "classification": classification,
        "surface": surface,
    }
    if kind == "hardcoded_port":
        details["port"] = "8420"
    else:
        details["match"] = "/Users/nate/Praxis" if kind == "absolute_user_path" else "localhost"
    return Finding(
        audit_kind="wiring",
        finding_kind=kind,
        subject="docs/SETUP.md:12",
        evidence="example",
        details=details,
    )


def test_hard_path_planners_review_unvalidated_findings() -> None:
    plan = _plan_port_env_var(
        Finding(
            audit_kind="wiring",
            finding_kind="hardcoded_port",
            subject="runtime/bug_tracker.py:1235",
            evidence="_store.prepare(text[:8000])",
            details={"port": "8000"},
        )
    )

    assert plan is not None
    assert plan.action_kind == "operator_review"
    assert plan.autorun_ok is False


def test_hard_path_planners_review_non_executable_surfaces() -> None:
    plans = [
        _plan_host_env_var(_finding("hardcoded_localhost", surface="doc")),
        _plan_port_env_var(_finding("hardcoded_port", surface="generated_artifact")),
        _plan_path_relative(_finding("absolute_user_path", surface="skill")),
        _plan_path_relative(
            _finding(
                "absolute_user_path",
                surface="historical_artifact",
                classification="historical_receipt_evidence",
            )
        ),
    ]

    assert all(plan is not None for plan in plans)
    assert [plan.action_kind for plan in plans if plan is not None] == [
        "operator_review",
        "operator_review",
        "operator_review",
        "operator_review",
    ]
    assert all(plan.autorun_ok is False for plan in plans if plan is not None)


def test_hard_path_planners_keep_code_edit_plans_for_executable_surfaces() -> None:
    host_plan = _plan_host_env_var(_finding("hardcoded_localhost", surface="source"))
    port_plan = _plan_port_env_var(_finding("hardcoded_port", surface="cli_surface"))
    path_plan = _plan_path_relative(_finding("absolute_user_path", surface="source"))

    assert host_plan is not None
    assert port_plan is not None
    assert path_plan is not None
    assert host_plan.action_kind == "regex_replace"
    assert port_plan.action_kind == "regex_replace"
    assert path_plan.action_kind == "regex_replace"
