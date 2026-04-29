from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from runtime.operations.queries import operator_repo_policy
from storage.postgres.repo_policy_contract_repository import RepoPolicyContractRecord


class _Subsystems:
    def get_pg_conn(self) -> object:
        return object()


def _record() -> RepoPolicyContractRecord:
    return RepoPolicyContractRecord(
        repo_policy_contract_id="repo_policy_contract.test",
        repo_root="/repo",
        status="active",
        current_revision_id="repo_policy_contract_revision.test",
        current_revision_no=1,
        current_contract_hash="sha256:test",
        disclosure_repeat_limit=5,
        bug_disclosure_count=0,
        pattern_disclosure_count=0,
        contract_body={
            "decision_ref": "architecture-policy::operator-onboarding::repo-policy",
            "repo_policy_sections": {
                "repo_rules": ["Follow repo-local authority."],
                "sops": ["Use the CQRS gateway for operation surfaces."],
                "anti_patterns": ["Do not build sidecar surfaces."],
                "forbidden_actions": ["delete migrations/*"],
                "sensitive_systems": [{"label": "Production Stripe"}],
            },
            "task_environment_contract": {"contract_digest": "task-contract:test"},
            "disclosure_policy": {"repeat_limit": 5},
        },
        change_reason="test",
        created_by="test",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


def test_repo_policy_contract_current_query_returns_runtime_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        operator_repo_policy,
        "get_repo_policy_contract",
        lambda *_args, **_kwargs: _record(),
    )

    payload = operator_repo_policy.handle_query_repo_policy_contract_current(
        operator_repo_policy.QueryRepoPolicyContractCurrent(repo_root="/repo"),
        _Subsystems(),
    )

    assert payload["ok"] is True
    assert payload["contract_present"] is True
    assert payload["repo_policy_contract"]["repo_policy_contract_id"] == "repo_policy_contract.test"
    assert payload["repo_policy_contract"]["repo_policy_sections"]["forbidden_actions"] == [
        "delete migrations/*"
    ]
    assert payload["repo_policy_contract"]["repo_policy_sections"]["forbidden_action_rules"][0]["action"] == "delete"


def test_repo_policy_submission_acceptance_query_evaluates_forbidden_actions(monkeypatch) -> None:
    monkeypatch.setattr(
        operator_repo_policy,
        "get_repo_policy_contract",
        lambda *_args, **_kwargs: _record(),
    )

    payload = operator_repo_policy.handle_query_repo_policy_submission_acceptance(
        operator_repo_policy.QueryRepoPolicySubmissionAcceptance(
            repo_root="/repo",
            submission={
                "summary": "Removed stale migration.",
                "declared_operations": [
                    {"action": "delete", "path": "migrations/001_old.sql"},
                ],
                "operation_set": [],
            },
        ),
        _Subsystems(),
    )

    assert payload["ok"] is True
    assert payload["acceptance_status"] == "failed"
    violation = payload["acceptance_report"]["repo_policy"]["violations"][0]
    assert violation["rule"] == "delete migrations/*"
    assert violation["match_kind"] == "glob"
    assert violation["operation"] == {"action": "delete", "path": "migrations/001_old.sql"}
    assert violation["rule_id"].startswith("forbidden_action_rule.")
