"""CQRS queries for operator repo-policy onboarding authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.repo_policy_onboarding import (
    get_repo_policy_contract,
    repo_policy_runtime_payload,
)
from runtime.workflow.artifact_contracts import evaluate_submission_acceptance
from runtime.workspace_paths import workflow_root


class QueryRepoPolicyContractCurrent(BaseModel):
    repo_root: str | None = None

    @field_validator("repo_root", mode="before")
    @classmethod
    def _normalize_repo_root(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("repo_root must be a non-empty string when provided")
        return value.strip()


class QueryRepoPolicySubmissionAcceptance(BaseModel):
    repo_root: str | None = None
    submission: dict[str, Any] = Field(default_factory=dict)
    acceptance_contract: dict[str, Any] = Field(default_factory=dict)

    @field_validator("repo_root", mode="before")
    @classmethod
    def _normalize_repo_root(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("repo_root must be a non-empty string when provided")
        return value.strip()

    @field_validator("submission", "acceptance_contract", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("submission and acceptance_contract must be JSON objects")


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    return getter() if callable(getter) else None


def _repo_root(query_repo_root: str | None) -> str:
    return query_repo_root or workflow_root()


def handle_query_repo_policy_contract_current(
    query: QueryRepoPolicyContractCurrent,
    subsystems: Any,
) -> dict[str, Any]:
    record = get_repo_policy_contract(
        _pg_conn(subsystems),
        repo_root=_repo_root(query.repo_root),
    )
    payload = repo_policy_runtime_payload(record)
    return {
        "ok": True,
        "operation": "operator.repo_policy_contract_current",
        "repo_policy_contract": payload,
        "contract_present": payload is not None,
    }


def handle_query_repo_policy_submission_acceptance(
    query: QueryRepoPolicySubmissionAcceptance,
    subsystems: Any,
) -> dict[str, Any]:
    contract = handle_query_repo_policy_contract_current(
        QueryRepoPolicyContractCurrent(repo_root=query.repo_root),
        subsystems,
    ).get("repo_policy_contract")
    status, report = evaluate_submission_acceptance(
        submission=query.submission,
        acceptance_contract=query.acceptance_contract,
        repo_policy_contract=contract if isinstance(contract, dict) else None,
    )
    return {
        "ok": True,
        "operation": "operator.repo_policy_submission_acceptance",
        "acceptance_status": status,
        "acceptance_report": report,
        "repo_policy_contract": contract,
    }


__all__ = [
    "QueryRepoPolicyContractCurrent",
    "QueryRepoPolicySubmissionAcceptance",
    "handle_query_repo_policy_contract_current",
    "handle_query_repo_policy_submission_acceptance",
]
