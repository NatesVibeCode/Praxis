"""Operator onboarding probes for repo-policy contracts."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from runtime.repo_policy_onboarding import (
    get_repo_policy_contract,
    repo_policy_probe_observed_state,
)
from storage.postgres import ensure_postgres_available

from .graph import GateGraph, GateProbe, gate_result


_PROBE_REPO_POLICY_CONTRACT = GateProbe(
    gate_ref="operator.repo_policy_contract",
    domain="operator",
    title="Repo policy contract captured",
    purpose=(
        "First-run setup should capture repo rules, SOPs, sensitive-system handling, "
        "forbidden actions, and starter anti-patterns as durable authority instead "
        "of leaving them in chat residue."
    ),
    depends_on=("platform.workflow_database", "runtime.env_file"),
    ok_cache_ttl_s=120,
)


def probe_repo_policy_contract(env: Mapping[str, str], repo_root: Path):
    probe = _PROBE_REPO_POLICY_CONTRACT
    database_url = str(env.get("WORKFLOW_DATABASE_URL") or "").strip()
    if not database_url:
        return gate_result(
            probe,
            status="blocked",
            observed_state=repo_policy_probe_observed_state(None),
            remediation_hint=(
                "Resolve the workflow database and repo .env gates first so Praxis has "
                "durable authority for repo policy onboarding."
            ),
            apply_ref="apply.operator.repo_policy_contract.write",
        )
    try:
        conn = ensure_postgres_available(env={"WORKFLOW_DATABASE_URL": database_url})
        record = get_repo_policy_contract(conn, repo_root=str(repo_root))
    except Exception as exc:  # noqa: BLE001 - onboarding probe should report state, not crash
        return gate_result(
            probe,
            status="unknown",
            observed_state={"error": str(exc), **repo_policy_probe_observed_state(None)},
            remediation_hint=(
                "Praxis could not inspect repo policy onboarding authority. Fix database authority "
                "or migration drift, then re-run setup graph."
            ),
            apply_ref="apply.operator.repo_policy_contract.write",
        )

    if record is None:
        return gate_result(
            probe,
            status="missing",
            observed_state=repo_policy_probe_observed_state(None),
            remediation_hint=(
                "Capture repo rules, SOPs, sensitive systems, forbidden actions, and starter anti-patterns "
                "through `praxis_setup(action=\"apply\", gate=\"operator.repo_policy_contract\", ...)`."
            ),
            apply_ref="apply.operator.repo_policy_contract.write",
        )

    return gate_result(
        probe,
        status="ok",
        observed_state=repo_policy_probe_observed_state(record),
        apply_ref="apply.operator.repo_policy_contract.write",
    )


def register(graph: GateGraph) -> None:
    graph.register(_PROBE_REPO_POLICY_CONTRACT, probe_repo_policy_contract)

