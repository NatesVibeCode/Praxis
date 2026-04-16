from __future__ import annotations

import json

from runtime.workflow.decision_context import (
    decision_workspace_overlays,
    explicit_authority_domains_for_job,
    infer_authority_domains_from_paths,
    render_decision_pack,
    resolve_job_decision_pack,
)


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        self.queries.append((query, args))
        if "FROM operator_decisions" in query:
            return list(self.rows)
        return []


def test_infer_authority_domains_from_paths_matches_governed_sections() -> None:
    domains = infer_authority_domains_from_paths(
        [
            "Code&DBs/Workflow/runtime/sandbox_runtime.py",
            "runtime/workflow/execution_backends.py",
            "docs/ARCHITECTURE.md",
        ]
    )

    assert "sandbox_execution" in domains
    assert "decision_tables" in domains


def test_explicit_authority_domains_for_job_merges_snapshot_job_and_scope_fields() -> None:
    domains = explicit_authority_domains_for_job(
        job={
            "authority_domains": ["sandbox_execution"],
            "scope": {"authority_domains": ["compile_authority"]},
        },
        spec_snapshot={"decision_authority_domains": ["decision_tables"]},
    )

    assert domains == ["decision_tables", "sandbox_execution", "compile_authority"]


def test_resolve_job_decision_pack_collects_active_architecture_policies() -> None:
    conn = _Conn(
        [
            {
                "operator_decision_id": "operator_decision.architecture_policy.sandbox_execution.docker_only_authority",
                "decision_key": "architecture-policy::sandbox-execution::docker-only-authority",
                "decision_kind": "architecture_policy",
                "decision_status": "decided",
                "title": "Workflow sandbox execution is Docker-only",
                "rationale": "Use docker_local or admitted cloudflare_remote only.",
                "decided_by": "nate",
                "decision_source": "cto.guidance",
                "effective_from": "2026-04-16T18:50:34+00:00",
                "effective_to": None,
                "decided_at": "2026-04-16T18:50:34+00:00",
                "updated_at": "2026-04-16T18:50:34+00:00",
                "decision_scope_kind": "authority_domain",
                "decision_scope_ref": "sandbox_execution",
            }
        ]
    )

    pack = resolve_job_decision_pack(
        conn,
        write_scope=["runtime/sandbox_runtime.py"],
        explicit_authority_domains=["sandbox_execution"],
    )

    assert pack["authority_domains"] == ["sandbox_execution"]
    assert pack["decision_keys"] == [
        "architecture-policy::sandbox-execution::docker-only-authority"
    ]
    assert pack["decisions"][0]["title"] == "Workflow sandbox execution is Docker-only"
    assert conn.queries


def test_render_decision_pack_and_workspace_overlays_are_machine_and_human_readable() -> None:
    decision_pack = {
        "pack_version": 1,
        "authority_domains": ["sandbox_execution"],
        "decision_keys": ["architecture-policy::sandbox-execution::docker-only-authority"],
        "decisions": [
            {
                "decision_key": "architecture-policy::sandbox-execution::docker-only-authority",
                "title": "Workflow sandbox execution is Docker-only",
                "rationale": "Do not restore host-local execution lanes.",
                "decision_scope_ref": "sandbox_execution",
            }
        ],
    }

    rendered = render_decision_pack(decision_pack)
    overlays = decision_workspace_overlays({"decision_pack": decision_pack})

    assert "** APPLICABLE DECISIONS **" in rendered
    assert "docker-only-authority" in rendered
    assert [overlay["relative_path"] for overlay in overlays] == [
        "_context/decision_pack.json",
        "_context/decision_summary.md",
    ]
    payload = json.loads(overlays[0]["content"])
    assert payload["authority_domains"] == ["sandbox_execution"]
