from __future__ import annotations

from pathlib import Path

from runtime.data_dictionary_wiring_audit import (
    _DEFAULT_REPO_ROOT,
    _DEFAULT_ROOT,
    audit_code_orphan_tables,
    audit_hard_paths,
    run_full_audit,
)


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class _FakeConn:
    def execute(self, sql: str, *params):
        if "FROM data_dictionary_objects" in sql:
            return [
                {"object_kind": "table:authority_event_contracts"},
                {"object_kind": "table:dead_table"},
            ]
        if "FROM audit_exclusions" in sql:
            return []
        if "pg_class" in sql or "authority_projection_contracts" in sql:
            return [{"table_name": "authority_event_contracts"}]
        return []


def test_default_audit_root_is_praxis_repo_not_parent_workspace() -> None:
    assert _DEFAULT_ROOT.name == "Workflow"
    assert (_DEFAULT_REPO_ROOT / "AGENTS.md").is_file()
    assert (_DEFAULT_REPO_ROOT / "Code&DBs" / "Workflow").is_dir()


def test_hard_path_audit_classifies_repo_surfaces(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "Skills/praxis-example/SKILL.md",
        "Use /Users/nate/Builds/recruiter-runtime/bin/recruiter-runtime\n",
    )
    _write(
        tmp_path,
        "config/cascade/specs/current.queue.json",
        '{"workdir": "/Volumes/Users/natha/Documents/Builds/Praxis"}\n',
    )
    _write(tmp_path, "docs/SETUP.md", "Run from /Users/nate/Praxis\n")
    _write(tmp_path, "docs/MCP.md", "Generated example http://127.0.0.1:8420\n")
    _write(tmp_path, "artifacts/old_run/receipt.md", "cwd=/Users/nate/Praxis\n")
    _write(
        tmp_path,
        "Code&DBs/Workflow/artifacts/workflow/old_fix.queue.json",
        "Database: postgresql://nate@127.0.0.1:5432/dag_workflow\n",
    )
    _write(tmp_path, "Code&DBs/Workflow/tests/unit/test_paths.py", "p='/Users/nate/Praxis'\n")
    _write(tmp_path, "Code&DBs/Workflow/runtime/live.py", "ROOT='/Users/nate/Praxis'\n")

    findings = audit_hard_paths(tmp_path)

    by_subject = {finding.subject.rsplit(":", 1)[0]: finding for finding in findings}
    assert by_subject["Skills/praxis-example/SKILL.md"].details == {
        "match": "/Users/nate/Builds/recruiter-runtime/bin/recruiter-runtime",
        "classification": "live_authority_bug",
        "surface": "skill",
        "recommended_action": (
            "Replace operator-local paths with registry, env, PATH, or repo-relative authority."
        ),
    }
    assert by_subject["config/cascade/specs/current.queue.json"].details[
        "surface"
    ] == "queue_spec"
    assert by_subject["docs/SETUP.md"].details["surface"] == "doc"
    assert by_subject["docs/MCP.md"].details["classification"] == (
        "generated_derived_artifact"
    )
    assert by_subject["artifacts/old_run/receipt.md"].details["classification"] == (
        "historical_receipt_evidence"
    )
    assert by_subject[
        "Code&DBs/Workflow/artifacts/workflow/old_fix.queue.json"
    ].details["classification"] == "historical_workflow_packet"
    assert by_subject["Code&DBs/Workflow/tests/unit/test_paths.py"].details[
        "classification"
    ] == "test_fixture"
    assert by_subject["Code&DBs/Workflow/runtime/live.py"].details["surface"] == "source"


def test_full_audit_rolls_up_hard_path_classifications(tmp_path: Path) -> None:
    _write(tmp_path, "Skills/praxis-example/SKILL.md", "Use /Users/nate/bin/tool\n")
    _write(tmp_path, "artifacts/old/receipt.md", "cwd=/Users/nate/Praxis\n")

    report = run_full_audit(object(), root=tmp_path, include_unwired=False)

    assert report["total"] == 2
    assert report["actionable_total"] == 1
    assert report["by_classification"] == {
        "historical_receipt_evidence": 1,
        "live_authority_bug": 1,
    }
    assert report["by_surface"] == {"historical_artifact": 1, "skill": 1}


def test_hard_path_audit_ignores_python_slice_bounds(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "Code&DBs/Workflow/runtime/slices.py",
        "\n".join((
            "summary = text[:8000]",
            "context = prompt[:3000]",
            "payload = evidence[:5000]",
        )),
    )

    findings = audit_hard_paths(tmp_path)

    assert [finding.to_payload() for finding in findings] == []


def test_hard_path_audit_keeps_contextual_ports(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "Code&DBs/Workflow/runtime/server.py",
        'API_URL = "http://127.0.0.1:8420"\n',
    )

    findings = audit_hard_paths(tmp_path)
    kinds = [finding.kind for finding in findings]

    assert "hardcoded_localhost" in kinds
    assert "hardcoded_port" in kinds
    assert any(
        finding.kind == "hardcoded_port" and finding.details["port"] == "8420"
        for finding in findings
    )


def test_code_orphan_audit_honors_db_native_references(tmp_path: Path) -> None:
    findings = audit_code_orphan_tables(_FakeConn(), root=tmp_path)

    assert [finding.subject for finding in findings] == ["table:dead_table"]
