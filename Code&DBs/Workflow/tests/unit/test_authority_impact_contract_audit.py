"""Pass 9 tests: authority impact contract drift audit."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.workflow.authority_impact_contract_audit import (
    audit_authority_impact_contract_coverage,
)


@dataclass
class _FakeConn:
    candidate_rows: list[dict[str, Any]] = field(default_factory=list)
    queries: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        normalized = " ".join(query.split())
        self.queries.append((normalized, args))
        assert "FROM code_change_candidate_payloads" in normalized
        target_paths = set(args[0])
        rows: list[dict[str, Any]] = []
        for record in self.candidate_rows:
            intended = record.get("intended_files") or []
            if any(path in target_paths for path in intended):
                rows.append(dict(record))
        return rows

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if query.strip().upper().startswith("SELECT"):
            return self.fetch(query, *args)
        return []


def test_audit_returns_empty_for_no_paths() -> None:
    result = audit_authority_impact_contract_coverage(_FakeConn(), paths=[])
    assert result.findings == []
    assert result.notes == ["no_paths_supplied"]


def test_audit_classifies_non_authority_paths() -> None:
    result = audit_authority_impact_contract_coverage(
        _FakeConn(), paths=["docs/notes.md", "README.md"]
    )
    assert result.not_authority_bearing_count == 2
    assert result.covered_count == 0
    assert result.uncovered_count == 0
    assert all(f.coverage == "not_authority_bearing" for f in result.findings)


def test_audit_marks_authority_paths_uncovered_when_no_candidate() -> None:
    result = audit_authority_impact_contract_coverage(
        _FakeConn(),
        paths=["Code&DBs/Databases/migrations/workflow/999_x.sql"],
    )
    assert result.uncovered_count == 1
    assert result.findings[0].coverage == "uncovered"
    assert result.findings[0].classified_unit_kind == "migration_ref"
    assert "lack_impact_contract_coverage" in result.notes[0]


def test_audit_marks_authority_paths_covered_when_candidate_intends_them() -> None:
    target_path = "Code&DBs/Databases/migrations/workflow/999_x.sql"
    conn = _FakeConn(
        candidate_rows=[
            {"candidate_id": "c1", "intended_files": [target_path, "docs/x.md"]},
        ]
    )
    result = audit_authority_impact_contract_coverage(conn, paths=[target_path])
    assert result.covered_count == 1
    assert result.uncovered_count == 0
    finding = result.findings[0]
    assert finding.coverage == "covered"
    assert finding.candidate_ids == ["c1"]


def test_audit_carries_multiple_candidates_for_one_path() -> None:
    target_path = "Code&DBs/Workflow/runtime/operations/commands/foo.py"
    conn = _FakeConn(
        candidate_rows=[
            {"candidate_id": "c1", "intended_files": [target_path]},
            {"candidate_id": "c2", "intended_files": [target_path, "other.py"]},
        ]
    )
    result = audit_authority_impact_contract_coverage(conn, paths=[target_path])
    finding = result.findings[0]
    assert finding.coverage == "covered"
    assert sorted(finding.candidate_ids) == ["c1", "c2"]


def test_audit_dedupes_input_paths_and_skips_blanks() -> None:
    conn = _FakeConn()
    result = audit_authority_impact_contract_coverage(
        conn, paths=["docs/a.md", "docs/a.md", "", "  "]
    )
    assert len(result.findings) == 1
    assert result.findings[0].path == "docs/a.md"


def test_audit_mixes_covered_uncovered_and_non_authority() -> None:
    covered_path = "Code&DBs/Databases/migrations/workflow/999_x.sql"
    uncovered_path = "Code&DBs/Workflow/runtime/operations/commands/orphan.py"
    docs_path = "docs/handbook.md"
    conn = _FakeConn(
        candidate_rows=[
            {"candidate_id": "c1", "intended_files": [covered_path]},
        ]
    )
    result = audit_authority_impact_contract_coverage(
        conn, paths=[covered_path, uncovered_path, docs_path]
    )
    payload = result.to_dict()
    assert payload["summary"]["total_paths"] == 3
    assert payload["summary"]["covered_count"] == 1
    assert payload["summary"]["uncovered_count"] == 1
    assert payload["summary"]["not_authority_bearing_count"] == 1
    coverages = {f["path"]: f["coverage"] for f in payload["findings"]}
    assert coverages[covered_path] == "covered"
    assert coverages[uncovered_path] == "uncovered"
    assert coverages[docs_path] == "not_authority_bearing"


def test_to_dict_round_trip_shape_is_stable() -> None:
    result = audit_authority_impact_contract_coverage(_FakeConn(), paths=["docs/x.md"])
    payload = result.to_dict()
    assert set(payload.keys()) == {"findings", "summary", "notes"}
    assert set(payload["summary"].keys()) == {
        "not_authority_bearing_count",
        "covered_count",
        "uncovered_count",
        "total_paths",
    }
