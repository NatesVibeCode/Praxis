"""Unit tests for runtime.operations.queries.semantic_invariant_scan."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from runtime.operations.queries.semantic_invariant_scan import (
    ScanSemanticInvariantsQuery,
    handle_scan_semantic_invariants,
)


class _SyncConn:
    def __init__(self, predicate_rows: list[dict[str, Any]]) -> None:
        self._rows = predicate_rows
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "FROM semantic_predicate_catalog" not in normalized:
            return []
        rows = [r for r in self._rows if r.get("predicate_kind") == "invariant"]
        if "predicate_slug = $1" in normalized:
            rows = [r for r in rows if r.get("predicate_slug") == args[0]]
        return [dict(r) for r in rows]


def _subsystems(conn: Any) -> SimpleNamespace:
    return SimpleNamespace(get_pg_conn=lambda: conn)


def _write_file(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_handler_runs_scanner_against_synthetic_tree(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "runtime" / "rogue.py",
        "from runtime.workflow.unified import submit_workflow_inline\n"
        "submit_workflow_inline(conn, spec)\n",
    )

    predicate_row = {
        "predicate_slug": "workflow_launch.flow_through_command_bus",
        "predicate_kind": "invariant",
        "applies_to_kind": "operation_class",
        "applies_to_ref": "workflow_launch_or_cancel",
        "summary": "command bus enforcement",
        "propagation_policy": json.dumps(
            {
                "forbidden_callsites_outside_command_bus": [
                    "runtime.workflow.unified.submit_workflow_inline",
                ],
                "allowed_authorities": [
                    "runtime.control_commands.submit_workflow_command",
                ],
                "scan_layers": ["runtime"],
            }
        ),
        "decision_ref": "decision.test",
    }
    conn = _SyncConn([predicate_row])

    out = handle_scan_semantic_invariants(
        ScanSemanticInvariantsQuery(workflow_root=str(tmp_path)),
        _subsystems(conn),
    )

    assert out["predicate_count"] == 1
    assert out["findings_count"] >= 1
    finding = out["findings"][0]
    assert finding["predicate_slug"] == "workflow_launch.flow_through_command_bus"
    assert finding["path"] == "runtime/rogue.py"
    assert "predicates_scanned" in out
    assert out["workflow_root"] == str(tmp_path)


def test_handler_filters_by_predicate_slug(tmp_path: Path) -> None:
    _write_file(tmp_path / "runtime" / "x.py", "noop\n")
    rows = [
        {
            "predicate_slug": "wanted",
            "predicate_kind": "invariant",
            "propagation_policy": {"forbidden_callsites": []},
        },
        {
            "predicate_slug": "ignored",
            "predicate_kind": "invariant",
            "propagation_policy": {"forbidden_callsites": []},
        },
    ]
    conn = _SyncConn(rows)
    out = handle_scan_semantic_invariants(
        ScanSemanticInvariantsQuery(predicate_slug="wanted", workflow_root=str(tmp_path)),
        _subsystems(conn),
    )
    assert out["predicates_scanned"] == ["wanted"]
    assert out["predicate_count"] == 1


def test_handler_returns_empty_when_no_invariant_predicates(tmp_path: Path) -> None:
    conn = _SyncConn([])
    out = handle_scan_semantic_invariants(
        ScanSemanticInvariantsQuery(workflow_root=str(tmp_path)),
        _subsystems(conn),
    )
    assert out["predicate_count"] == 0
    assert out["findings_count"] == 0
    assert out["findings"] == []
