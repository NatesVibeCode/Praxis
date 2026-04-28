"""Tests for runtime.semantic_invariant_scanner.

Two tracks:

1. Unit tests against synthetic predicate rows + a tmp_path tree, proving
   the scanner detects violations and respects the allow-list.

2. A live regression test that runs the scanner against the seeded
   ``workflow_launch.flow_through_command_bus`` invariant.  When zero
   findings, the bypass cleanup from the CQRS audit holds.  When findings
   appear, the predicate did its job and tells you exactly which file
   re-introduced a direct unified-dispatch call.

The live regression test locks the cleaned-up CQRS boundary: workflow launch
and cancellation surfaces must flow through
``runtime.control_commands.submit_workflow_command`` or
``execute_control_intent`` rather than direct unified-dispatch calls.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from runtime.semantic_invariant_scanner import (
    scan_all_invariant_predicates,
    scan_invariant_predicate,
)


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


def _write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def test_scanner_detects_forbidden_callsite_in_synthetic_tree(tmp_path: Path) -> None:
    _write(
        tmp_path / "runtime" / "rogue.py",
        "from runtime.workflow.unified import submit_workflow_inline\n"
        "submit_workflow_inline(conn, spec)\n",
    )
    _write(
        tmp_path / "runtime" / "control_commands.py",
        "def submit_workflow_command(*a, **k): pass\n",
    )

    predicate = {
        "predicate_slug": "workflow_launch.flow_through_command_bus",
        "predicate_kind": "invariant",
        "propagation_policy": {
            "forbidden_callsites_outside_command_bus": [
                "runtime.workflow.unified.submit_workflow_inline",
            ],
            "allowed_authorities": [
                "runtime.control_commands.submit_workflow_command",
            ],
            "scan_layers": ["runtime"],
        },
    }
    findings = scan_invariant_predicate(predicate=predicate, workflow_root=tmp_path)
    assert findings, "expected at least one finding for the rogue file"
    paths = {f["path"] for f in findings}
    assert "runtime/rogue.py" in paths
    assert "runtime/control_commands.py" not in paths  # allowed authority


def test_scanner_respects_allowed_authority_directory(tmp_path: Path) -> None:
    _write(
        tmp_path / "runtime" / "control_commands.py",
        "from runtime.workflow.unified import submit_workflow_inline\n"
        "submit_workflow_inline(conn, spec)\n",
    )
    predicate = {
        "predicate_slug": "x",
        "predicate_kind": "invariant",
        "propagation_policy": {
            "forbidden_callsites_outside_command_bus": [
                "runtime.workflow.unified.submit_workflow_inline",
            ],
            "allowed_authorities": [
                "runtime.control_commands.submit_workflow_command",
            ],
            "scan_layers": ["runtime"],
        },
    }
    findings = scan_invariant_predicate(predicate=predicate, workflow_root=tmp_path)
    assert findings == []


def test_scanner_detects_alias_import_of_forbidden_module_callsite(tmp_path: Path) -> None:
    _write(
        tmp_path / "runtime" / "alias_rogue.py",
        "from runtime.workflow import unified\n"
        "unified.submit_workflow_inline(conn, spec)\n",
    )
    predicate = {
        "predicate_slug": "workflow_launch.flow_through_command_bus",
        "predicate_kind": "invariant",
        "propagation_policy": {
            "forbidden_callsites_outside_command_bus": [
                "runtime.workflow.unified.submit_workflow_inline",
            ],
            "allowed_authorities": [
                "runtime.control_commands.submit_workflow_command",
            ],
            "scan_layers": ["runtime"],
        },
    }

    findings = scan_invariant_predicate(predicate=predicate, workflow_root=tmp_path)

    assert findings, "expected the alias import to count as a forbidden callsite"
    assert {finding["path"] for finding in findings} == {"runtime/alias_rogue.py"}


def test_scanner_skips_non_invariant_predicates(tmp_path: Path) -> None:
    _write(tmp_path / "runtime" / "rogue.py", "submit_workflow_inline(conn, spec)\n")
    findings = scan_all_invariant_predicates(
        predicates=[
            {
                "predicate_slug": "x",
                "predicate_kind": "causal",
                "propagation_policy": {
                    "forbidden_callsites": ["submit_workflow_inline"],
                },
            }
        ],
        workflow_root=tmp_path,
    )
    assert findings == []


def test_workflow_launch_invariant_holds_against_live_tree() -> None:
    """Lock-in regression for BUG-* CQRS bypass cleanup.

    Mirror the seeded ``workflow_launch.flow_through_command_bus``
    predicate (migration 237) and assert no callsite outside the allowed
    command-bus authority calls the forbidden unified-dispatch entry
    points (submit/retry/cancel submission APIs).

    If this test fails, someone re-introduced a direct call to
    ``submit_workflow`` / ``submit_workflow_inline`` / ``retry_job`` /
    ``cancel_run`` / ``cancel_job`` from a domain authority that should be
    flowing through ``runtime.control_commands.submit_workflow_command`` or
    ``runtime.control_commands.execute_control_intent``.
    """

    predicate = {
        "predicate_slug": "workflow_launch.flow_through_command_bus",
        "predicate_kind": "invariant",
        "propagation_policy": {
            "forbidden_callsites_outside_command_bus": [
                "runtime.workflow.unified.submit_workflow",
                "runtime.workflow.unified.submit_workflow_inline",
                "runtime.workflow.unified.retry_job",
                "runtime.workflow.unified.cancel_run",
                "runtime.workflow.unified.cancel_job",
            ],
            "allowed_authorities": [
                "runtime.control_commands.submit_workflow_command",
                "runtime.control_commands.execute_control_intent",
                "runtime.control_commands",
                "runtime.command_handlers",
            ],
            "scan_layers": ["runtime", "surfaces"],
        },
    }
    findings = scan_invariant_predicate(
        predicate=predicate,
        workflow_root=_WORKFLOW_ROOT,
    )
    assert findings == [], (
        "Direct unified-dispatch callsites detected outside the command-bus "
        "authority. Route the call through "
        "runtime.control_commands.submit_workflow_command or "
        "execute_control_intent. Findings: " + str(findings)
    )


def test_scanner_runs_cleanly_against_live_tree() -> None:
    """Mechanism test: the scanner must execute against the real workflow tree
    without crashing and must return a list of well-formed finding records.

    This complements the live cleanup test by proving the scanner is
    operational even as new predicates are added.
    """

    predicate = {
        "predicate_slug": "workflow_launch.flow_through_command_bus",
        "predicate_kind": "invariant",
        "propagation_policy": {
            "forbidden_callsites_outside_command_bus": [
                "runtime.workflow.unified.submit_workflow_inline",
            ],
            "allowed_authorities": [
                "runtime.control_commands.submit_workflow_command",
                "runtime.control_commands",
                "runtime.command_handlers",
            ],
            "scan_layers": ["runtime", "surfaces"],
        },
    }
    findings = scan_invariant_predicate(
        predicate=predicate,
        workflow_root=_WORKFLOW_ROOT,
    )
    assert isinstance(findings, list)
    for f in findings:
        assert set(f.keys()) >= {"predicate_slug", "callsite", "path", "line", "rule"}
        assert f["predicate_slug"] == "workflow_launch.flow_through_command_bus"
        assert f["callsite"] == "runtime.workflow.unified.submit_workflow_inline"
        assert isinstance(f["line"], int) and f["line"] > 0
