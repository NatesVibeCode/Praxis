"""Anti-bypass tests for the runtime state-semantics consumer boundary.

Consumers of bug status shapes in the ``runtime/`` and ``surfaces/`` trees
must route through the state-semantics authority:

  * ``runtime.primitive_contracts`` — defines ``_BUG_STATUS_SEMANTICS`` and
    exports ``bug_open_status_values`` / ``bug_resolved_status_values`` /
    ``bug_resolved_status_values_with_legacy`` /
    ``bug_status_legacy_resolved_aliases`` / ``bug_status_sql_in_literal``
    / ``bug_status_sql_equals_literal``.
  * ``runtime.bug_tracker`` — defines ``class BugStatus(enum.Enum)`` with
    members ``OPEN``, ``IN_PROGRESS``, ``FIXED``, ``WONT_FIX``,
    ``DEFERRED`` and owns bug-state transition writes.

Concretely, no module outside the state-semantics authority may:

  * hand-roll a collection literal containing two or more canonical bug
    statuses (e.g. ``{"OPEN", "IN_PROGRESS"}``); or
  * compare against a uniquely-bug-status literal (``IN_PROGRESS``,
    ``FIXED``, ``WONT_FIX``, ``DEFERRED``) via ``==`` / ``!=``; or
  * assign a bug-status literal into a ``next_status`` or
    ``current_status`` slot (dict/kwarg).

``OPEN`` is excluded from the unique-compare set because the token is
also used for circuit-breaker and event states — so bare ``== "OPEN"``
does not uniquely imply a bug-status consumer. A ``"OPEN", "FIXED"``
collection literal, however, is unique to bugs.

This test walks ``runtime/**/*.py`` and ``surfaces/**/*.py`` and fails
when a non-authority module introduces a forbidden pattern.

It enforces:
``decision.2026-04-19.runtime-state-semantics-consumer-boundary``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


# Modules that ARE the state-semantics authority consumers should route
# through. ``bug_tracker`` is treated as a co-authority because it
# defines the ``BugStatus`` enum and persists the writes directly — its
# SQL literals, transition code and internal normalization are the
# canonical truth.
_STATE_SEMANTICS_AUTHORITY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/bug_tracker.py",
        "runtime/primitive_contracts.py",
    }
)


_BUG_STATUS_ANY = r"(?:OPEN|IN_PROGRESS|FIXED|WONT_FIX|DEFERRED)"
# Bug-status values that are uniquely bug-shaped. ``OPEN`` is omitted
# because it overlaps with circuit-breaker and generic event vocabulary.
_BUG_STATUS_UNIQUE = r"(?:IN_PROGRESS|FIXED|WONT_FIX|DEFERRED)"


# Forbidden patterns target code-level constructs where canonical bug
# statuses appear in a status-shaped context. Documentation and
# user-facing help strings that reference the tokens are not matched
# unless they happen to land inside one of these code shapes.
_FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Collection literal pairing two or more canonical bug statuses.
    re.compile(
        rf"""["']{_BUG_STATUS_ANY}["']\s*,\s*["']{_BUG_STATUS_ANY}["']"""
    ),
    # Equality / inequality against a uniquely-bug-status literal.
    re.compile(
        rf"""(?:==|!=)\s*["']{_BUG_STATUS_UNIQUE}["']"""
    ),
    # Reverse equality / inequality with a uniquely-bug-status literal.
    re.compile(
        rf"""["']{_BUG_STATUS_UNIQUE}["']\s*(?:==|!=)"""
    ),
    # ``next_status`` / ``current_status`` dict/kwarg assigned a bug status.
    re.compile(
        rf"""["']?(?:next|current)_status["']?\s*[:=]\s*["']{_BUG_STATUS_ANY}["']"""
    ),
)


def _relative_python_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if p.is_file())


def _as_relative_key(path: Path) -> str:
    return path.relative_to(_WORKFLOW_ROOT).as_posix()


@pytest.mark.parametrize(
    "scanned_root",
    [
        _WORKFLOW_ROOT / "runtime",
        _WORKFLOW_ROOT / "surfaces",
    ],
    ids=["runtime", "surfaces"],
)
def test_no_raw_bug_status_literals(scanned_root: Path) -> None:
    """No module outside the state-semantics authority may hand-roll bug-status literals."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _STATE_SEMANTICS_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        for pattern in _FORBIDDEN_PATTERNS:
            for match in pattern.finditer(source):
                offenders.append(
                    f"{key}: matches {pattern.pattern!r} -> {match.group(0)!r}"
                )
    assert not offenders, (
        "Raw bug-status literals detected — route through "
        "runtime.primitive_contracts.bug_open_status_values / "
        "bug_resolved_status_values / bug_status_sql_equals_literal, or "
        "runtime.bug_tracker.BugStatus.<NAME>.value:\n  - "
        + "\n  - ".join(offenders)
    )


def test_state_semantics_authority_allowlist_paths_exist() -> None:
    """Fail loudly if an allowlist entry drifts out of sync with the filesystem."""
    for relative in _STATE_SEMANTICS_AUTHORITY_ALLOWLIST:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), (
            f"allowlisted state-semantics authority path does not exist: {relative}"
        )


def test_bug_open_status_values_projects_from_authority() -> None:
    """Smoke check: primitive_contracts exports the canonical open set."""
    from runtime.primitive_contracts import (
        bug_open_status_values,
        bug_resolved_status_values,
    )

    open_statuses = set(bug_open_status_values())
    resolved_statuses = set(bug_resolved_status_values())
    assert open_statuses == {"OPEN", "IN_PROGRESS"}
    assert resolved_statuses == {"FIXED", "WONT_FIX", "DEFERRED"}
    # The open and resolved partitions must be disjoint by construction.
    assert open_statuses.isdisjoint(resolved_statuses)


def test_bug_status_enum_matches_state_semantics_contract() -> None:
    """Smoke check: BugStatus enum and _BUG_STATUS_SEMANTICS stay aligned."""
    from runtime.bug_tracker import BugStatus
    from runtime.primitive_contracts import (
        bug_open_status_values,
        bug_resolved_status_values,
    )

    enum_values = {member.value for member in BugStatus}
    contract_values = set(bug_open_status_values()) | set(bug_resolved_status_values())
    assert enum_values == contract_values


def test_state_semantics_contract_projects_predicates() -> None:
    """Smoke check: the contract projects status predicates and helpers from authority."""
    from runtime.primitive_contracts import (
        build_state_semantics_contract,
        bug_open_status_values,
        bug_resolved_status_values,
    )

    contract = build_state_semantics_contract()
    bug_section = contract["bug"]
    assert set(bug_section["open_statuses"]) == set(bug_open_status_values())
    assert set(bug_section["resolved_statuses"]) == set(bug_resolved_status_values())
    predicates = bug_section["status_predicates"]
    assert predicates["OPEN"]["is_open"] is True
    assert predicates["FIXED"]["is_resolved"] is True


def test_bug_query_default_open_only_helpers_pin_two_policies() -> None:
    """Pin the two canonical bug-query open_only defaults and their semantic roles.

    Machine-facing list surfaces (API /bugs, MCP praxis_bugs) default open_only
    to False so generic consumers can see the full bug set. Operator-backlog
    surfaces (CLI workflow bugs list, praxis_issue_backlog,
    praxis_bug_replay_provenance_backfill) default open_only to True so the
    actionable slice is the zero-flag path.

    Changing either default must be a deliberate contract move, not an
    incidental edit to a handler.
    """
    from runtime.primitive_contracts import (
        bug_query_default_open_only_backlog,
        bug_query_default_open_only_list,
        build_state_semantics_contract,
    )

    assert bug_query_default_open_only_list() is False
    assert bug_query_default_open_only_backlog() is True

    contract = build_state_semantics_contract()
    query_defaults = contract["bug"]["query_defaults"]
    assert query_defaults["list"]["open_only"] is False
    assert query_defaults["backlog"]["open_only"] is True
    assert (
        query_defaults["list"]["helper"]
        == "runtime.primitive_contracts.bug_query_default_open_only_list"
    )
    assert (
        query_defaults["backlog"]["helper"]
        == "runtime.primitive_contracts.bug_query_default_open_only_backlog"
    )


def test_bug_query_default_open_only_consumers_route_through_authority() -> None:
    """Every surface that declares a bug-query open_only default must import the authority helper.

    Hand-rolled ``open_only=True``/``False`` defaults would re-diverge the
    machine-vs-operator contract. This test walks the known consumer files and
    asserts each imports one of the authority helpers.
    """
    consumers: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "surfaces/api/handlers/_bug_surface_contract.py",
            (
                "bug_query_default_open_only_list",
                "bug_query_default_open_only_backlog",
            ),
        ),
        (
            "surfaces/api/handlers/_query_bugs.py",
            ("bug_query_default_open_only_list",),
        ),
        (
            "surfaces/api/handlers/workflow_query_core.py",
            ("bug_query_default_open_only_backlog",),
        ),
        (
            "surfaces/mcp/tools/operator.py",
            ("bug_query_default_open_only_backlog",),
        ),
        (
            "surfaces/cli/commands/query.py",
            ("bug_query_default_open_only_backlog",),
        ),
        (
            "runtime/operations/queries/operator_observability.py",
            ("bug_query_default_open_only_backlog",),
        ),
        (
            "runtime/operations/commands/operator_maintenance.py",
            ("bug_query_default_open_only_backlog",),
        ),
    )

    missing: list[str] = []
    for relative, required_names in consumers:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), f"consumer file missing: {relative}"
        source = path.read_text(encoding="utf-8")
        for name in required_names:
            if name not in source:
                missing.append(f"{relative}: missing import of {name}")
    assert not missing, (
        "bug-query open_only consumers must route through the authority "
        "helpers in runtime.primitive_contracts:\n  - " + "\n  - ".join(missing)
    )
