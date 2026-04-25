"""Anti-bypass tests for the runtime proof-ref consumer boundary.

Consumers of proof-ref shapes in the ``runtime/`` and ``surfaces/`` trees
must route through the authority:

  * ``runtime.bug_evidence`` — defines ``EVIDENCE_ROLE_OBSERVED_IN``,
    ``EVIDENCE_ROLE_ATTEMPTED_FIX``, ``EVIDENCE_ROLE_VALIDATES_FIX``,
    ``EVIDENCE_ROLE_DISCOVERED_BY`` (and the ``ALLOWED_EVIDENCE_ROLES`` /
    ``ALLOWED_EVIDENCE_KINDS`` sets).
  * ``runtime.primitive_contracts.build_proof_ref_contract`` — projects
    the proof-ref primitive (allowed ref kinds, evidence kinds, and
    evidence roles) from the authority for /orient consumers.
  * ``runtime.proof_timeline`` — projects existing receipt, run,
    verification, healing, and bug-evidence rows into one queryable
    proof timeline.

Concretely, no module outside the proof-ref authority may hand-roll
``evidence_role="validates_fix"`` (or the other role literals) in a
kwarg / assignment / comparison context. Consumers MUST import the
named constants from ``runtime.bug_evidence`` instead.

This test walks ``runtime/**/*.py`` and ``surfaces/**/*.py`` and fails
when a non-authority module introduces the forbidden pattern.

It enforces:
``decision.2026-04-19.runtime-proof-ref-consumer-boundary``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


# Modules that ARE the proof-ref authority consumers should route
# through. ``bug_tracker`` is treated as a co-authority because it
# persists and queries bug-evidence rows directly — its SQL literals
# and internal normalization code are the canonical truth.
_PROOF_REF_AUTHORITY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/bug_evidence.py",
        "runtime/bug_tracker.py",
        "runtime/primitive_contracts.py",
        "runtime/proof_timeline.py",
    }
)


_EVIDENCE_ROLE_VALUES = r"(?:observed_in|attempted_fix|validates_fix|discovered_by)"


# Forbidden patterns target code-level constructs where a raw evidence
# role literal appears in a role-shaped context. Matches on
# ``evidence_role="validates_fix"`` (kwarg), ``evidence_role =
# "validates_fix"`` (assignment), and
# ``evidence_role == "validates_fix"`` (comparison). Documentation and
# error strings that mention the role by name are not matched.
_FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"""evidence_role\s*=\s*["']{_EVIDENCE_ROLE_VALUES}["']"""
    ),
    re.compile(
        rf"""evidence_role\s*(?:==|!=)\s*["']{_EVIDENCE_ROLE_VALUES}["']"""
    ),
    re.compile(
        rf"""["']{_EVIDENCE_ROLE_VALUES}["']\s*(?:==|!=)\s*evidence_role"""
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
def test_no_raw_evidence_role_literals(scanned_root: Path) -> None:
    """No module outside the proof-ref authority may hand-roll evidence_role literals."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _PROOF_REF_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        for pattern in _FORBIDDEN_PATTERNS:
            for match in pattern.finditer(source):
                offenders.append(f"{key}: matches {pattern.pattern!r} -> {match.group(0)!r}")
    assert not offenders, (
        "Raw evidence_role literals detected — route through "
        "runtime.bug_evidence.EVIDENCE_ROLE_VALIDATES_FIX / "
        "EVIDENCE_ROLE_OBSERVED_IN / EVIDENCE_ROLE_ATTEMPTED_FIX / "
        "EVIDENCE_ROLE_DISCOVERED_BY:\n  - "
        + "\n  - ".join(offenders)
    )


def test_proof_ref_authority_allowlist_paths_exist() -> None:
    """Fail loudly if an allowlist entry drifts out of sync with the filesystem."""
    for relative in _PROOF_REF_AUTHORITY_ALLOWLIST:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), (
            f"allowlisted proof-ref authority path does not exist: {relative}"
        )


def test_bug_evidence_exports_canonical_role_constants() -> None:
    """Smoke check: the authority exposes named constants for all roles."""
    from runtime.bug_evidence import (
        ALLOWED_EVIDENCE_ROLES,
        EVIDENCE_ROLE_ATTEMPTED_FIX,
        EVIDENCE_ROLE_DISCOVERED_BY,
        EVIDENCE_ROLE_OBSERVED_IN,
        EVIDENCE_ROLE_VALIDATES_FIX,
    )

    assert EVIDENCE_ROLE_OBSERVED_IN == "observed_in"
    assert EVIDENCE_ROLE_ATTEMPTED_FIX == "attempted_fix"
    assert EVIDENCE_ROLE_VALIDATES_FIX == "validates_fix"
    assert EVIDENCE_ROLE_DISCOVERED_BY == "discovered_by"
    assert ALLOWED_EVIDENCE_ROLES == frozenset(
        {
            EVIDENCE_ROLE_OBSERVED_IN,
            EVIDENCE_ROLE_ATTEMPTED_FIX,
            EVIDENCE_ROLE_VALIDATES_FIX,
            EVIDENCE_ROLE_DISCOVERED_BY,
        }
    )


def test_proof_ref_contract_projects_roles_from_authority() -> None:
    """Smoke check: the contract mirrors the bug_evidence authority roles."""
    from runtime.bug_evidence import ALLOWED_EVIDENCE_KINDS, ALLOWED_EVIDENCE_ROLES
    from runtime.primitive_contracts import build_proof_ref_contract
    from runtime.proof_timeline import PROOF_TIMELINE_AUTHORITY

    contract = build_proof_ref_contract()
    assert contract["authority"] == PROOF_TIMELINE_AUTHORITY
    assert contract["timeline_projection"]["bug_entrypoint"] == (
        "runtime.proof_timeline.bug_proof_timeline"
    )
    assert set(contract["allowed_evidence_roles"]) == set(ALLOWED_EVIDENCE_ROLES)
    assert set(contract["allowed_evidence_kinds"]) == set(ALLOWED_EVIDENCE_KINDS)
    assert "evidence_role" in contract["required_fields"]
