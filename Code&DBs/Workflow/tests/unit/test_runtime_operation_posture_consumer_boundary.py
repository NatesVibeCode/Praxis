"""Anti-bypass tests for the runtime operation-posture consumer boundary.

Consumers and producers of operation posture in the ``runtime/`` and
``surfaces/`` trees must route through the authority — ``runtime.posture.Posture``
(the canonical enum) and ``runtime.primitive_contracts.build_operation_posture_contract``
(which projects posture rules from the same catalog authority).

Concretely, no module outside the posture authority may:

  * hand-roll a posture assignment like ``recommended_posture = "observe"``
    (producers must emit ``Posture.OBSERVE.value`` so the enum stays the
    only naming source), or
  * hand-roll a posture comparison like ``posture == "operate"`` (consumers
    must compare against ``Posture.OPERATE.value`` or route through the
    posture enforcer), or
  * embed a raw ``"observe" | "operate" | "build"`` literal in a
    posture-shaped context (``posture in ("observe", ...)`` and the like).

This test walks ``runtime/**/*.py`` and ``surfaces/**/*.py`` and fails when
a non-authority module introduces one of the forbidden patterns.

It enforces:
``decision.2026-04-19.runtime-operation-posture-consumer-boundary``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


# Modules that ARE the posture authority consumers should route through.
# Paths are relative to _WORKFLOW_ROOT.
_POSTURE_AUTHORITY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/posture.py",
        "runtime/primitive_contracts.py",
    }
)


# Forbidden patterns. These target code-level constructs where a posture
# value appears as a raw string literal in a posture-shaped context —
# either a producer assignment (``recommended_posture="observe"``,
# ``self._posture = "operate"``) or a consumer comparison
# (``posture == "build"``, ``posture in ("observe", ...)``). Error
# messages and docstrings that happen to mention the words are not in
# a posture-shaped context and are not matched.
_POSTURE_VALUES = r"(?:observe|operate|build)"

_FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Assignment: ``recommended_posture = "observe"`` /
    # ``recommended_posture="observe"`` (kwarg form) — producers must
    # emit ``Posture.X.value``.
    re.compile(
        rf"""recommended_posture\s*=\s*["']{_POSTURE_VALUES}["']"""
    ),
    # Assignment: ``self._posture = "observe"`` or
    # ``self._posture: str = "observe"`` — private posture state must
    # seed from the Posture enum.
    re.compile(
        rf"""_posture\s*(?::\s*[A-Za-z_][\w\[\], ]*)?\s*=\s*["']{_POSTURE_VALUES}["']"""
    ),
    # Comparison: ``posture == "observe"`` / ``posture != "operate"``.
    re.compile(
        rf"""posture\s*(?:==|!=)\s*["']{_POSTURE_VALUES}["']"""
    ),
    # Reversed comparison: ``"observe" == posture`` / ``"build" != posture``.
    re.compile(
        rf"""["']{_POSTURE_VALUES}["']\s*(?:==|!=)\s*posture"""
    ),
    # Membership: ``posture in ("observe", ...)`` / ``posture in {{"operate"}}``.
    re.compile(
        rf"""posture\s+in\s+[\(\[\{{][^\)\]\}}]*["']{_POSTURE_VALUES}["']"""
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
def test_no_raw_posture_literal_assignments_or_compares(scanned_root: Path) -> None:
    """No module outside the posture authority may hand-roll posture literals."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _POSTURE_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        for pattern in _FORBIDDEN_PATTERNS:
            for match in pattern.finditer(source):
                offenders.append(f"{key}: matches {pattern.pattern!r} -> {match.group(0)!r}")
    assert not offenders, (
        "Raw posture literals detected — route through "
        "runtime.posture.Posture (Posture.OBSERVE / OPERATE / BUILD) or "
        "runtime.primitive_contracts.build_operation_posture_contract():\n  - "
        + "\n  - ".join(offenders)
    )


def test_posture_authority_allowlist_paths_exist() -> None:
    """Fail loudly if an allowlist entry drifts out of sync with the filesystem."""
    for relative in _POSTURE_AUTHORITY_ALLOWLIST:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), (
            f"allowlisted posture authority path does not exist: {relative}"
        )


def test_posture_enum_projects_canonical_values() -> None:
    """Smoke check: the Posture enum owns the observe/operate/build naming."""
    from runtime.posture import Posture

    assert Posture.OBSERVE.value == "observe"
    assert Posture.OPERATE.value == "operate"
    assert Posture.BUILD.value == "build"


def test_operation_posture_contract_projects_posture_rules() -> None:
    """Smoke check: the contract exposes posture rules for consumers."""
    from runtime.primitive_contracts import build_operation_posture_contract

    contract = build_operation_posture_contract()
    rules = contract["posture_rules"]
    assert set(rules) == {"observe", "operate", "build"}
    # OBSERVE forbids mutations; OPERATE and BUILD allow them.
    assert "mutate" in rules["observe"]["forbids"]
    assert "mutate" in rules["operate"]["allows"]
    assert "mutate" in rules["build"]["allows"]
