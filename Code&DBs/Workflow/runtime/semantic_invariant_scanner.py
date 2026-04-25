"""Static scanner for invariant predicates declared in semantic_predicate_catalog.

Invariant predicates declare structural rules that should hold across the
codebase.  This module reads each enabled invariant predicate from the
catalog and produces a list of violations, where a violation is a callsite
that breaks the declared rule.

Initial supported policy shapes (under ``propagation_policy``):

  * ``forbidden_callsites_outside_command_bus`` — a list of dotted paths.
    Each occurrence in scanned source files is a violation unless the file
    is part of the command bus authority itself (``allowed_authorities``)
    or under ``tests/``.

  * ``forbidden_callsites`` — a generic list of dotted paths that must not
    appear anywhere in the scanned source layers.  No allow-list.

The scanner returns structured findings; callers render or assert.

Today's scan is grep-style on Python source.  Future iterations could
parse the AST or walk import graphs, but the simple form catches the
class of regressions this audit addressed.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


def _scan_layers(workflow_root: Path, layers: Iterable[str]) -> list[Path]:
    """Return a list of ``.py`` files under each declared layer."""
    files: list[Path] = []
    for layer in layers:
        layer_dir = workflow_root / layer
        if not layer_dir.is_dir():
            continue
        for path in layer_dir.rglob("*.py"):
            if "/__pycache__/" in str(path) or "/tests/" in str(path):
                continue
            files.append(path)
    return files


def _file_imports_or_uses_callsite(source: str, callsite: str) -> list[int]:
    """Return line numbers where ``callsite`` is used in the file.

    Detects two shapes:

    1. The full dotted path appearing on a line (``runtime.x.y.foo(...)``).
    2. ``from runtime.x.y import foo`` followed anywhere in the file by a
       bare ``foo(`` call or use.  In that case both the import line and
       any usage line are reported.
    """
    if "." not in callsite:
        return []
    module_path, _, callable_name = callsite.rpartition(".")
    lines = source.splitlines()
    findings: list[int] = []

    # Check for full dotted reference.
    for line_no, line in enumerate(lines, start=1):
        if callsite in line:
            findings.append(line_no)

    # Check for bare-name imports of the callable.
    import_patterns = (
        f"from {module_path} import",
    )
    imports_callable = False
    import_line_no: int | None = None
    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if any(stripped.startswith(p) for p in import_patterns) and callable_name in stripped:
            # Confirm callable_name appears as a token (comma- or whitespace-separated).
            after_import = stripped.split("import", 1)[1]
            tokens = [t.strip().strip("()") for t in after_import.split(",")]
            tokens = [t.split(" as ")[0].strip() for t in tokens]
            if callable_name in tokens:
                imports_callable = True
                import_line_no = line_no
                findings.append(line_no)
                break

    if imports_callable:
        for line_no, line in enumerate(lines, start=1):
            if line_no == import_line_no:
                continue
            # Bare-name usage as ``foo(`` somewhere on the line.
            if f"{callable_name}(" in line:
                findings.append(line_no)

    # Dedupe + sort.
    return sorted(set(findings))


def _allowed_authority_paths(workflow_root: Path, allowed: Iterable[str]) -> set[Path]:
    """Resolve allowed_authorities (dotted module paths) to filesystem paths."""
    resolved: set[Path] = set()
    for entry in allowed:
        # Drop the trailing ``.callable`` segment if present; we want the module path.
        parts = str(entry).split(".")
        # Walk back until we find an existing file or directory.
        while parts:
            candidate = workflow_root.joinpath(*parts).with_suffix(".py")
            if candidate.exists():
                resolved.add(candidate)
                break
            dir_candidate = workflow_root.joinpath(*parts)
            if dir_candidate.is_dir():
                resolved.add(dir_candidate)
                break
            parts = parts[:-1]
    return resolved


def _path_under_any(path: Path, roots: Iterable[Path]) -> bool:
    for root in roots:
        try:
            path.relative_to(root)
        except ValueError:
            continue
        return True
    return False


def scan_invariant_predicate(
    *,
    predicate: Mapping[str, Any],
    workflow_root: Path,
) -> list[dict[str, Any]]:
    """Run one invariant predicate and return its findings.

    Returns a list of ``{predicate_slug, callsite, path, line}`` entries.
    """

    slug = str(predicate.get("predicate_slug") or "").strip()
    policy_value = predicate.get("propagation_policy") or {}
    if isinstance(policy_value, str):
        try:
            policy = json.loads(policy_value)
        except json.JSONDecodeError:
            policy = {}
    else:
        policy = dict(policy_value)

    forbidden_outside_bus = policy.get("forbidden_callsites_outside_command_bus") or []
    forbidden_anywhere = policy.get("forbidden_callsites") or []
    if not forbidden_outside_bus and not forbidden_anywhere:
        return []

    layers = policy.get("scan_layers") or ["runtime", "surfaces"]
    files = _scan_layers(workflow_root, layers)
    allowed_paths = _allowed_authority_paths(
        workflow_root,
        policy.get("allowed_authorities") or [],
    )

    findings: list[dict[str, Any]] = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        path_under_allowed = _path_under_any(path, allowed_paths)
        for callsite in forbidden_anywhere:
            if not callsite:
                continue
            for line_no in _file_imports_or_uses_callsite(text, callsite):
                findings.append(
                    {
                        "predicate_slug": slug,
                        "callsite": callsite,
                        "path": str(path.relative_to(workflow_root)),
                        "line": line_no,
                        "rule": "forbidden_callsites",
                    }
                )
        for callsite in forbidden_outside_bus:
            if not callsite:
                continue
            if path_under_allowed:
                continue
            for line_no in _file_imports_or_uses_callsite(text, callsite):
                findings.append(
                    {
                        "predicate_slug": slug,
                        "callsite": callsite,
                        "path": str(path.relative_to(workflow_root)),
                        "line": line_no,
                        "rule": "forbidden_callsites_outside_command_bus",
                    }
                )
    return findings


def scan_all_invariant_predicates(
    *,
    predicates: Iterable[Mapping[str, Any]],
    workflow_root: Path,
) -> list[dict[str, Any]]:
    """Scan every invariant predicate; flatten the findings."""

    findings: list[dict[str, Any]] = []
    for predicate in predicates:
        if predicate.get("predicate_kind") != "invariant":
            continue
        findings.extend(
            scan_invariant_predicate(
                predicate=predicate,
                workflow_root=workflow_root,
            )
        )
    return findings


__all__ = [
    "scan_invariant_predicate",
    "scan_all_invariant_predicates",
]
