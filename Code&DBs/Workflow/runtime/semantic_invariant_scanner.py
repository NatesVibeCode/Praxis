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

The scanner is intentionally lightweight, but it now uses Python's AST so
module-alias imports such as ``from runtime.workflow import unified`` are
resolved instead of silently bypassing the invariant.
"""

from __future__ import annotations

import ast
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

    Detects three shapes:

    1. The full dotted call appears directly (``runtime.x.y.foo(...)``).
    2. The callable is imported by name and used bare later
       (``from runtime.x.y import foo`` then ``foo(...)``).
    3. The module or one of its parents is imported under an alias and the
       callable is reached through that alias
       (``from runtime.x import y`` then ``y.foo(...)``).
    """
    if "." not in callsite:
        return []
    module_path, _, callable_name = callsite.rpartition(".")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        lines = source.splitlines()
        return sorted(
            {
                line_no
                for line_no, line in enumerate(lines, start=1)
                if callsite in line or f"{callable_name}(" in line
            }
        )

    findings: set[int] = set()
    callable_aliases: set[str] = set()
    module_aliases: dict[str, str] = {}

    def _bind_module_alias(bound_name: str, resolved_module: str, line_no: int) -> None:
        if not bound_name:
            return
        module_aliases[bound_name] = resolved_module
        if module_path == resolved_module or module_path.startswith(f"{resolved_module}."):
            findings.add(line_no)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if not node.module:
                continue
            for alias in node.names:
                local_name = alias.asname or alias.name
                imported_module = f"{node.module}.{alias.name}"
                if node.module == module_path and alias.name == callable_name:
                    callable_aliases.add(local_name)
                    findings.add(node.lineno)
                    continue
                _bind_module_alias(local_name, imported_module, node.lineno)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".", 1)[0]
                _bind_module_alias(local_name, alias.name, node.lineno)

    def _attribute_chain_name(node: ast.AST) -> str | None:
        parts: list[str] = []
        current = node
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if not isinstance(current, ast.Name):
            return None
        parts.append(current.id)
        return ".".join(reversed(parts))

    def _expand_alias(dotted_name: str) -> str:
        head, dot, tail = dotted_name.partition(".")
        if not dot:
            return module_aliases.get(head, dotted_name)
        resolved_head = module_aliases.get(head, head)
        return f"{resolved_head}.{tail}"

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in callable_aliases:
            findings.add(node.lineno)
            continue
        dotted_name = _attribute_chain_name(func)
        if dotted_name and _expand_alias(dotted_name) == callsite:
            findings.add(node.lineno)

    return sorted(findings)


def _allowed_authority_paths(workflow_root: Path, allowed: Iterable[str]) -> set[Path]:
    """Resolve allowed_authorities (dotted module paths) to filesystem paths.

    Strategy: try the full path as a module file or package dir first; if
    that fails, drop exactly one trailing ``.callable`` segment and try
    again.  No greedy walk-back — that would silently widen the allow-list
    to a parent directory and let bypass sites slip through.
    """

    resolved: set[Path] = set()
    for entry in allowed:
        parts = str(entry).split(".")
        candidates_to_try: list[list[str]] = []
        if parts:
            candidates_to_try.append(parts)
        if len(parts) > 1:
            candidates_to_try.append(parts[:-1])
        for attempt in candidates_to_try:
            file_candidate = workflow_root.joinpath(*attempt).with_suffix(".py")
            if file_candidate.exists():
                resolved.add(file_candidate)
                break
            dir_candidate = workflow_root.joinpath(*attempt)
            if dir_candidate.is_dir():
                resolved.add(dir_candidate)
                break
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
