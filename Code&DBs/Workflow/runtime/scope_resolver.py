"""Graph-based scope resolver for workflow specs.

Extends the AST import parsing in health_map.py into a full scope resolver.
Given a write_scope (list of file paths), automatically computes the read_scope
and context_sections the model needs to do the job correctly.

The import graph tells us:
  - what each file imports (direct dependencies)
  - what imports each file (reverse deps — what would break)
  - which test files exercise each file
  - the blast radius (transitive reverse deps)

This context is pre-computed and injected into the prompt, so the model
sees the interfaces without needing to discover them at runtime.

Cache: The ImportGraph is cached for 60 seconds to avoid scanning the
filesystem on every dispatch.
"""

from __future__ import annotations

import ast
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# ImportGraph
# ---------------------------------------------------------------------------

class ScopeResolutionError(ValueError):
    """Raised when a workflow scope file reference cannot be resolved exactly."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        file_path: str,
        matches: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.file_path = file_path
        self.matches = matches


class ImportGraph:
    """File-level import graph for a Python project.

    Nodes are file paths relative to ``root_dir``.
    Edges are "imports" relationships derived from AST analysis.
    """

    def __init__(
        self,
        root_dir: str,
        *,
        forward: dict[str, set[str]],   # file -> set of files it imports
        reverse: dict[str, set[str]],   # file -> set of files that import it
        all_files: set[str],
    ) -> None:
        self._root_dir = root_dir
        self._forward = forward   # path -> {path, ...}
        self._reverse = reverse   # path -> {path, ...}
        self._all_files = all_files

    @classmethod
    def build(cls, root_dir: str) -> "ImportGraph":
        """Parse all .py files under root_dir and build the adjacency lists.

        File paths stored in the graph are relative to root_dir so that
        callers can work with either relative or basename lookups.
        """
        root = Path(root_dir).resolve()
        forward: dict[str, set[str]] = {}
        reverse: dict[str, set[str]] = {}
        all_files: set[str] = set()

        # Map from module stem → list of relative paths (there can be
        # multiple files with the same stem in different packages).
        stem_to_paths: dict[str, list[str]] = {}

        # First pass: discover all .py files and their stems
        py_files: list[Path] = []
        for py_file in root.rglob("*.py"):
            rel = str(py_file.relative_to(root))
            all_files.add(rel)
            py_files.append(py_file)
            stem = py_file.stem
            stem_to_paths.setdefault(stem, []).append(rel)
            # Also index by "package.stem" notation for from-imports
            parts = list(py_file.relative_to(root).parts)
            if len(parts) > 1:
                pkg_stem = ".".join(p.replace(".py", "") for p in parts)
                stem_to_paths.setdefault(pkg_stem, []).append(rel)
                # index by last two segments as well (runtime.workflow → dispatch)
                for depth in range(1, len(parts)):
                    sub_key = ".".join(p.replace(".py", "") for p in parts[depth:])
                    stem_to_paths.setdefault(sub_key, []).append(rel)

        # Second pass: parse imports and build edges
        for py_file in py_files:
            rel = str(py_file.relative_to(root))
            forward[rel] = set()

            try:
                source = py_file.read_text(encoding="utf-8", errors="replace")
                tree = ast.parse(source)
            except (SyntaxError, OSError):
                continue

            imported_names: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported_names.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imported_names.add(node.module)
                        # Collect submodule names too (from .dispatch import X)
                        if node.level and node.level > 0:
                            # Relative import — resolve against current package
                            pkg_parts = list(py_file.relative_to(root).parent.parts)
                            up = node.level - 1
                            if up < len(pkg_parts):
                                base_parts = pkg_parts[:len(pkg_parts) - up]
                            else:
                                base_parts = []
                            if node.module:
                                resolved = ".".join(base_parts + node.module.split("."))
                            else:
                                resolved = ".".join(base_parts)
                            imported_names.add(resolved)

            # Resolve each imported name to a file path
            for name in imported_names:
                # Try progressively shorter suffixes
                candidates: list[str] = []
                parts_name = name.split(".")
                for depth in range(len(parts_name), 0, -1):
                    key = ".".join(parts_name[:depth])
                    if key in stem_to_paths:
                        candidates.extend(stem_to_paths[key])
                        break
                    # Also try just the last segment
                    last = parts_name[depth - 1]
                    if last in stem_to_paths:
                        candidates.extend(stem_to_paths[last])
                        break

                for candidate in candidates:
                    if candidate != rel:  # skip self-imports
                        forward[rel].add(candidate)
                        reverse.setdefault(candidate, set()).add(rel)

        # Ensure every file has an entry in reverse even if nothing imports it
        for f in all_files:
            reverse.setdefault(f, set())

        return cls(root_dir=str(root), forward=forward, reverse=reverse, all_files=all_files)

    # ------------------------------------------------------------------
    # Normalisation helper
    # ------------------------------------------------------------------

    def _resolve(self, file_path: str) -> str:
        """Normalise a caller-supplied path to the key used in the graph.

        Accepts:
          - relative paths (runtime/dispatch.py)
          - absolute paths (/path/to/root/runtime/dispatch.py)
          - bare stems (dispatch) — matched against the graph
        """
        root = Path(self._root_dir)
        raw = str(file_path or "").strip()
        if not raw:
            raise ScopeResolutionError(
                "scope.file_ref_unresolved",
                "empty scope file reference cannot be resolved",
                file_path=str(file_path),
            )

        # Absolute path → make relative
        p = Path(raw)
        if p.is_absolute():
            try:
                relative = str(p.relative_to(root))
            except ValueError as exc:
                raise ScopeResolutionError(
                    "scope.file_ref_outside_root",
                    f"scope file reference is outside root {root}: {raw}",
                    file_path=raw,
                ) from exc
            if relative in self._forward:
                return relative
            raise ScopeResolutionError(
                "scope.file_ref_unresolved",
                f"scope file reference {raw!r} is under {root} but is not in the Python import graph",
                file_path=raw,
            )

        # Already relative and exists as a key
        if raw in self._forward:
            return raw

        # Normalise separators
        normalised = raw.replace("\\", "/").lstrip("./")
        if normalised in self._forward:
            return normalised

        suffix_matches = [f for f in self._all_files if f.endswith(normalised)]
        if len(suffix_matches) == 1:
            return suffix_matches[0]
        if len(suffix_matches) > 1:
            ordered = tuple(sorted(suffix_matches))
            raise ScopeResolutionError(
                "scope.file_ref_ambiguous",
                f"scope file reference {raw!r} matched multiple files: {ordered}",
                file_path=raw,
                matches=ordered,
            )

        # Bare stem match. Do not use a stem from a longer path; that would
        # turn a misspelled qualified ref into a surprising unrelated file.
        if "/" not in normalised:
            stem = Path(raw).stem
            matches = [f for f in self._all_files if Path(f).stem == stem]
        else:
            matches = []
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            ordered = tuple(sorted(matches))
            raise ScopeResolutionError(
                "scope.file_ref_ambiguous",
                f"scope file reference {raw!r} matched multiple files: {ordered}",
                file_path=raw,
                matches=ordered,
            )

        raise ScopeResolutionError(
            "scope.file_ref_unresolved",
            f"scope file reference {raw!r} does not match any Python file under {root}",
            file_path=raw,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def imports_of(self, file_path: str) -> list[str]:
        """Direct dependencies — what this file imports."""
        key = self._resolve(file_path)
        return sorted(self._forward.get(key, set()))

    def importers_of(self, file_path: str) -> list[str]:
        """Reverse dependencies — what would break if this file changes."""
        key = self._resolve(file_path)
        return sorted(self._reverse.get(key, set()))

    def tests_for(self, file_path: str) -> list[str]:
        """Test files (under a tests/ directory) that import this file."""
        key = self._resolve(file_path)
        importers = self._reverse.get(key, set())
        return sorted(
            f for f in importers
            if "test" in Path(f).parts or Path(f).stem.startswith("test_")
        )

    def siblings_of(self, file_path: str) -> list[str]:
        """Files in the same directory with similar name patterns."""
        key = self._resolve(file_path)
        target = Path(key)
        parent = target.parent
        stem = target.stem

        results: list[str] = []
        for f in self._all_files:
            p = Path(f)
            if p == target:
                continue
            if p.parent == parent:
                # Same directory — include if stem shares a significant prefix
                # (at least 4 chars) or is in a test file pattern
                common = _common_prefix_length(p.stem, stem)
                if common >= 4 or p.stem.startswith("test_") or stem.startswith("test_"):
                    results.append(f)
        return sorted(results)

    def blast_radius(self, file_path: str) -> list[str]:
        """Transitive reverse dependencies — everything downstream of this file."""
        key = self._resolve(file_path)
        visited: set[str] = set()
        queue = list(self._reverse.get(key, set()))
        while queue:
            node = queue.pop()
            if node in visited:
                continue
            visited.add(node)
            queue.extend(self._reverse.get(node, set()) - visited)
        return sorted(visited)


def _common_prefix_length(a: str, b: str) -> int:
    length = 0
    for x, y in zip(a, b):
        if x == y:
            length += 1
        else:
            break
    return length


# ---------------------------------------------------------------------------
# ImportGraph cache (60-second TTL)
# ---------------------------------------------------------------------------

@dataclass
class _CacheEntry:
    graph: ImportGraph
    built_at: float


_GRAPH_CACHE: dict[str, _CacheEntry] = {}
_CACHE_TTL_S = 60.0


def _get_cached_graph(root_dir: str) -> ImportGraph:
    """Return a cached ImportGraph for root_dir, rebuilding if stale."""
    resolved = str(Path(root_dir).resolve())
    now = time.monotonic()
    entry = _GRAPH_CACHE.get(resolved)
    if entry is None or (now - entry.built_at) > _CACHE_TTL_S:
        graph = ImportGraph.build(resolved)
        _GRAPH_CACHE[resolved] = _CacheEntry(graph=graph, built_at=now)
        return graph
    return entry.graph


# ---------------------------------------------------------------------------
# File signature extraction
# ---------------------------------------------------------------------------

def extract_file_signatures(file_path: str) -> dict[str, Any]:
    """Extract public interfaces from a Python file using AST.

    Returns a dict with:
      - ``functions``: list of {name, params, first_line}
      - ``classes``: list of class names
      - ``constants``: list of module-level constant names
      - ``imports``: list of import statement strings (first 20)
    """
    try:
        source = Path(file_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"functions": [], "classes": [], "constants": [], "imports": [], "error": "unreadable"}

    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return {"functions": [], "classes": [], "constants": [], "imports": [], "error": str(exc)}

    lines = source.splitlines()

    functions: list[dict[str, str]] = []
    classes: list[str] = []
    constants: list[str] = []
    imports: list[str] = []

    for node in ast.iter_child_nodes(tree):
        # --- imports ---
        if isinstance(node, ast.Import):
            for alias in node.names:
                asname = f" as {alias.asname}" if alias.asname else ""
                imports.append(f"import {alias.name}{asname}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            names = ", ".join(
                (f"{a.name} as {a.asname}" if a.asname else a.name)
                for a in node.names
            )
            dots = "." * (node.level or 0)
            imports.append(f"from {dots}{mod} import {names}")

        # --- classes ---
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)

        # --- module-level constants (ALL_CAPS or Type annotated assignments) ---
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    constants.append(target.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id.isupper():
                constants.append(node.target.id)

        # --- top-level functions ---
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            params = _format_params(node.args)
            # Grab the source line of the def
            first_line = ""
            if 1 <= node.lineno <= len(lines):
                first_line = lines[node.lineno - 1].rstrip()
            functions.append({
                "name": node.name,
                "params": params,
                "first_line": first_line,
            })

    # Also grab methods from classes (one level deep)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            for item in ast.iter_child_nodes(node):
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    params = _format_params(item.args)
                    first_line = ""
                    if 1 <= item.lineno <= len(lines):
                        first_line = lines[item.lineno - 1].rstrip()
                    functions.append({
                        "name": f"{node.name}.{item.name}",
                        "params": params,
                        "first_line": first_line,
                    })

    return {
        "functions": functions,
        "classes": classes,
        "constants": constants,
        "imports": imports[:20],  # cap at 20 to keep context compact
    }


def _format_params(args: ast.arguments) -> str:
    """Format function parameters as a compact string."""
    parts: list[str] = []
    for arg in args.posonlyargs:
        parts.append(arg.arg)
    if args.posonlyargs:
        parts.append("/")
    for arg in args.args:
        parts.append(arg.arg)
    if args.vararg:
        parts.append(f"*{args.vararg.arg}")
    elif args.kwonlyargs:
        parts.append("*")
    for arg in args.kwonlyargs:
        parts.append(arg.arg)
    if args.kwarg:
        parts.append(f"**{args.kwarg.arg}")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# ScopeResolution dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScopeResolution:
    """Full scope resolution result for a workflow spec."""

    write_scope: list[str]
    """The original write_scope from the spec."""

    computed_read_scope: list[str]
    """Files the model needs to read: direct deps + 1-level reverse deps."""

    test_scope: list[str]
    """Test files that exercise any write_scope file."""

    blast_radius: list[str]
    """Transitive reverse deps — everything that could break."""

    context_sections: list[dict[str, Any]]
    """Ready-to-inject context_sections for WorkflowSpec."""


# ---------------------------------------------------------------------------
# resolve_scope
# ---------------------------------------------------------------------------

def resolve_scope(
    write_scope: list[str],
    *,
    root_dir: str,
) -> ScopeResolution:
    """Compute read scope and context sections from write_scope.

    Steps:
      a. Build (or retrieve cached) ImportGraph from root_dir.
      b. For each file in write_scope:
           - collect direct imports (imports_of)
           - collect 1-level reverse deps (importers_of)
           - collect test files (tests_for)
      c. Deduplicate across all write_scope files.
      d. Read the first 100 lines of each computed read_scope file.
      e. Build context_sections with file signatures + import lists.
      f. Return ScopeResolution.

    Args:
        write_scope: Relative (or absolute) paths to files the spec will write.
        root_dir: Project root — the same as WorkflowSpec.workdir.

    Returns:
        ScopeResolution with all computed context pre-loaded.
    """
    graph = _get_cached_graph(root_dir)
    root = Path(root_dir).resolve()

    read_set: set[str] = set()
    test_set: set[str] = set()
    blast_set: set[str] = set()

    for wf in write_scope:
        read_set.update(graph.imports_of(wf))
        read_set.update(graph.importers_of(wf))
        test_set.update(graph.tests_for(wf))
        blast_set.update(graph.blast_radius(wf))

    # Exclude the write_scope files themselves from read_scope
    write_norm = {graph._resolve(wf) for wf in write_scope}
    read_set -= write_norm
    test_set -= write_norm
    blast_set -= write_norm

    computed_read_scope = sorted(read_set)
    test_scope = sorted(test_set)
    blast_radius_list = sorted(blast_set)

    # Build context_sections
    context_sections: list[dict[str, Any]] = []

    for rel_path in computed_read_scope:
        abs_path = str(root / rel_path)
        sigs = extract_file_signatures(abs_path)

        # Structural signatures only — agents read files themselves.
        content_parts: list[str] = []

        if sigs.get("imports"):
            content_parts.append("# Imports\n" + "\n".join(sigs["imports"]))

        if sigs.get("classes"):
            content_parts.append("# Classes\n" + ", ".join(sigs["classes"]))

        if sigs.get("functions"):
            func_lines = []
            for fn in sigs["functions"]:
                func_lines.append(fn["first_line"] if fn["first_line"] else f"def {fn['name']}({fn['params']})")
            content_parts.append("# Functions / Methods\n" + "\n".join(func_lines))

        if not content_parts:
            continue

        context_sections.append({
            "name": f"scope:{rel_path}",
            "content": "\n\n".join(content_parts),
        })

    return ScopeResolution(
        write_scope=list(write_scope),
        computed_read_scope=computed_read_scope,
        test_scope=test_scope,
        blast_radius=blast_radius_list,
        context_sections=context_sections,
    )
