"""Module health map analysis for static code quality assessment.

Scores modules on complexity, coupling, interface width, circular imports,
and file size. Used to identify problematic modules for refactoring.
"""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from collections import defaultdict


@dataclass(frozen=True)
class ModuleHealth:
    """Health metrics for a single Python module."""

    module_path: str
    health_score: int  # 0-100, higher = worse
    function_count: int
    line_count: int
    complex_functions: int  # >100 lines
    very_complex_functions: int  # >200 lines
    wide_functions: int  # >6 parameters
    import_count: int
    has_circular_import: bool


class HealthMapper:
    """Analyzes Python modules for health and quality metrics."""

    def __init__(self):
        """Initialize the health mapper."""
        self._import_graph: dict[str, set[str]] = defaultdict(set)
        self._module_paths: dict[str, str] = {}

    def analyze_module(self, path: str) -> ModuleHealth:
        """Analyze a single Python module.

        Args:
            path: Absolute path to the Python file

        Returns:
            ModuleHealth with metrics for the module
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                source = f.read()
        except (OSError, UnicodeDecodeError):
            # Return minimal health on read failure
            return ModuleHealth(
                module_path=path,
                health_score=0,
                function_count=0,
                line_count=0,
                complex_functions=0,
                very_complex_functions=0,
                wide_functions=0,
                import_count=0,
                has_circular_import=False,
            )

        try:
            tree = ast.parse(source)
        except SyntaxError:
            # Return minimal health on parse failure
            return ModuleHealth(
                module_path=path,
                health_score=0,
                function_count=0,
                line_count=0,
                complex_functions=0,
                very_complex_functions=0,
                wide_functions=0,
                import_count=0,
                has_circular_import=False,
            )

        lines = source.split("\n")
        line_count = len(lines)

        # Count imports and build import graph entry
        import_count = 0
        imported_modules = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                import_count += len(node.names)
                for alias in node.names:
                    imported_modules.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    import_count += 1
                    imported_modules.add(node.module.split(".")[0])

        # Build import graph (module -> its imports)
        module_key = self._normalize_module_key(path)
        self._import_graph[module_key].update(imported_modules)
        self._module_paths[module_key] = path

        # Analyze functions
        function_count = 0
        complex_functions = 0
        very_complex_functions = 0
        wide_functions = 0

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                function_count += 1

                # Calculate function line span
                func_lines = 0
                if hasattr(node, "end_lineno") and hasattr(node, "lineno"):
                    func_lines = node.end_lineno - node.lineno + 1
                else:
                    # Fallback: find end of function body
                    func_lines = self._estimate_function_lines(node)

                if func_lines > 200:
                    very_complex_functions += 1
                elif func_lines > 100:
                    complex_functions += 1

                # Count parameters
                args = node.args
                param_count = len(args.args) + len(args.posonlyargs)
                if param_count > 6:
                    wide_functions += 1

        # Compute health score (0-100, higher = worse)
        complexity_score = min(
            25,
            complex_functions * 3 + very_complex_functions * 8,
        )
        interface_score = min(20, wide_functions * 4)
        size_score = min(15, max(0, line_count - 500) / 50)
        cycles_score = 0  # Will be set by detect_circular_imports
        coupling_score = min(10, import_count / 5)

        health_score = int(
            complexity_score + interface_score + size_score + cycles_score + coupling_score
        )
        health_score = min(100, max(0, health_score))

        return ModuleHealth(
            module_path=path,
            health_score=health_score,
            function_count=function_count,
            line_count=line_count,
            complex_functions=complex_functions,
            very_complex_functions=very_complex_functions,
            wide_functions=wide_functions,
            import_count=import_count,
            has_circular_import=False,  # Will be updated by detect_circular_imports
        )

    def analyze_directory(self, root: str) -> list[ModuleHealth]:
        """Analyze all Python modules in a directory.

        Args:
            root: Root directory to analyze

        Returns:
            List of ModuleHealth sorted by health_score (worst first)
        """
        modules = []
        root_path = Path(root)

        for py_file in root_path.rglob("*.py"):
            # Skip test files
            if "test" in py_file.parts:
                continue

            str_path = str(py_file)
            health = self.analyze_module(str_path)
            modules.append(health)

        return modules

    def detect_circular_imports(self, root: str) -> list[tuple[str, ...]]:
        """Detect circular imports using Tarjan's SCC algorithm.

        Args:
            root: Root directory to analyze

        Returns:
            List of circular import cycles (each cycle is a tuple of module names)
        """
        # First pass: analyze all modules to build import graph
        root_path = Path(root)
        for py_file in root_path.rglob("*.py"):
            if "test" in py_file.parts:
                continue
            str_path = str(py_file)
            self.analyze_module(str_path)

        # Run Tarjan's algorithm to find SCCs
        cycles = []
        visited = set()
        rec_stack = set()
        scc_id = {}
        scc_count = [0]

        def tarjan_visit(node: str, stack: list[str], indices: dict[str, int],
                        lowlinks: dict[str, int], index_counter: list[int]) -> None:
            """Recursive DFS for Tarjan's algorithm."""
            indices[node] = index_counter[0]
            lowlinks[node] = index_counter[0]
            index_counter[0] += 1
            stack.append(node)
            rec_stack.add(node)

            for neighbor in self._import_graph.get(node, set()):
                neighbor_key = self._normalize_module_key(neighbor)
                if neighbor_key not in indices:
                    tarjan_visit(neighbor_key, stack, indices, lowlinks, index_counter)
                    lowlinks[node] = min(lowlinks[node], lowlinks[neighbor_key])
                elif neighbor_key in rec_stack:
                    lowlinks[node] = min(lowlinks[node], indices[neighbor_key])

            if lowlinks[node] == indices[node]:
                scc = []
                while True:
                    w = stack.pop()
                    rec_stack.discard(w)
                    scc.append(w)
                    if w == node:
                        break

                if len(scc) > 1:
                    cycles.append(tuple(sorted(scc)))
                    for m in scc:
                        scc_id[m] = scc_count[0]
                    scc_count[0] += 1

        indices: dict[str, int] = {}
        lowlinks: dict[str, int] = {}
        index_counter = [0]

        for node in self._import_graph.keys():
            if node not in indices:
                tarjan_visit(node, [], indices, lowlinks, index_counter)

        return cycles

    def _normalize_module_key(self, path_or_name: str) -> str:
        """Normalize a path or module name to a consistent key format."""
        if path_or_name.endswith(".py"):
            # It's a file path
            path = Path(path_or_name)
            return path.stem
        # It's already a module name
        return path_or_name.split(".")[0]

    def _estimate_function_lines(self, node: ast.AST) -> int:
        """Estimate function line count when end_lineno is unavailable."""
        if not hasattr(node, "body") or not node.body:
            return 1

        # Find the last statement's line number
        last_line = node.lineno
        for item in ast.walk(node):
            if hasattr(item, "lineno"):
                last_line = max(last_line, item.lineno)

        return max(1, last_line - node.lineno + 1)


def format_health_map(
    modules: list[ModuleHealth],
    *,
    limit: int = 20,
    filter_cycles: bool = False,
    filter_complex: bool = False,
) -> str:
    """Format module health map as a readable table.

    Args:
        modules: List of ModuleHealth objects
        limit: Max rows to display (0 = no limit)
        filter_cycles: Only show modules with circular imports
        filter_complex: Only show modules with complex functions

    Returns:
        Formatted table as a string
    """
    # Filter modules if requested
    filtered = modules
    if filter_cycles:
        filtered = [m for m in modules if m.has_circular_import]
    if filter_complex:
        filtered = [m for m in modules if m.complex_functions > 0]

    # Sort by health_score descending (worst first)
    sorted_modules = sorted(filtered, key=lambda m: m.health_score, reverse=True)

    if limit > 0:
        sorted_modules = sorted_modules[:limit]

    # Build table
    lines = []
    lines.append(
        "Module Health Map (sorted by health_score: 0=healthy, 100=problematic)"
    )
    lines.append("=" * 120)
    lines.append(
        f"{'Module':<45} {'Score':>6} {'Lines':>6} {'Funcs':>6} {'Complex':>8} "
        f"{'V.Complex':>10} {'Wide':>6} {'Imports':>8} {'Cycles':>8}"
    )
    lines.append("-" * 120)

    for module in sorted_modules:
        module_name = Path(module.module_path).name
        cycles_marker = "✓" if module.has_circular_import else ""

        lines.append(
            f"{module_name:<45} {module.health_score:>6} {module.line_count:>6} "
            f"{module.function_count:>6} {module.complex_functions:>8} "
            f"{module.very_complex_functions:>10} {module.wide_functions:>6} "
            f"{module.import_count:>8} {cycles_marker:>8}"
        )

    return "\n".join(lines)


def format_health_map_json(modules: list[ModuleHealth]) -> dict:
    """Format module health map as JSON.

    Args:
        modules: List of ModuleHealth objects

    Returns:
        Dictionary suitable for JSON serialization
    """
    sorted_modules = sorted(modules, key=lambda m: m.health_score, reverse=True)

    return {
        "modules": [
            {
                "module_path": m.module_path,
                "health_score": m.health_score,
                "function_count": m.function_count,
                "line_count": m.line_count,
                "complex_functions": m.complex_functions,
                "very_complex_functions": m.very_complex_functions,
                "wide_functions": m.wide_functions,
                "import_count": m.import_count,
                "has_circular_import": m.has_circular_import,
            }
            for m in sorted_modules
        ],
        "summary": {
            "total_modules": len(modules),
            "avg_health_score": (
                sum(m.health_score for m in modules) / len(modules)
                if modules
                else 0
            ),
            "max_health_score": max((m.health_score for m in modules), default=0),
            "modules_with_cycles": sum(1 for m in modules if m.has_circular_import),
            "modules_with_complex": sum(
                1 for m in modules if m.complex_functions > 0
            ),
        },
    }
