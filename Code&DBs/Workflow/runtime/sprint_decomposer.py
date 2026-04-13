"""Sprint decomposition engine.

Breaks objectives into micro-sprints with complexity classification,
dependency detection, and critical-path analysis.
"""
from __future__ import annotations

import enum
import os
import re
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional


class ComplexityClass(enum.Enum):
    """Classifies the side-effect profile of a work unit."""
    PURE = "pure"               # no side effects, single file
    IO = "io"                   # file / network operations
    INTEGRATION = "integration" # multi-file, cross-module
    SYSTEM = "system"           # infrastructure changes


_ESTIMATE_MINUTES: dict[ComplexityClass, int] = {
    ComplexityClass.PURE: 10,
    ComplexityClass.IO: 20,
    ComplexityClass.INTEGRATION: 30,
    ComplexityClass.SYSTEM: 15,
}


@dataclass(frozen=True)
class MicroSprint:
    sprint_id: str
    label: str
    description: str
    complexity: ComplexityClass
    estimated_minutes: int
    file_targets: tuple[str, ...]
    depends_on: tuple[str, ...]
    verify_command: Optional[str]


class SprintDecomposer:
    """Decomposes an objective into ordered micro-sprints."""

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def decompose(
        self,
        objective: str,
        write_scope: list[str],
        complexity_hint: Optional[str] = None,
    ) -> list[MicroSprint]:
        """Return one micro-sprint per file in *write_scope*."""
        sprints: list[MicroSprint] = []
        id_by_file: dict[str, str] = {}

        # First pass: create sprints with IDs
        for path in write_scope:
            sid = uuid.uuid4().hex[:12]
            id_by_file[path] = sid

        # Detect cross-file import relationships
        import_graph = self._build_import_graph(write_scope)

        # Second pass: build MicroSprint objects
        for path in write_scope:
            sid = id_by_file[path]
            complexity = self._classify(path, write_scope, import_graph, complexity_hint)
            minutes = _ESTIMATE_MINUTES[complexity]
            deps = self._detect_deps(path, write_scope, import_graph, id_by_file)
            verify = self._verify_command(path)
            label = os.path.basename(path)
            desc = f"{objective} -- {label}"

            sprints.append(MicroSprint(
                sprint_id=sid,
                label=label,
                description=desc,
                complexity=complexity,
                estimated_minutes=minutes,
                file_targets=(path,),
                depends_on=tuple(deps),
                verify_command=verify,
            ))
        return sprints

    def group_by_complexity(
        self, sprints: list[MicroSprint]
    ) -> dict[str, list[MicroSprint]]:
        groups: dict[str, list[MicroSprint]] = defaultdict(list)
        for s in sprints:
            groups[s.complexity.value].append(s)
        return dict(groups)

    def critical_path(self, sprints: list[MicroSprint]) -> list[MicroSprint]:
        """Topological sort by depends_on, return the longest chain."""
        by_id: dict[str, MicroSprint] = {s.sprint_id: s for s in sprints}

        # Kahn's algorithm for topo sort
        in_degree: dict[str, int] = {s.sprint_id: 0 for s in sprints}
        children: dict[str, list[str]] = defaultdict(list)
        for s in sprints:
            for dep in s.depends_on:
                if dep in by_id:
                    children[dep].append(s.sprint_id)
                    in_degree[s.sprint_id] += 1

        # longest-path via DP on topo order
        topo: list[str] = []
        queue: deque[str] = deque(
            sid for sid, deg in in_degree.items() if deg == 0
        )
        while queue:
            node = queue.popleft()
            topo.append(node)
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        dist: dict[str, int] = {sid: by_id[sid].estimated_minutes for sid in topo}
        pred: dict[str, Optional[str]] = {sid: None for sid in topo}

        for sid in topo:
            for child in children[sid]:
                new_dist = dist[sid] + by_id[child].estimated_minutes
                if new_dist > dist[child]:
                    dist[child] = new_dist
                    pred[child] = sid

        if not topo:
            return []

        # Trace back longest chain
        end = max(topo, key=lambda sid: dist[sid])
        chain: list[str] = []
        cur: Optional[str] = end
        while cur is not None:
            chain.append(cur)
            cur = pred[cur]
        chain.reverse()
        return [by_id[sid] for sid in chain]

    def total_estimate(self, sprints: list[MicroSprint]) -> int:
        """Sum of estimated_minutes accounting for parallelism.

        Independent sprints (no mutual dependency) run in parallel,
        so we compute the critical-path length.
        """
        if not sprints:
            return 0
        cp = self.critical_path(sprints)
        return sum(s.estimated_minutes for s in cp)

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_import_graph(write_scope: list[str]) -> dict[str, set[str]]:
        """Map each file to the set of other scope files it imports from."""
        # Build module-name -> file mapping
        mod_to_file: dict[str, str] = {}
        for path in write_scope:
            base = os.path.splitext(os.path.basename(path))[0]
            mod_to_file[base] = path

        graph: dict[str, set[str]] = {p: set() for p in write_scope}
        for path in write_scope:
            if not path.endswith(".py"):
                continue
            try:
                with open(path) as fh:
                    content = fh.read()
            except (OSError, IOError):
                # File might not exist yet during planning
                content = ""
            for m, f in mod_to_file.items():
                if f == path:
                    continue
                if re.search(rf"\b(?:import|from)\s+\S*{re.escape(m)}\b", content):
                    graph[path].add(f)
        return graph

    @staticmethod
    def _classify(
        path: str,
        write_scope: list[str],
        import_graph: dict[str, set[str]],
        hint: Optional[str],
    ) -> ComplexityClass:
        ext = os.path.splitext(path)[1].lower()
        base = os.path.basename(path).lower()

        if hint:
            hint_lower = hint.lower()
            for cc in ComplexityClass:
                if cc.value == hint_lower:
                    return cc

        # Config files -> SYSTEM
        if ext in (".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".env"):
            return ComplexityClass.SYSTEM

        # SQL -> IO
        if ext == ".sql":
            return ComplexityClass.IO

        # Python test files -> PURE
        if ext == ".py" and "test" in base:
            return ComplexityClass.PURE

        # If this file imports from other scope files -> INTEGRATION
        if path in import_graph and import_graph[path]:
            return ComplexityClass.INTEGRATION

        # If other scope files import from this one -> INTEGRATION
        for other, deps in import_graph.items():
            if path in deps:
                return ComplexityClass.INTEGRATION

        # Default for .py
        if ext == ".py":
            return ComplexityClass.PURE

        return ComplexityClass.PURE

    @staticmethod
    def _detect_deps(
        path: str,
        write_scope: list[str],
        import_graph: dict[str, set[str]],
        id_by_file: dict[str, str],
    ) -> list[str]:
        deps: list[str] = []
        imported_files = import_graph.get(path, set())
        for dep_file in imported_files:
            if dep_file in id_by_file:
                deps.append(id_by_file[dep_file])
        return deps

    @staticmethod
    def _verify_command(path: str) -> Optional[str]:
        ext = os.path.splitext(path)[1].lower()
        if ext == ".py":
            return f"python3 -m pytest {path}"
        if ext == ".sql":
            return f"python3 -c \"open('{path}').read()\""
        if ext == ".json":
            return f"python3 -m json.tool {path}"
        return None
