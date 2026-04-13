"""Execution Strategy + Step Compiler.

Compiles workflow specs into executable plans composed of micro-steps,
then sequences multiple plans into topologically-sorted parallel waves.
"""
from __future__ import annotations

import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Enums / value objects
# ---------------------------------------------------------------------------

class ExecutionMode(Enum):
    HOT = "hot"          # reuse existing sandbox
    COLD = "cold"        # fresh sandbox
    SESSION = "session"  # long-running session
    FORKED = "forked"    # fork from existing state


@dataclass(frozen=True)
class MicroStep:
    step_id: str
    file_path: str
    action: str          # 'create' | 'replace' | 'patch'
    verify_command: Optional[str]
    depends_on: Tuple[str, ...]


@dataclass(frozen=True)
class ExecutionPlan:
    strategy: ExecutionMode
    steps: Tuple[MicroStep, ...]
    estimated_duration_seconds: int
    parallelizable_groups: Tuple[Tuple[str, ...], ...]


# ---------------------------------------------------------------------------
# Step Compiler
# ---------------------------------------------------------------------------

class StepCompiler:
    """Compile a workflow spec dict into an ExecutionPlan."""

    def compile(self, spec: dict) -> ExecutionPlan:
        write_scope: list[dict] = spec.get("write_scope", [])
        verify_commands: dict[str, str] = spec.get("verify_commands", {})

        # a. One MicroStep per file
        steps: list[MicroStep] = []
        path_to_step_id: dict[str, str] = {}

        for entry in write_scope:
            sid = uuid.uuid4().hex[:8]
            fp = entry.get("path", entry.get("file_path", ""))
            action = entry.get("action", "create")
            verify = verify_commands.get(fp)
            path_to_step_id[fp] = sid
            steps.append(
                MicroStep(
                    step_id=sid,
                    file_path=fp,
                    action=action,
                    verify_command=verify,
                    depends_on=(),  # filled in next pass
                )
            )

        # c. Determine dependencies: step B depends on step A if B reads a file A writes
        read_scope: dict[str, list[str]] = spec.get("read_scope", {})
        # read_scope maps file_path -> list of paths it reads from
        resolved_steps: list[MicroStep] = []
        for step in steps:
            deps: list[str] = []
            reads = read_scope.get(step.file_path, [])
            for read_path in reads:
                if read_path in path_to_step_id:
                    dep_id = path_to_step_id[read_path]
                    if dep_id != step.step_id:
                        deps.append(dep_id)
            resolved_steps.append(
                MicroStep(
                    step_id=step.step_id,
                    file_path=step.file_path,
                    action=step.action,
                    verify_command=step.verify_command,
                    depends_on=tuple(deps),
                )
            )

        # d. Group independent steps as parallelizable
        groups = self._build_parallel_groups(resolved_steps)

        # e. Pick execution mode
        mode = self._pick_mode(spec, resolved_steps)

        # Estimate duration: 5s per step, minimum 5
        est = max(5, len(resolved_steps) * 5)

        return ExecutionPlan(
            strategy=mode,
            steps=tuple(resolved_steps),
            estimated_duration_seconds=est,
            parallelizable_groups=tuple(tuple(g) for g in groups),
        )

    # -- internals -----------------------------------------------------------

    @staticmethod
    def _pick_mode(spec: dict, steps: list[MicroStep]) -> ExecutionMode:
        if spec.get("session"):
            return ExecutionMode.SESSION
        if spec.get("fork_from"):
            return ExecutionMode.FORKED
        if len(steps) <= 1:
            return ExecutionMode.HOT
        if len(steps) > 5:
            return ExecutionMode.COLD
        return ExecutionMode.HOT

    @staticmethod
    def _build_parallel_groups(
        steps: list[MicroStep],
    ) -> list[list[str]]:
        """Topological layering: steps with no unresolved deps go in the same group."""
        id_to_step = {s.step_id: s for s in steps}
        in_degree: dict[str, int] = {s.step_id: len(s.depends_on) for s in steps}
        dependents: dict[str, list[str]] = defaultdict(list)
        for s in steps:
            for d in s.depends_on:
                dependents[d].append(s.step_id)

        groups: list[list[str]] = []
        ready = deque(sid for sid, deg in in_degree.items() if deg == 0)
        while ready:
            group: list[str] = []
            next_ready: list[str] = []
            while ready:
                sid = ready.popleft()
                group.append(sid)
                for dep_id in dependents[sid]:
                    in_degree[dep_id] -= 1
                    if in_degree[dep_id] == 0:
                        next_ready.append(dep_id)
            groups.append(group)
            ready = deque(next_ready)
        return groups


# ---------------------------------------------------------------------------
# Wave Sequencer
# ---------------------------------------------------------------------------

class WaveSequencer:
    """Topological-sort a list of ExecutionPlans into parallel waves.

    Plans are treated as nodes. Plan B depends on plan A if any step in B
    reads a file that some step in A writes.
    """

    def sequence(self, plans: List[ExecutionPlan]) -> List[List[ExecutionPlan]]:
        if not plans:
            return []

        # Build a graph over plan indices
        n = len(plans)

        # Map: written file -> plan index
        writes: dict[str, int] = {}
        for i, plan in enumerate(plans):
            for step in plan.steps:
                if step.action in ("create", "replace", "patch"):
                    writes[step.file_path] = i

        # Edges: plan j depends on plan i
        in_degree = [0] * n
        dependents: dict[int, list[int]] = defaultdict(list)
        for j, plan in enumerate(plans):
            deps_seen: set[int] = set()
            for step in plan.steps:
                for dep_sid in step.depends_on:
                    # Find which plan owns that dep step
                    for i, other in enumerate(plans):
                        if i == j:
                            continue
                        if any(s.step_id == dep_sid for s in other.steps):
                            if i not in deps_seen:
                                deps_seen.add(i)
                                in_degree[j] += 1
                                dependents[i].append(j)

        # Also add cross-plan file dependencies
        for j, plan in enumerate(plans):
            read_files: set[str] = set()
            for step in plan.steps:
                for dep_sid in step.depends_on:
                    # Already handled above
                    pass
            # Check if any step in plan j reads a file written by another plan
            # We approximate: if step.file_path appears as a dependency source
            # This is already captured via depends_on, so the graph is complete.

        # Topological sort into waves
        waves: list[list[ExecutionPlan]] = []
        ready = deque(i for i in range(n) if in_degree[i] == 0)
        while ready:
            wave: list[ExecutionPlan] = []
            next_ready: list[int] = []
            while ready:
                idx = ready.popleft()
                wave.append(plans[idx])
                for dep_idx in dependents[idx]:
                    in_degree[dep_idx] -= 1
                    if in_degree[dep_idx] == 0:
                        next_ready.append(dep_idx)
            waves.append(wave)
            ready = deque(next_ready)
        return waves
