"""Conflict resolver for concurrent job write scopes.

Detects write conflicts between jobs and produces serialization groups
so that conflicting jobs run sequentially instead of in parallel.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum


class ConflictType(Enum):
    """Classification of write conflicts between jobs."""

    PARALLEL_WRITERS = "parallel_writers"
    CHAINED_DEPENDENCY = "chained_dependency"
    READ_WRITE_OVERLAP = "read_write_overlap"


@dataclass(frozen=True)
class JobWriteScope:
    """Declares which files a job reads and writes."""

    job_label: str
    write_paths: tuple[str, ...]
    read_paths: tuple[str, ...]


@dataclass(frozen=True)
class WriteConflict:
    """A detected conflict between two jobs on a specific file."""

    conflict_type: ConflictType
    file_path: str
    job_a: str
    job_b: str
    risk_level: str  # 'high' | 'medium' | 'low'


@dataclass(frozen=True)
class SerializationGroup:
    """A set of jobs that must run sequentially."""

    group_id: str
    job_labels: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class ConflictAnalysis:
    """Full result of conflict analysis across all pending jobs."""

    conflicts: tuple[WriteConflict, ...]
    serialization_groups: tuple[SerializationGroup, ...]
    parallel_safe_jobs: tuple[str, ...]


class _UnionFind:
    """Union-find (disjoint set) for transitive group merging."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]
            x = self._parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._rank[ra] < self._rank[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        if self._rank[ra] == self._rank[rb]:
            self._rank[ra] += 1

    def groups(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = defaultdict(list)
        for item in self._parent:
            result[self.find(item)].append(item)
        return dict(result)


class ConflictResolver:
    """Analyzes job write scopes and resolves conflicts."""

    def analyze(self, jobs: list[JobWriteScope]) -> ConflictAnalysis:
        """Identify all conflicts and produce serialization groups."""
        conflicts = self._detect_conflicts(jobs)
        groups = self._build_serialization_groups(jobs, conflicts)
        conflicting_labels = set()
        for g in groups:
            conflicting_labels.update(g.job_labels)
        all_labels = {j.job_label for j in jobs}
        parallel_safe = sorted(all_labels - conflicting_labels)
        return ConflictAnalysis(
            conflicts=tuple(conflicts),
            serialization_groups=tuple(groups),
            parallel_safe_jobs=tuple(parallel_safe),
        )

    def serialize(self, jobs: list[JobWriteScope]) -> list[SerializationGroup]:
        """Return serialization groups for jobs that cannot run in parallel."""
        analysis = self.analyze(jobs)
        return list(analysis.serialization_groups)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _detect_conflicts(self, jobs: list[JobWriteScope]) -> list[WriteConflict]:
        file_writers: dict[str, list[str]] = defaultdict(list)
        file_readers: dict[str, list[str]] = defaultdict(list)

        for job in jobs:
            for wp in job.write_paths:
                file_writers[wp].append(job.job_label)
            for rp in job.read_paths:
                file_readers[rp].append(job.job_label)

        conflicts: list[WriteConflict] = []
        seen: set[tuple[str, str, str]] = set()

        # Parallel writers: 2+ jobs write the same file
        for path, writers in file_writers.items():
            if len(writers) < 2:
                continue
            for i, a in enumerate(writers):
                for b in writers[i + 1 :]:
                    key = (path, min(a, b), max(a, b))
                    if key not in seen:
                        seen.add(key)
                        conflicts.append(
                            WriteConflict(
                                conflict_type=ConflictType.PARALLEL_WRITERS,
                                file_path=path,
                                job_a=a,
                                job_b=b,
                                risk_level="high",
                            )
                        )

        # Read-write overlap: one reads what another writes
        for path, writers in file_writers.items():
            readers = file_readers.get(path, [])
            for w in writers:
                for r in readers:
                    if w == r:
                        continue
                    key = (path, min(w, r), max(w, r))
                    if key not in seen:
                        seen.add(key)
                        conflicts.append(
                            WriteConflict(
                                conflict_type=ConflictType.READ_WRITE_OVERLAP,
                                file_path=path,
                                job_a=w,
                                job_b=r,
                                risk_level="medium",
                            )
                        )

        return conflicts

    def _build_serialization_groups(
        self,
        jobs: list[JobWriteScope],
        conflicts: list[WriteConflict],
    ) -> list[SerializationGroup]:
        if not conflicts:
            return []

        uf = _UnionFind()
        # Ensure every conflicting label is registered
        for c in conflicts:
            uf.find(c.job_a)
            uf.find(c.job_b)
            uf.union(c.job_a, c.job_b)

        raw_groups = uf.groups()
        result: list[SerializationGroup] = []
        for _root, members in sorted(raw_groups.items()):
            if len(members) < 2:
                continue
            members_sorted = tuple(sorted(members))
            result.append(
                SerializationGroup(
                    group_id=uuid.uuid5(
                        uuid.NAMESPACE_DNS, ":".join(members_sorted)
                    ).hex[:12],
                    job_labels=members_sorted,
                    reason="write-scope conflict requires sequential execution",
                )
            )
        return result
