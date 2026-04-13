"""Tests for runtime.conflict_resolver."""

import pathlib
import sys
import importlib.util

_WORKFLOW_ROOT = str(pathlib.Path(__file__).resolve().parents[2])

# Import the module directly to avoid runtime/__init__.py pulling in
# domain.py which requires Python 3.10+ (slots=True on dataclass).
_spec = importlib.util.spec_from_file_location(
    "conflict_resolver",
    f"{_WORKFLOW_ROOT}/runtime/conflict_resolver.py",
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["conflict_resolver"] = _mod
_spec.loader.exec_module(_mod)

ConflictAnalysis = _mod.ConflictAnalysis
ConflictResolver = _mod.ConflictResolver
ConflictType = _mod.ConflictType
JobWriteScope = _mod.JobWriteScope
SerializationGroup = _mod.SerializationGroup
WriteConflict = _mod.WriteConflict


def _labels(groups):
    """Extract sorted job_labels from serialization groups for easy assertion."""
    return [g.job_labels for g in groups]


class TestNoConflicts:
    def test_disjoint_write_scopes(self):
        jobs = [
            JobWriteScope("build", write_paths=("out/build.log",), read_paths=()),
            JobWriteScope("test", write_paths=("out/test.log",), read_paths=()),
            JobWriteScope("lint", write_paths=("out/lint.log",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        assert len(result.conflicts) == 0
        assert len(result.serialization_groups) == 0
        assert set(result.parallel_safe_jobs) == {"build", "test", "lint"}


class TestParallelWriters:
    def test_two_jobs_write_same_file(self):
        jobs = [
            JobWriteScope("deploy-a", write_paths=("shared/config.json",), read_paths=()),
            JobWriteScope("deploy-b", write_paths=("shared/config.json",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        assert len(result.conflicts) == 1
        c = result.conflicts[0]
        assert c.conflict_type == ConflictType.PARALLEL_WRITERS
        assert c.file_path == "shared/config.json"
        assert c.risk_level == "high"
        assert {c.job_a, c.job_b} == {"deploy-a", "deploy-b"}

    def test_parallel_writers_creates_serialization_group(self):
        jobs = [
            JobWriteScope("a", write_paths=("f.txt",), read_paths=()),
            JobWriteScope("b", write_paths=("f.txt",), read_paths=()),
        ]
        groups = ConflictResolver().serialize(jobs)
        assert len(groups) == 1
        assert set(groups[0].job_labels) == {"a", "b"}


class TestReadWriteOverlap:
    def test_one_reads_what_another_writes(self):
        jobs = [
            JobWriteScope("writer", write_paths=("data.db",), read_paths=()),
            JobWriteScope("reader", write_paths=(), read_paths=("data.db",)),
        ]
        result = ConflictResolver().analyze(jobs)
        rw_conflicts = [
            c for c in result.conflicts if c.conflict_type == ConflictType.READ_WRITE_OVERLAP
        ]
        assert len(rw_conflicts) == 1
        assert rw_conflicts[0].risk_level == "medium"
        assert rw_conflicts[0].file_path == "data.db"

    def test_read_write_overlap_not_detected_for_same_job(self):
        """A job reading its own writes is not a conflict."""
        jobs = [
            JobWriteScope("self-rw", write_paths=("f.txt",), read_paths=("f.txt",)),
        ]
        result = ConflictResolver().analyze(jobs)
        assert len(result.conflicts) == 0


class TestTransitiveGrouping:
    def test_a_b_and_b_c_merge_into_one_group(self):
        jobs = [
            JobWriteScope("a", write_paths=("shared1.txt",), read_paths=()),
            JobWriteScope("b", write_paths=("shared1.txt", "shared2.txt"), read_paths=()),
            JobWriteScope("c", write_paths=("shared2.txt",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        assert len(result.serialization_groups) == 1
        assert set(result.serialization_groups[0].job_labels) == {"a", "b", "c"}
        assert len(result.parallel_safe_jobs) == 0

    def test_two_separate_conflict_clusters(self):
        jobs = [
            JobWriteScope("a", write_paths=("x.txt",), read_paths=()),
            JobWriteScope("b", write_paths=("x.txt",), read_paths=()),
            JobWriteScope("c", write_paths=("y.txt",), read_paths=()),
            JobWriteScope("d", write_paths=("y.txt",), read_paths=()),
            JobWriteScope("e", write_paths=("z.txt",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        assert len(result.serialization_groups) == 2
        group_sets = [set(g.job_labels) for g in result.serialization_groups]
        assert {"a", "b"} in group_sets
        assert {"c", "d"} in group_sets
        assert result.parallel_safe_jobs == ("e",)


class TestParallelSafeJobs:
    def test_non_conflicting_jobs_identified(self):
        jobs = [
            JobWriteScope("conflict-a", write_paths=("shared.txt",), read_paths=()),
            JobWriteScope("conflict-b", write_paths=("shared.txt",), read_paths=()),
            JobWriteScope("safe-1", write_paths=("own1.txt",), read_paths=()),
            JobWriteScope("safe-2", write_paths=("own2.txt",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        assert set(result.parallel_safe_jobs) == {"safe-1", "safe-2"}


class TestComplexOverlapping:
    def test_many_jobs_complex_scopes(self):
        jobs = [
            JobWriteScope("j1", write_paths=("a.txt", "b.txt"), read_paths=("c.txt",)),
            JobWriteScope("j2", write_paths=("b.txt", "c.txt"), read_paths=("d.txt",)),
            JobWriteScope("j3", write_paths=("d.txt",), read_paths=("e.txt",)),
            JobWriteScope("j4", write_paths=("e.txt",), read_paths=()),
            JobWriteScope("j5", write_paths=("solo.txt",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        # j1-j2 share write on b.txt (PARALLEL_WRITERS)
        # j2 writes c.txt which j1 reads (READ_WRITE_OVERLAP)
        # j3 writes d.txt which j2 reads (READ_WRITE_OVERLAP) -> j2-j3 linked
        # j4 writes e.txt which j3 reads (READ_WRITE_OVERLAP) -> j3-j4 linked
        # Transitively: j1, j2, j3, j4 all in one group
        assert len(result.serialization_groups) == 1
        assert set(result.serialization_groups[0].job_labels) == {"j1", "j2", "j3", "j4"}
        assert result.parallel_safe_jobs == ("j5",)

        # Check conflict types present
        types = {c.conflict_type for c in result.conflicts}
        assert ConflictType.PARALLEL_WRITERS in types
        assert ConflictType.READ_WRITE_OVERLAP in types


class TestSingleJob:
    def test_single_job_no_conflicts(self):
        jobs = [
            JobWriteScope("only", write_paths=("out.txt",), read_paths=("in.txt",)),
        ]
        result = ConflictResolver().analyze(jobs)
        assert len(result.conflicts) == 0
        assert len(result.serialization_groups) == 0
        assert result.parallel_safe_jobs == ("only",)

    def test_empty_jobs_list(self):
        result = ConflictResolver().analyze([])
        assert len(result.conflicts) == 0
        assert len(result.serialization_groups) == 0
        assert len(result.parallel_safe_jobs) == 0


class TestFrozenDataclasses:
    def test_all_results_are_frozen(self):
        jobs = [
            JobWriteScope("a", write_paths=("f.txt",), read_paths=()),
            JobWriteScope("b", write_paths=("f.txt",), read_paths=()),
        ]
        result = ConflictResolver().analyze(jobs)
        # ConflictAnalysis is frozen
        try:
            result.conflicts = ()
            assert False, "Should have raised"
        except AttributeError:
            pass
        # WriteConflict is frozen
        try:
            result.conflicts[0].risk_level = "low"
            assert False, "Should have raised"
        except AttributeError:
            pass
