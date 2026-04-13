"""Tests for sprint_decomposer and sandbox_artifacts modules (~22 tests)."""
from __future__ import annotations

import importlib
import os
import sys
import tempfile

import pytest

# Direct file imports to avoid runtime/__init__.py (which requires Python 3.10+)
_runtime_dir = os.path.join(os.path.dirname(__file__), "..", "..", "runtime")

_spec_sd = importlib.util.spec_from_file_location(
    "sprint_decomposer", os.path.join(_runtime_dir, "sprint_decomposer.py")
)
_mod_sd = importlib.util.module_from_spec(_spec_sd)
sys.modules["sprint_decomposer"] = _mod_sd
_spec_sd.loader.exec_module(_mod_sd)
ComplexityClass = _mod_sd.ComplexityClass
MicroSprint = _mod_sd.MicroSprint
SprintDecomposer = _mod_sd.SprintDecomposer

_spec_sa = importlib.util.spec_from_file_location(
    "sandbox_artifacts", os.path.join(_runtime_dir, "sandbox_artifacts.py")
)
_mod_sa = importlib.util.module_from_spec(_spec_sa)
sys.modules["sandbox_artifacts"] = _mod_sa
_spec_sa.loader.exec_module(_mod_sa)
ArtifactRecord = _mod_sa.ArtifactRecord
ArtifactStore = _mod_sa.ArtifactStore


# ======================================================================
# Sprint Decomposer tests
# ======================================================================


class TestComplexityClass:
    def test_enum_values(self):
        assert ComplexityClass.PURE.value == "pure"
        assert ComplexityClass.IO.value == "io"
        assert ComplexityClass.INTEGRATION.value == "integration"
        assert ComplexityClass.SYSTEM.value == "system"

    def test_all_members(self):
        assert len(ComplexityClass) == 4


class TestMicroSprintDataclass:
    def test_frozen(self):
        s = MicroSprint(
            sprint_id="abc",
            label="x.py",
            description="d",
            complexity=ComplexityClass.PURE,
            estimated_minutes=10,
            file_targets=("x.py",),
            depends_on=(),
            verify_command=None,
        )
        with pytest.raises(AttributeError):
            s.label = "changed"  # type: ignore[misc]


class TestDecompose:
    def test_one_sprint_per_file(self):
        d = SprintDecomposer()
        sprints = d.decompose("build feature", ["a.py", "b.py", "c.py"])
        assert len(sprints) == 3

    def test_python_test_classified_pure(self):
        d = SprintDecomposer()
        sprints = d.decompose("test", ["test_foo.py"])
        assert sprints[0].complexity == ComplexityClass.PURE
        assert sprints[0].estimated_minutes == 10

    def test_sql_classified_io(self):
        d = SprintDecomposer()
        sprints = d.decompose("migrate", ["schema.sql"])
        assert sprints[0].complexity == ComplexityClass.IO
        assert sprints[0].estimated_minutes == 20

    def test_json_classified_system(self):
        d = SprintDecomposer()
        sprints = d.decompose("config", ["settings.json"])
        assert sprints[0].complexity == ComplexityClass.SYSTEM
        assert sprints[0].estimated_minutes == 15

    def test_verify_command_python(self):
        d = SprintDecomposer()
        sprints = d.decompose("x", ["foo.py"])
        assert sprints[0].verify_command is not None
        assert "pytest" in sprints[0].verify_command

    def test_verify_command_sql(self):
        d = SprintDecomposer()
        sprints = d.decompose("x", ["init.sql"])
        assert sprints[0].verify_command is not None

    def test_verify_command_json(self):
        d = SprintDecomposer()
        sprints = d.decompose("x", ["cfg.json"])
        assert "json.tool" in sprints[0].verify_command

    def test_unique_sprint_ids(self):
        d = SprintDecomposer()
        sprints = d.decompose("build", ["a.py", "b.py", "c.py", "d.py"])
        ids = [s.sprint_id for s in sprints]
        assert len(set(ids)) == len(ids)


class TestGroupByComplexity:
    def test_groups(self):
        d = SprintDecomposer()
        sprints = d.decompose("x", ["test_a.py", "schema.sql", "config.json"])
        groups = d.group_by_complexity(sprints)
        assert "pure" in groups
        assert "io" in groups
        assert "system" in groups


class TestCriticalPath:
    def test_no_deps_returns_longest_single(self):
        d = SprintDecomposer()
        sprints = d.decompose("x", ["a.py"])
        cp = d.critical_path(sprints)
        assert len(cp) == 1

    def test_empty_input(self):
        d = SprintDecomposer()
        assert d.critical_path([]) == []


class TestTotalEstimate:
    def test_single_file(self):
        d = SprintDecomposer()
        sprints = d.decompose("x", ["test_a.py"])
        assert d.total_estimate(sprints) == 10

    def test_empty(self):
        d = SprintDecomposer()
        assert d.total_estimate([]) == 0


# ======================================================================
# Sandbox Artifacts tests
# ======================================================================


@pytest.fixture
def store():
    from _pg_test_conn import get_test_conn
    import uuid
    conn = get_test_conn()
    s = ArtifactStore(conn)
    s._test_prefix = uuid.uuid4().hex[:8]
    return s


class TestArtifactCapture:
    def test_capture_returns_record(self, store):
        pfx = store._test_prefix
        rec = store.capture(f"{pfx}/foo.py", "print('hi')\n", f"{pfx}-sandbox-1")
        assert isinstance(rec, ArtifactRecord)
        assert rec.file_path == f"{pfx}/foo.py"
        assert rec.sandbox_id == f"{pfx}-sandbox-1"
        assert rec.byte_count == len("print('hi')\n".encode())
        assert rec.line_count == 1

    def test_sha256_deterministic(self, store):
        pfx = store._test_prefix
        r1 = store.capture(f"{pfx}/a.py", "content", f"{pfx}-s1")
        r2 = store.capture(f"{pfx}/b.py", "content", f"{pfx}-s2")
        assert r1.sha256 == r2.sha256

    def test_sha256_differs_for_different_content(self, store):
        pfx = store._test_prefix
        r1 = store.capture(f"{pfx}/a.py", "aaa", f"{pfx}-s1")
        r2 = store.capture(f"{pfx}/b.py", "bbb", f"{pfx}-s1")
        assert r1.sha256 != r2.sha256


class TestArtifactGet:
    def test_get_existing(self, store):
        pfx = store._test_prefix
        rec = store.capture(f"{pfx}/x.py", "data", f"{pfx}-s1")
        fetched = store.get(rec.artifact_id)
        assert fetched is not None
        assert fetched.artifact_id == rec.artifact_id

    def test_get_missing(self, store):
        assert store.get("nonexistent") is None


class TestArtifactListBySandbox:
    def test_filters_by_sandbox(self, store):
        pfx = store._test_prefix
        store.capture(f"{pfx}/a.py", "1", f"{pfx}-s1")
        store.capture(f"{pfx}/b.py", "2", f"{pfx}-s1")
        store.capture(f"{pfx}/c.py", "3", f"{pfx}-s2")
        results = store.list_by_sandbox(f"{pfx}-s1")
        assert len(results) == 2
        assert all(r.sandbox_id == f"{pfx}-s1" for r in results)

    def test_latest_sandbox_id_returns_most_recent_sandbox(self, store):
        pfx = store._test_prefix
        store.capture(f"{pfx}/a.py", "1", f"{pfx}-s1")
        store.capture(f"{pfx}/b.py", "2", f"{pfx}-s2")

        assert store.latest_sandbox_id() == f"{pfx}-s2"


class TestArtifactSearch:
    def test_search_by_path(self, store):
        pfx = store._test_prefix
        store.capture(f"{pfx}/src/foo.py", "x", f"{pfx}-s1")
        store.capture(f"{pfx}/src/bar.py", "y", f"{pfx}-s1")
        store.capture(f"{pfx}/tests/baz.py", "z", f"{pfx}-s1")
        results = store.search(f"{pfx}/src/")
        assert len(results) == 2


class TestArtifactDiff:
    def test_same_content(self, store):
        pfx = store._test_prefix
        a = store.capture(f"{pfx}/a.py", "same", f"{pfx}-s1")
        b = store.capture(f"{pfx}/b.py", "same", f"{pfx}-s1")
        d = store.diff(a.artifact_id, b.artifact_id)
        assert d["same_hash"] is True
        assert d["size_delta"] == 0

    def test_different_content(self, store):
        pfx = store._test_prefix
        a = store.capture(f"{pfx}/a.py", "short", f"{pfx}-s1")
        b = store.capture(f"{pfx}/b.py", "much longer content here", f"{pfx}-s1")
        d = store.diff(a.artifact_id, b.artifact_id)
        assert d["same_hash"] is False
        assert d["size_delta"] > 0

    def test_missing_artifact(self, store):
        pfx = store._test_prefix
        a = store.capture(f"{pfx}/a.py", "x", f"{pfx}-s1")
        d = store.diff(a.artifact_id, "nope")
        assert "error" in d


class TestArtifactStats:
    def test_stats_empty(self, store):
        # Stats counts ALL artifacts in DB, use relative count
        s_before = store.stats()
        # Just verify the shape
        assert "total_artifacts" in s_before
        assert "total_bytes" in s_before
        assert "unique_sandboxes" in s_before

    def test_stats_after_inserts(self, store):
        pfx = store._test_prefix
        s_before = store.stats()
        store.capture(f"{pfx}/a.py", "hello", f"{pfx}-s1")
        store.capture(f"{pfx}/b.py", "world", f"{pfx}-s2")
        s_after = store.stats()
        assert s_after["total_artifacts"] - s_before["total_artifacts"] == 2
        assert s_after["unique_sandboxes"] - s_before["unique_sandboxes"] == 2
        assert s_after["total_bytes"] > s_before["total_bytes"]
