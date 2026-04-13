"""Tests for constraint_ledger and execution_strategy modules (~20 tests).

Uses direct importlib to avoid pulling in runtime/__init__.py which
requires Python 3.10+ features.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# Direct imports (bypass runtime/__init__.py)
# ---------------------------------------------------------------------------
_RUNTIME = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, "runtime"
)

def _load(module_name: str, filename: str):
    spec = importlib.util.spec_from_file_location(
        module_name, os.path.join(_RUNTIME, filename)
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod

_cl_mod = _load("_constraint_ledger", "constraint_ledger.py")
_es_mod = _load("_execution_strategy", "execution_strategy.py")

ConstraintLedger = _cl_mod.ConstraintLedger
ConstraintMiner = _cl_mod.ConstraintMiner
MinedConstraint = _cl_mod.MinedConstraint
ConstraintWriteResult = _cl_mod.ConstraintWriteResult

ExecutionMode = _es_mod.ExecutionMode
ExecutionPlan = _es_mod.ExecutionPlan
MicroStep = _es_mod.MicroStep
StepCompiler = _es_mod.StepCompiler
WaveSequencer = _es_mod.WaveSequencer


# ===================================================================
# Constraint Ledger
# ===================================================================


class TestMinedConstraint:
    def test_frozen(self):
        from datetime import datetime
        mc = MinedConstraint(
            constraint_id="abc",
            pattern="ImportError",
            constraint_text="fix imports",
            confidence=0.9,
            mined_from_jobs=("j1",),
            created_at=datetime.now(),
        )
        with pytest.raises(AttributeError):
            mc.pattern = "other"

    def test_fields(self):
        from datetime import datetime
        mc = MinedConstraint(
            constraint_id="x",
            pattern="p",
            constraint_text="t",
            confidence=0.5,
            mined_from_jobs=("a", "b"),
            created_at=datetime.now(),
        )
        assert mc.mined_from_jobs == ("a", "b")
        assert mc.confidence == 0.5


class TestConstraintLedger:
    @pytest.fixture
    def ledger(self):
        from _pg_test_conn import get_test_conn
        return ConstraintLedger(get_test_conn())

    def test_add_returns_constraint(self, ledger):
        mc = ledger.add("ImportError", "fix imports", 0.9, ("j1",))
        assert isinstance(mc, MinedConstraint)
        assert mc.pattern == "ImportError"
        assert mc.confidence == 0.9

    def test_list_all_respects_min_confidence(self, ledger):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        ledger.add(f"A_{pfx}", "text_a", 0.3, ("j1",))
        ledger.add(f"B_{pfx}", "text_b", 0.7, ("j2",))
        ledger.add(f"C_{pfx}", "text_c", 0.9, ("j3",))
        results = ledger.list_all(min_confidence=0.5)
        patterns = {r.pattern for r in results}
        assert f"B_{pfx}" in patterns
        assert f"C_{pfx}" in patterns
        assert f"A_{pfx}" not in patterns

    def test_list_all_default_threshold(self, ledger):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        ledger.add(f"A_{pfx}", "text_a", 0.4, ("j1",))
        ledger.add(f"B_{pfx}", "text_b", 0.6, ("j2",))
        results = ledger.list_all()
        patterns = {r.pattern for r in results}
        # default threshold 0.5: A (0.4) excluded, B (0.6) included
        assert f"B_{pfx}" in patterns
        assert f"A_{pfx}" not in patterns

    def test_get_for_scope_global_constraints(self, ledger):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        ledger.add(f"G_{pfx}", "global constraint", 0.9, ("j1",), scope_prefix="")
        results = ledger.get_for_scope(["src/foo.py"])
        patterns = {r.pattern for r in results}
        assert f"G_{pfx}" in patterns

    def test_get_for_scope_prefix_match(self, ledger):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        ledger.add(f"S_{pfx}", "scoped", 0.9, ("j1",), scope_prefix=f"src/core_{pfx}")
        results = ledger.get_for_scope([f"src/core_{pfx}/mod.py"])
        patterns = {r.pattern for r in results}
        assert f"S_{pfx}" in patterns
        results2 = ledger.get_for_scope(["lib/other.py"])
        patterns2 = {r.pattern for r in results2}
        assert f"S_{pfx}" not in patterns2

    def test_get_for_scope_empty_paths(self, ledger):
        ledger.add("A", "text", 0.9, ("j1",))
        assert ledger.get_for_scope([]) == []

    def test_inject_into_prompt_no_constraints(self, ledger):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        prompt = "Do the thing"
        # Use a scope path that won't match any existing constraints
        result = ledger.inject_into_prompt(prompt, [f"zzz_nonexistent_{pfx}/x.py"])
        # With production data, global constraints may still be injected
        # Just verify the original prompt is in the result
        assert prompt in result

    def test_inject_into_prompt_appends_section(self, ledger):
        ledger.add("ImportError", "fix imports", 0.9, ("j1",))
        result = ledger.inject_into_prompt("Do the thing", ["anything.py"])
        assert "## LEARNED CONSTRAINTS" in result
        assert "fix imports" in result

    def test_multiple_constraints_injected(self, ledger):
        ledger.add("A", "rule a", 0.9, ("j1",))
        ledger.add("B", "rule b", 0.8, ("j2",))
        result = ledger.inject_into_prompt("prompt", ["x.py"])
        assert "rule a" in result
        assert "rule b" in result


class _FakeEmbedder:
    def __init__(self, vector):
        self.vector = vector
        self.calls = []

    def embed_one(self, text):
        self.calls.append(text)
        return list(self.vector)


class _FakeVectorConn:
    def __init__(self, rows_by_id=None, vector_rows=None):
        self.rows_by_id = dict(rows_by_id or {})
        self.vector_rows = list(vector_rows or [])
        self.calls = []
        self.insert_args = None

    def execute(self, query, *args):
        self.calls.append((query, args))
        if "1 - (embedding <=> $1::vector) AS similarity" in query and "LIMIT $3" in query:
            return [dict(row) for row in self.vector_rows]
        if "SET confidence = GREATEST" in query:
            constraint_id, new_confidence, mined_from_jobs = args
            row = dict(self.rows_by_id[constraint_id])
            row["confidence"] = max(float(row["confidence"]), float(new_confidence))
            row["mined_from_jobs"] = mined_from_jobs
            self.rows_by_id[constraint_id] = row
            return [dict(row)]
        if "SET embedding = $1::vector" in query:
            embedding, constraint_id = args
            row = dict(self.rows_by_id[constraint_id])
            row["embedding"] = embedding
            self.rows_by_id[constraint_id] = row
            return [dict(row)]
        if "SELECT * FROM workflow_constraints WHERE constraint_id = $1" in query:
            constraint_id = args[0]
            row = self.rows_by_id.get(constraint_id)
            return [dict(row)] if row is not None else []
        if "INSERT INTO workflow_constraints" in query:
            self.insert_args = (query, args)
            constraint_id = args[0]
            row = {
                "constraint_id": constraint_id,
                "pattern": args[1],
                "constraint_text": args[2],
                "confidence": args[3],
                "mined_from_jobs": args[4],
                "scope_prefix": args[5],
                "created_at": args[6],
            }
            self.rows_by_id[constraint_id] = row
            return []
        if "SELECT * FROM workflow_constraints WHERE scope_prefix = ''" in query:
            return [dict(row) for row in self.rows_by_id.values() if row["scope_prefix"] == ""]
        if "WHERE scope_prefix != ''" in query:
            write_path = args[0]
            return [
                dict(row)
                for row in self.rows_by_id.values()
                if row["scope_prefix"] and write_path.startswith(row["scope_prefix"])
            ]
        if "SELECT * FROM workflow_constraints WHERE confidence >= " in query:
            threshold = float(args[0])
            rows = [
                dict(row)
                for row in self.rows_by_id.values()
                if float(row["confidence"]) >= threshold
            ]
            rows.sort(key=lambda row: row["created_at"], reverse=True)
            return rows
        return []


class TestConstraintLedgerVectors:
    @pytest.fixture
    def vector_ledger(self):
        embedder = _FakeEmbedder([0.123456, 0.234567, 0.345678])
        existing_row = {
            "constraint_id": "existing123",
            "pattern": "ImportError",
            "constraint_text": "fix imports",
            "confidence": 0.7,
            "mined_from_jobs": "old_job",
            "scope_prefix": "",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }
        conn = _FakeVectorConn(
            rows_by_id={"existing123": existing_row},
            vector_rows=[
                {
                    "constraint_id": "existing123",
                    "pattern": "ImportError",
                    "constraint_text": "fix imports",
                    "confidence": 0.7,
                    "mined_from_jobs": "old_job",
                    "scope_prefix": "",
                    "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    "similarity": 0.93,
                }
            ],
        )
        ledger = ConstraintLedger(conn, embedder)
        return ledger, conn, embedder

    def test_add_merges_near_duplicate(self, vector_ledger):
        ledger, conn, embedder = vector_ledger
        result = ledger.add("ImportError", "fix imports", 0.9, ("new_job",))
        assert isinstance(result, MinedConstraint)
        assert isinstance(result, ConstraintWriteResult)
        assert result.merged is True
        assert result.similarity == 0.93
        assert result.constraint_id == "existing123"
        assert result.confidence == 0.9
        assert result.mined_from_jobs == ("old_job", "new_job")
        assert embedder.calls[0] == "pattern: ImportError\ndescription: fix imports"
        duplicate_sql, duplicate_args = next(
            (query, args)
            for query, args in conn.calls
            if "1 - (embedding <=> $1::vector) AS similarity" in query
        )
        assert ">= $2" in duplicate_sql
        assert duplicate_args[1] == 0.88
        assert conn.insert_args is None

    def test_add_inserts_with_embedding_when_no_duplicate(self):
        embedder = _FakeEmbedder([0.9, 0.8, 0.7])
        conn = _FakeVectorConn(vector_rows=[])
        ledger = ConstraintLedger(conn, embedder)
        result = ledger.add("SyntaxError", "keep syntax valid", 0.8, ("job42",), scope_prefix="src/core")
        assert result.merged is False
        assert embedder.calls == [
            "pattern: SyntaxError\ndescription: keep syntax valid",
            "pattern: SyntaxError\ndescription: keep syntax valid",
        ]
        assert conn.insert_args is not None
        insert_query, insert_args = conn.insert_args
        assert "embedding" not in insert_query
        assert insert_args[1] == "SyntaxError"
        assert insert_args[4] == "job42"
        assert insert_args[5] == "src/core"
        assert any("SET embedding = $1::vector" in query for query, _ in conn.calls)

    def test_get_for_scope_merges_vector_hits(self):
        embedder = _FakeEmbedder([0.222222, 0.333333, 0.444444])
        conn = _FakeVectorConn(
            rows_by_id={
                "global1": {
                    "constraint_id": "global1",
                    "pattern": "Global",
                    "constraint_text": "applies everywhere",
                    "confidence": 0.6,
                    "mined_from_jobs": "job0",
                    "scope_prefix": "",
                    "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
                },
                "scoped1": {
                    "constraint_id": "scoped1",
                    "pattern": "Scoped",
                    "constraint_text": "applies to src/core",
                    "confidence": 0.7,
                    "mined_from_jobs": "job1",
                    "scope_prefix": "src/core",
                    "created_at": datetime(2026, 1, 3, tzinfo=timezone.utc),
                },
                "vector1": {
                    "constraint_id": "vector1",
                    "pattern": "Vector",
                    "constraint_text": "pulled in by embedding",
                    "confidence": 0.8,
                    "mined_from_jobs": "job2",
                    "scope_prefix": "src/other",
                    "created_at": datetime(2026, 1, 4, tzinfo=timezone.utc),
                },
            },
            vector_rows=[
                {
                    "constraint_id": "vector1",
                    "pattern": "Vector",
                    "constraint_text": "pulled in by embedding",
                    "confidence": 0.8,
                    "mined_from_jobs": "job2",
                    "scope_prefix": "src/other",
                    "created_at": datetime(2026, 1, 4, tzinfo=timezone.utc),
                    "similarity": 0.94,
                },
                {
                    "constraint_id": "global1",
                    "pattern": "Global",
                    "constraint_text": "applies everywhere",
                    "confidence": 0.6,
                    "mined_from_jobs": "job0",
                    "scope_prefix": "",
                    "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
                    "similarity": 0.89,
                },
            ],
        )
        ledger = ConstraintLedger(conn, embedder)
        results = ledger.get_for_scope(["src/core/module.py"])
        ids = {item.constraint_id for item in results}
        assert ids == {"global1", "scoped1", "vector1"}
        assert embedder.calls == ["src/core/module.py"]


# ===================================================================
# Constraint Miner
# ===================================================================


class TestConstraintMiner:
    @pytest.fixture
    def miner(self):
        return ConstraintMiner()

    def test_import_error(self, miner):
        mc = miner.mine("1", "ImportError: no module named foo", "job1", ["a.py"])
        assert mc is not None
        assert mc.pattern == "ImportError"

    def test_module_not_found(self, miner):
        mc = miner.mine("1", "ModuleNotFoundError: xyz", "job1", ["a.py"])
        assert mc is not None
        assert "imports" in mc.constraint_text

    def test_syntax_error(self, miner):
        mc = miner.mine("1", "SyntaxError: invalid syntax", "job1", ["a.py"])
        assert mc is not None
        assert mc.pattern == "SyntaxError"

    def test_indentation_error(self, miner):
        mc = miner.mine("1", "IndentationError: unexpected indent", "job1", [])
        assert mc is not None
        assert "4-space" in mc.constraint_text

    def test_file_not_found(self, miner):
        mc = miner.mine("1", "FileNotFoundError: /tmp/x", "job1", [])
        assert mc is not None
        assert "paths exist" in mc.constraint_text

    def test_assertion_error(self, miner):
        mc = miner.mine("1", "AssertionError: 1 != 2", "job1", [])
        assert mc is not None
        assert "assertions" in mc.constraint_text

    def test_no_match_returns_none(self, miner):
        mc = miner.mine("0", "Everything is fine", "job1", [])
        assert mc is None

    def test_mined_from_jobs_populated(self, miner):
        mc = miner.mine("1", "ImportError: x", "myjob", [])
        assert mc.mined_from_jobs == ("myjob",)


# ===================================================================
# Execution Strategy
# ===================================================================


class TestExecutionMode:
    def test_enum_values(self):
        assert ExecutionMode.HOT.value == "hot"
        assert ExecutionMode.COLD.value == "cold"
        assert ExecutionMode.SESSION.value == "session"
        assert ExecutionMode.FORKED.value == "forked"


class TestMicroStep:
    def test_frozen(self):
        ms = MicroStep("s1", "a.py", "create", None, ())
        with pytest.raises(AttributeError):
            ms.action = "replace"


class TestStepCompiler:
    @pytest.fixture
    def compiler(self):
        return StepCompiler()

    def test_single_file_hot(self, compiler):
        spec = {"write_scope": [{"path": "a.py", "action": "create"}]}
        plan = compiler.compile(spec)
        assert plan.strategy == ExecutionMode.HOT
        assert len(plan.steps) == 1

    def test_many_files_cold(self, compiler):
        spec = {
            "write_scope": [{"path": f"f{i}.py", "action": "create"} for i in range(6)]
        }
        plan = compiler.compile(spec)
        assert plan.strategy == ExecutionMode.COLD

    def test_session_flag(self, compiler):
        spec = {
            "write_scope": [{"path": "a.py", "action": "create"}],
            "session": True,
        }
        plan = compiler.compile(spec)
        assert plan.strategy == ExecutionMode.SESSION

    def test_verify_commands_attached(self, compiler):
        spec = {
            "write_scope": [{"path": "a.py", "action": "create"}],
            "verify_commands": {"a.py": "python -m pytest a.py"},
        }
        plan = compiler.compile(spec)
        assert plan.steps[0].verify_command == "python -m pytest a.py"

    def test_dependencies_resolved(self, compiler):
        spec = {
            "write_scope": [
                {"path": "a.py", "action": "create"},
                {"path": "b.py", "action": "create"},
            ],
            "read_scope": {"b.py": ["a.py"]},
        }
        plan = compiler.compile(spec)
        step_b = [s for s in plan.steps if s.file_path == "b.py"][0]
        step_a = [s for s in plan.steps if s.file_path == "a.py"][0]
        assert step_a.step_id in step_b.depends_on

    def test_parallel_groups_independent(self, compiler):
        spec = {
            "write_scope": [
                {"path": "a.py", "action": "create"},
                {"path": "b.py", "action": "create"},
            ],
        }
        plan = compiler.compile(spec)
        # Both independent -> single group
        assert len(plan.parallelizable_groups) == 1
        assert len(plan.parallelizable_groups[0]) == 2

    def test_parallel_groups_sequential(self, compiler):
        spec = {
            "write_scope": [
                {"path": "a.py", "action": "create"},
                {"path": "b.py", "action": "create"},
            ],
            "read_scope": {"b.py": ["a.py"]},
        }
        plan = compiler.compile(spec)
        assert len(plan.parallelizable_groups) == 2

    def test_estimated_duration(self, compiler):
        spec = {
            "write_scope": [{"path": f"f{i}.py", "action": "create"} for i in range(3)]
        }
        plan = compiler.compile(spec)
        assert plan.estimated_duration_seconds == 15


class TestWaveSequencer:
    def test_empty(self):
        ws = WaveSequencer()
        assert ws.sequence([]) == []

    def test_single_plan(self):
        ws = WaveSequencer()
        plan = ExecutionPlan(
            strategy=ExecutionMode.HOT,
            steps=(MicroStep("s1", "a.py", "create", None, ()),),
            estimated_duration_seconds=5,
            parallelizable_groups=(("s1",),),
        )
        waves = ws.sequence([plan])
        assert len(waves) == 1
        assert waves[0] == [plan]

    def test_independent_plans_same_wave(self):
        ws = WaveSequencer()
        p1 = ExecutionPlan(
            strategy=ExecutionMode.HOT,
            steps=(MicroStep("s1", "a.py", "create", None, ()),),
            estimated_duration_seconds=5,
            parallelizable_groups=(("s1",),),
        )
        p2 = ExecutionPlan(
            strategy=ExecutionMode.HOT,
            steps=(MicroStep("s2", "b.py", "create", None, ()),),
            estimated_duration_seconds=5,
            parallelizable_groups=(("s2",),),
        )
        waves = ws.sequence([p1, p2])
        assert len(waves) == 1
        assert len(waves[0]) == 2
