"""Tests for the module_indexer functional synonym detection system."""
import ast
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import runtime.module_indexer as module_indexer_mod
from runtime.module_indexer import (
    BehaviorFingerprint,
    CodeUnit,
    ModuleIndexer,
    extract_code_units,
    walk_codebase,
    _extract_behavior,
    _extract_verbs,
    _build_summary,
    _function_signature,
    _source_hash,
)


# ---------------------------------------------------------------------------
# AST extraction tests
# ---------------------------------------------------------------------------

def test_extract_verbs_from_function_names():
    assert _extract_verbs("fetch_user_data") == ["fetch"]
    assert _extract_verbs("create_and_save") == ["create", "save"]
    assert _extract_verbs("subscribe_to_events") == ["subscribe"]
    assert _extract_verbs("checkpoint_replay_state") == ["checkpoint", "replay"]
    assert _extract_verbs("just_a_name") == []


def test_extract_behavior_finds_imports():
    source = "import subprocess\nimport json\nfrom pathlib import Path\n"
    tree = ast.parse(source)
    fp = _extract_behavior(tree, source)
    assert "subprocess" in fp.imports
    assert "json" in fp.imports
    assert "pathlib" in fp.imports


def test_extract_behavior_finds_sql_tables():
    source = '''
query = """
    INSERT INTO receipts (receipt_id, workflow_id, run_id, request_id, started_at, finished_at)
    VALUES ($1, $2, $3, $4)
"""
other = "SELECT * FROM workflow_outbox WHERE run_id = $1"
'''
    tree = ast.parse(source)
    fp = _extract_behavior(tree, source)
    assert "receipts" in fp.db_tables
    assert "workflow_outbox" in fp.db_tables


def test_extract_behavior_finds_io_patterns():
    source = '''
import subprocess
result = subprocess.run(["ls"], capture_output=True)
with open("file.txt", "w") as f:
    f.write("data")
'''
    tree = ast.parse(source)
    fp = _extract_behavior(tree, source)
    assert "spawns subprocesses" in fp.io_patterns
    assert "reads/writes files" in fp.io_patterns


def test_extract_behavior_finds_classes():
    source = '''
class MyWorker:
    pass

class EventHandler:
    pass
'''
    tree = ast.parse(source)
    fp = _extract_behavior(tree, source)
    assert "MyWorker" in fp.data_structures
    assert "EventHandler" in fp.data_structures


def test_extract_behavior_finds_exception_handlers():
    source = '''
try:
    pass
except FileNotFoundError:
    pass
except TimeoutError:
    pass
'''
    tree = ast.parse(source)
    fp = _extract_behavior(tree, source)
    assert "FileNotFoundError" in fp.exceptions_handled
    assert "TimeoutError" in fp.exceptions_handled


# ---------------------------------------------------------------------------
# Summary building tests
# ---------------------------------------------------------------------------

def test_build_summary_includes_docstring():
    fp = BehaviorFingerprint()
    summary = _build_summary("outbox", "module", "Durable event delivery system.", "", fp)
    assert "outbox" in summary
    assert "Durable event delivery" in summary


def test_build_summary_includes_behavior():
    fp = BehaviorFingerprint(
        db_tables=["workflow_outbox", "subscription_checkpoints"],
        io_patterns=["async Postgres operations"],
        key_operations=["subscribe", "checkpoint", "replay"],
    )
    summary = _build_summary("subscriber", "class", "", "class subscriber(Protocol)", fp)
    assert "workflow_outbox" in summary
    assert "subscribe" in summary


# ---------------------------------------------------------------------------
# Code unit extraction tests
# ---------------------------------------------------------------------------

def test_extract_code_units_from_python_file():
    source = '''"""Module docstring."""

class Fetcher:
    """Fetches data from remote sources."""
    def fetch(self, url: str) -> dict:
        pass

def process_batch(items: list) -> int:
    """Process a batch of items."""
    return len(items)
'''
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(source)
        filepath = f.name

    try:
        units = extract_code_units(filepath, os.path.dirname(filepath))
        names = {u.name for u in units}
        kinds = {u.name: u.kind for u in units}

        # Should find module, class, and function
        assert os.path.splitext(os.path.basename(filepath))[0] in names
        assert "Fetcher" in names
        assert "process_batch" in names
        assert kinds["Fetcher"] == "class"
        assert kinds["process_batch"] == "function"

        # Each unit should have a summary
        for unit in units:
            assert unit.summary
            assert unit.source_hash
    finally:
        os.unlink(filepath)


def test_extract_skips_private_and_test_classes():
    source = '''
class _PrivateHelper:
    pass

class PublicClass:
    pass

def _private_func():
    pass

def public_func():
    pass
'''
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(source)
        filepath = f.name

    try:
        units = extract_code_units(filepath, os.path.dirname(filepath))
        names = {u.name for u in units}

        assert "PublicClass" in names
        assert "public_func" in names
        assert "_PrivateHelper" not in names
        assert "_private_func" not in names
    finally:
        os.unlink(filepath)


def test_walk_codebase_finds_python_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        subdir = os.path.join(tmpdir, "src")
        os.makedirs(subdir)

        # Write a module file
        with open(os.path.join(subdir, "worker.py"), "w") as f:
            f.write('"""Worker module."""\nclass Worker:\n    pass\n')

        # Write a test file (should be skipped)
        with open(os.path.join(subdir, "test_worker.py"), "w") as f:
            f.write('def test_thing(): pass\n')

        # Write __init__.py (should be skipped)
        with open(os.path.join(subdir, "__init__.py"), "w") as f:
            f.write('')

        units = walk_codebase(tmpdir, ["src"])
        names = {u.name for u in units}

        assert "worker" in names or "Worker" in names
        assert "test_worker" not in names


def test_index_codebase_upserts_embedding_in_same_statement(monkeypatch):
    unit = CodeUnit(
        module_id="module.alpha",
        module_path="src/example.py",
        kind="module",
        name="example",
        docstring="Example module.",
        signature="",
        behavior=BehaviorFingerprint(imports=["json"]),
        summary="Module example. Uses json.",
        source_hash="hash1234",
    )

    class _FakeEmbedder:
        DIMENSIONS = 384
        authority = None

        def __init__(self, model_name: str) -> None:
            self.model_name = model_name
            self.dimensions = self.DIMENSIONS

        def embed(self, texts):
            assert texts == [unit.summary]
            return [[0.01] * self.DIMENSIONS]

        def embed_one(self, text):
            raise AssertionError("index_codebase should batch embeddings")

    class _FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *args):
            self.calls.append((query, args))
            if "SELECT module_id, source_hash FROM module_embeddings" in query:
                return []
            return []

        def fetchval(self, query: str, *args):
            return 0

    monkeypatch.setattr(module_indexer_mod, "EmbeddingService", _FakeEmbedder)
    monkeypatch.setattr(module_indexer_mod, "walk_codebase", lambda repo_root, subdirs: [unit])

    conn = _FakeConn()
    result = ModuleIndexer(conn=conn, repo_root="/repo").index_codebase(subdirs=["src"])

    insert_query, insert_args = next(
        (query, args)
        for query, args in conn.calls
        if "INSERT INTO module_embeddings" in query
    )
    assert "embedding" in insert_query
    assert "EXCLUDED.embedding" in insert_query
    assert isinstance(insert_args[9], str)
    assert insert_args[9].startswith("[")
    assert result == {
        "indexed": 1,
        "skipped": 0,
        "pruned_orphans": 0,
        "pruned_missing": 0,
        "total": 1,
        "observability_state": "complete",
        "errors": (),
    }


def test_index_codebase_skips_missing_embedding_without_writing_null(monkeypatch, capsys):
    unit = CodeUnit(
        module_id="module.alpha",
        module_path="src/example.py",
        kind="module",
        name="example",
        docstring="Example module.",
        signature="",
        behavior=BehaviorFingerprint(imports=["json"]),
        summary="Module example. Uses json.",
        source_hash="hash1234",
    )

    class _FakeEmbedder:
        DIMENSIONS = 384
        authority = None

        def __init__(self, model_name: str) -> None:
            self.model_name = model_name
            self.dimensions = self.DIMENSIONS

        def embed(self, texts):
            assert texts == [unit.summary]
            return [None]

        def embed_one(self, text):
            raise AssertionError("index_codebase should batch embeddings")

    class _FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *args):
            self.calls.append((query, args))
            if "SELECT module_id, source_hash FROM module_embeddings" in query:
                return []
            return []

        def fetchval(self, query: str, *args):
            return 0

    monkeypatch.setattr(module_indexer_mod, "EmbeddingService", _FakeEmbedder)
    monkeypatch.setattr(module_indexer_mod, "walk_codebase", lambda repo_root, subdirs: [unit])

    conn = _FakeConn()
    result = ModuleIndexer(conn=conn, repo_root="/repo").index_codebase(subdirs=["src"])
    captured = capsys.readouterr()

    assert result["indexed"] == 0
    assert result["skipped"] == 0
    assert result["total"] == 1
    assert result["observability_state"] == "degraded"
    assert result["errors"] == (
        {
            "scope": "index_codebase",
            "code": "module_indexer.index_failed",
            "module_path": "src/example.py",
            "kind": "module",
            "name": "example",
            "error_type": "RuntimeError",
            "error_message": "embedding_missing",
        },
    )
    assert "embedding_missing" in captured.err
    assert not any("INSERT INTO module_embeddings" in query for query, _ in conn.calls)


# ---------------------------------------------------------------------------
# Source hash tests
# ---------------------------------------------------------------------------

def test_source_hash_deterministic():
    h1 = _source_hash("hello world")
    h2 = _source_hash("hello world")
    h3 = _source_hash("hello world!")
    assert h1 == h2
    assert h1 != h3
    assert len(h1) == 16


# ---------------------------------------------------------------------------
# Behavior fingerprint tests
# ---------------------------------------------------------------------------

def test_behavior_to_text():
    fp = BehaviorFingerprint(
        db_tables=["receipts"],
        io_patterns=["spawns subprocesses"],
        key_operations=["execute", "dispatch"],
    )
    text = fp.to_text()
    assert "receipts" in text
    assert "subprocesses" in text
    assert "execute" in text


def test_behavior_to_dict_omits_empty():
    fp = BehaviorFingerprint(imports=["json"], db_tables=[])
    d = fp.to_dict()
    assert "imports" in d
    assert "db_tables" not in d  # empty list omitted


# ---------------------------------------------------------------------------
# Integration test — requires Postgres with pgvector
# ---------------------------------------------------------------------------

def _get_pg_conn():
    """Try to get a Postgres connection. Skip if unavailable."""
    try:
        from storage.postgres.connection import ensure_postgres_available
        env = {
            "WORKFLOW_DATABASE_URL": "postgresql://postgres@localhost:5432/praxis_test",
            "PATH": os.environ.get("PATH", ""),
        }
        return ensure_postgres_available(env=env)
    except Exception:
        return None


@pytest.fixture
def pg_conn():
    conn = _get_pg_conn()
    if conn is None:
        pytest.skip("Postgres unavailable")
    return conn


def test_indexer_index_and_search(pg_conn):
    """End-to-end: index temp files, search by functional description."""
    with tempfile.TemporaryDirectory() as tmpdir:
        srcdir = os.path.join(tmpdir, "src")
        os.makedirs(srcdir)

        # Write a module about durable messaging
        with open(os.path.join(srcdir, "durable_queue.py"), "w") as f:
            f.write(
                '"""Durable message queue with checkpoint-based replay.\n'
                '\n'
                'Persists messages to Postgres and supports subscriber\n'
                'checkpoint resumption across process restarts.\n'
                '"""\n'
                '\n'
                'class DurableQueue:\n'
                '    """Queue that persists messages and supports replay."""\n'
                '    def publish(self, message: dict) -> str:\n'
                '        pass\n'
                '    def subscribe(self, consumer_id: str) -> list:\n'
                '        pass\n'
                '    def checkpoint(self, consumer_id: str, offset: int) -> None:\n'
                '        pass\n'
            )

        # Write a module about HTTP routing
        with open(os.path.join(srcdir, "http_router.py"), "w") as f:
            f.write(
                '"""HTTP request router."""\n'
                'class Router:\n'
                '    def route(self, path: str) -> callable:\n'
                '        pass\n'
            )

        indexer = ModuleIndexer(conn=pg_conn, repo_root=tmpdir)

        # Index
        result = indexer.index_codebase(subdirs=["src"])
        assert result["indexed"] > 0

        # Search for "durable messaging" should find durable_queue
        results = indexer.search("durable messaging with checkpoint", limit=5, threshold=0.2)
        names = [r["name"] for r in results]
        assert any("durable" in n.lower() or "DurableQueue" in n for n in names), \
            f"Expected durable_queue in results, got: {names}"

        # Cleanup test entries
        pg_conn.execute(
            "DELETE FROM module_embeddings WHERE module_path LIKE 'src/%'"
        )

    # Stats should still work
    stats = indexer.stats()
    assert "total_indexed" in stats


def test_indexer_skips_unchanged(pg_conn):
    """Re-indexing with unchanged source should skip."""
    with tempfile.TemporaryDirectory() as tmpdir:
        srcdir = os.path.join(tmpdir, "src")
        os.makedirs(srcdir)

        with open(os.path.join(srcdir, "stable.py"), "w") as f:
            f.write('"""Stable module."""\nclass Stable:\n    pass\n')

        indexer = ModuleIndexer(conn=pg_conn, repo_root=tmpdir)

        r1 = indexer.index_codebase(subdirs=["src"])
        assert r1["indexed"] > 0
        assert r1["skipped"] == 0

        r2 = indexer.index_codebase(subdirs=["src"])
        assert r2["indexed"] == 0
        assert r2["skipped"] > 0

        # Cleanup
        pg_conn.execute(
            "DELETE FROM module_embeddings WHERE module_path LIKE 'src/%'"
        )


def test_stats_surfaces_backend_errors():
    class _BrokenConn:
        def fetchval(self, query: str):
            raise RuntimeError("index snapshot offline")

        def execute(self, query: str):
            raise RuntimeError("index snapshot offline")

    indexer = ModuleIndexer(conn=_BrokenConn(), repo_root=os.getcwd())
    stats = indexer.stats()

    assert stats["total_indexed"] == 0
    assert stats["by_kind"] == {}
    assert stats["observability_state"] == "degraded"
    assert stats["errors"] == (
        {
            "scope": "stats",
            "code": "module_indexer.stats_failed",
            "error_type": "RuntimeError",
            "error_message": "index snapshot offline",
        },
    )


def test_indexer_surfaces_write_failures(monkeypatch):
    class _BrokenConn:
        def execute(self, query: str, *params):
            if query.strip().startswith("INSERT INTO module_embeddings"):
                raise RuntimeError("write lane offline")
            return []

    class _FakeEmbedder:
        dimensions = 2

        def embed(self, summaries):
            return [(0.1, 0.2) for _ in summaries]

    with tempfile.TemporaryDirectory() as tmpdir:
        srcdir = os.path.join(tmpdir, "src")
        os.makedirs(srcdir)
        with open(os.path.join(srcdir, "writer.py"), "w") as f:
            f.write('"""Writer module."""\ndef write_item():\n    return True\n')

        indexer = ModuleIndexer(conn=_BrokenConn(), repo_root=tmpdir)
        monkeypatch.setattr(indexer, "_embedder", _FakeEmbedder())

        result = indexer.index_codebase(subdirs=["src"], force=True)

    assert result["indexed"] == 0
    assert result["skipped"] == 0
    assert result["total"] == 2
    assert result["observability_state"] == "degraded"
    assert result["errors"] == (
        {
            "scope": "index_codebase",
            "code": "module_indexer.index_failed",
            "module_path": "src/writer.py",
            "kind": "module",
            "name": "writer",
            "error_type": "RuntimeError",
            "error_message": "write lane offline",
        },
        {
            "scope": "index_codebase",
            "code": "module_indexer.index_failed",
            "module_path": "src/writer.py",
            "kind": "function",
            "name": "write_item",
            "error_type": "RuntimeError",
            "error_message": "write lane offline",
        },
    )
