"""Contract test for ModuleIndexer.stale_check.

Builds a fake index with a synthetic file, then mutates the file and asserts
stale_check spots the drift. Also verifies a deleted file shows up as
``missing``. Uses an in-memory connection stub so the test runs without
Postgres / pgvector.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

import pytest

from runtime.module_indexer import ModuleIndexer


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class _FakeRow(dict):
    """asyncpg-style row that supports both dict and attribute access."""


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def execute(self, sql: str, *_args):  # noqa: D401 - protocol stub
        return [_FakeRow(row) for row in self._rows]

    def fetchval(self, sql: str, *_args):
        return len(self._rows)


def _make_indexer(tmpdir: Path, rows: list[dict]) -> ModuleIndexer:
    indexer = ModuleIndexer.__new__(ModuleIndexer)
    indexer._conn = _FakeConn(rows)
    indexer._repo_root = str(tmpdir)
    indexer._embedder = None
    indexer._vector_store = None
    return indexer


def test_stale_check_reports_zero_for_unmodified_file(tmp_path: Path) -> None:
    rel = "Code&DBs/Workflow/runtime/example.py"
    target = tmp_path / rel
    target.parent.mkdir(parents=True)
    source = "x = 1\n"
    target.write_text(source)

    indexer = _make_indexer(
        tmp_path,
        [{"module_path": rel, "source_hash": _hash(source)}],
    )

    result = indexer.stale_check()
    assert result["stale_count"] == 0
    assert result["missing_count"] == 0
    assert result["checked"] == 1
    assert result["stale_paths"] == ()
    assert result["missing_paths"] == ()


def test_stale_check_flags_modified_file(tmp_path: Path) -> None:
    rel = "Code&DBs/Workflow/runtime/example.py"
    target = tmp_path / rel
    target.parent.mkdir(parents=True)
    target.write_text("x = 1\n")

    indexer = _make_indexer(
        tmp_path,
        [{"module_path": rel, "source_hash": _hash("OLD")}],
    )

    result = indexer.stale_check()
    assert result["stale_count"] == 1
    assert rel in result["stale_paths"]


def test_stale_check_flags_missing_file(tmp_path: Path) -> None:
    rel = "Code&DBs/Workflow/runtime/deleted.py"
    indexer = _make_indexer(
        tmp_path,
        [{"module_path": rel, "source_hash": _hash("anything")}],
    )

    result = indexer.stale_check()
    assert result["missing_count"] == 1
    assert rel in result["missing_paths"]


class _PruneFakeConn:
    """Captures DELETE/SELECT calls so we can assert on the prune sweep."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = list(rows)
        self.deletes: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        normalized = " ".join(sql.split())
        if normalized.startswith("DELETE FROM module_embeddings"):
            self.deletes.append((normalized, args))
            if "module_id = $1" in normalized:
                self._rows = [r for r in self._rows if r["module_id"] != args[0]]
            elif "module_path = $1" in normalized:
                self._rows = [r for r in self._rows if r["module_path"] != args[0]]
            return []
        if "module_id <> ALL" in normalized:
            path = args[0]
            keep_ids = set(args[1])
            return [
                _FakeRow(module_id=r["module_id"])
                for r in self._rows
                if r["module_path"] == path and r["module_id"] not in keep_ids
            ]
        if "DISTINCT module_path" in normalized:
            prefix = args[0].rstrip("%")
            return [
                _FakeRow(module_path=p)
                for p in sorted({r["module_path"] for r in self._rows if r["module_path"].startswith(prefix)})
            ]
        if normalized.startswith("SELECT module_id, source_hash"):
            return [_FakeRow(module_id=r["module_id"], source_hash=r["source_hash"]) for r in self._rows]
        if normalized.startswith("SELECT module_path, MIN(source_hash)"):
            grouped: dict[str, str] = {}
            for r in self._rows:
                grouped.setdefault(r["module_path"], r["source_hash"])
            return [_FakeRow(module_path=p, source_hash=h) for p, h in grouped.items()]
        return []

    def fetchval(self, sql: str, *args):
        return len(self._rows)


def test_index_codebase_prunes_orphans_and_missing_files(tmp_path: Path, monkeypatch) -> None:
    walked_subdir = "Code&DBs/Workflow/runtime"
    rel_keep = f"{walked_subdir}/extant.py"
    rel_missing = f"{walked_subdir}/deleted.py"
    keep_module_id = "keep-module"
    orphan_id = "orphan-class"
    missing_id = "missing-module"

    starting_rows = [
        {"module_id": keep_module_id, "module_path": rel_keep, "source_hash": "fresh"},
        {"module_id": orphan_id, "module_path": rel_keep, "source_hash": "old"},
        {"module_id": missing_id, "module_path": rel_missing, "source_hash": "old"},
    ]
    conn = _PruneFakeConn(starting_rows)

    indexer = ModuleIndexer.__new__(ModuleIndexer)
    indexer._conn = conn
    indexer._repo_root = str(tmp_path)
    indexer._embedder = None
    indexer._vector_store = None

    fake_unit = type(
        "Unit",
        (),
        {
            "module_id": keep_module_id,
            "module_path": rel_keep,
            "source_hash": "fresh",
        },
    )()
    monkeypatch.setattr(
        "runtime.module_indexer.walk_codebase",
        lambda repo_root, subdirs: [fake_unit],
    )

    result = indexer.index_codebase(subdirs=[walked_subdir])

    assert result["pruned_orphans"] == 1
    assert result["pruned_missing"] == 1
    assert result["skipped"] == 1
    remaining_ids = {r["module_id"] for r in conn._rows}
    assert remaining_ids == {keep_module_id}


def test_stale_check_sample_limit_truncates(tmp_path: Path) -> None:
    rows = []
    for i in range(120):
        rel = f"Code&DBs/Workflow/runtime/missing_{i:03d}.py"
        rows.append({"module_path": rel, "source_hash": _hash("X")})

    indexer = _make_indexer(tmp_path, rows)

    result = indexer.stale_check(sample_limit=10)
    assert result["missing_count"] == 120
    assert len(result["missing_paths"]) == 10
