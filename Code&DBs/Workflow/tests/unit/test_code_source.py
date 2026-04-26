"""Unit tests for the code search source plugin."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from runtime.sources.code_source import (
    CodeSourceError,
    maybe_refresh_index,
    search_code,
)
from surfaces.mcp.tools._search_envelope import parse_envelope


class _StubIndexer:
    """Stand-in for ModuleIndexer used in semantic-mode tests."""

    def __init__(self, hits=None, stats=None) -> None:
        self._hits = hits or []
        self._stats = stats or {
            "total_indexed": 0,
            "by_kind": {},
            "observability_state": "complete",
        }

    def search(self, *, query, limit, kind, threshold):  # noqa: D401
        return list(self._hits)

    def stats(self):
        return dict(self._stats)


def _make_repo(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text))
    return tmp_path


def test_exact_mode_finds_literal_with_context(tmp_path: Path):
    repo = _make_repo(
        tmp_path,
        {
            "Code&DBs/Workflow/runtime/example.py": """\
                import subprocess

                def run_thing():
                    subprocess.run(["ls"])
                    return True
            """,
        },
    )
    envelope = parse_envelope(
        {
            "query": "subprocess.",
            "mode": "exact",
            "scope": {"paths": ["Code&DBs/Workflow/runtime/**/*.py"]},
            "shape": "context",
            "context_lines": 1,
        }
    )
    indexer = _StubIndexer()
    results, freshness = search_code(envelope=envelope, indexer=indexer, repo_root=repo)
    assert results, "expected at least one match"
    assert all(r["source"] == "code" for r in results)
    assert any("subprocess" in r["match_text"] for r in results)
    assert any(r.get("context") for r in results)
    assert freshness["status"] == "complete"


def test_regex_mode_finds_pattern(tmp_path: Path):
    repo = _make_repo(
        tmp_path,
        {
            "Code&DBs/Workflow/runtime/auth.py": """\
                class FooAuthority:
                    pass

                class BarAuthority:
                    pass

                class Unrelated:
                    pass
            """,
        },
    )
    envelope = parse_envelope(
        {
            "query": "/class.*Authority/",
            "mode": "regex",
            "scope": {"paths": ["Code&DBs/Workflow/runtime/**/*.py"]},
            "shape": "match",
        }
    )
    indexer = _StubIndexer()
    results, _ = search_code(envelope=envelope, indexer=indexer, repo_root=repo)
    matches = [r["match_text"] for r in results]
    assert any("FooAuthority" in m for m in matches)
    assert any("BarAuthority" in m for m in matches)
    assert all("Unrelated" not in m for m in matches)


def test_path_glob_excludes_outside_scope(tmp_path: Path):
    repo = _make_repo(
        tmp_path,
        {
            "Code&DBs/Workflow/runtime/match.py": "needle = 1\n",
            "tests/elsewhere.py": "needle = 2\n",
        },
    )
    envelope = parse_envelope(
        {
            "query": "needle",
            "mode": "exact",
            "scope": {"paths": ["Code&DBs/Workflow/**/*.py"]},
            "shape": "match",
        }
    )
    indexer = _StubIndexer()
    results, _ = search_code(envelope=envelope, indexer=indexer, repo_root=repo)
    assert results
    assert all("Code&DBs/Workflow/" in r["path"] for r in results)


def test_exclude_terms_filters_match(tmp_path: Path):
    repo = _make_repo(
        tmp_path,
        {
            "Code&DBs/Workflow/runtime/legacy.py": "subprocess.run(['legacy'])\n",
            "Code&DBs/Workflow/runtime/modern.py": "subprocess.run(['modern'])\n",
        },
    )
    envelope = parse_envelope(
        {
            "query": "subprocess.",
            "mode": "exact",
            "scope": {
                "paths": ["Code&DBs/Workflow/runtime/**/*.py"],
                "exclude_terms": ["legacy"],
            },
            "shape": "match",
        }
    )
    indexer = _StubIndexer()
    results, _ = search_code(envelope=envelope, indexer=indexer, repo_root=repo)
    assert results
    assert all("legacy" not in r["match_text"] for r in results)


def test_invalid_regex_raises(tmp_path: Path):
    repo = _make_repo(tmp_path, {"a.py": "x = 1\n"})
    envelope = parse_envelope({"query": "/(/", "mode": "regex"})
    indexer = _StubIndexer()
    with pytest.raises(CodeSourceError):
        search_code(envelope=envelope, indexer=indexer, repo_root=repo)


class _FreshnessIndexer:
    """Indexer stub that tracks last_indexed_iso + supports stale check."""

    def __init__(self, *, stale_count: int = 0, stale_paths=()) -> None:
        self._stale_count = stale_count
        self._stale_paths = list(stale_paths)
        self._last_indexed_iso = "2026-04-01T00:00:00+00:00"
        self.index_paths_called_with: list[tuple] = []

    def stale_check(self, *, sample_limit: int):
        return {
            "stale_count": self._stale_count,
            "missing_count": 0,
            "stale_paths": self._stale_paths,
            "missing_paths": (),
            "observability_state": "complete",
        }

    def index_paths(self, paths, *, force=False, stall_budget_seconds=30.0):
        self.index_paths_called_with.append((tuple(paths or ()), force))
        self._last_indexed_iso = "2026-04-01T00:01:00+00:00"
        return {"indexed": len(self._stale_paths), "elapsed_seconds": 0.01}

    def last_indexed_iso(self):
        return self._last_indexed_iso

    def stats(self):
        return {
            "total_indexed": 100,
            "by_kind": {"module": 100},
            "observability_state": "complete",
        }


def test_maybe_refresh_index_below_threshold_is_noop():
    indexer = _FreshnessIndexer(stale_count=2)
    report = maybe_refresh_index(indexer, stale_threshold=5)
    assert report["triggered"] is False
    assert report["status"] == "fresh"
    assert indexer.index_paths_called_with == []


def test_maybe_refresh_index_triggers_when_drift_above_threshold():
    indexer = _FreshnessIndexer(
        stale_count=10,
        stale_paths=["Code&DBs/Workflow/runtime/foo.py", "Code&DBs/Workflow/surfaces/bar.py"],
    )
    report = maybe_refresh_index(indexer, stale_threshold=5)
    assert report["triggered"] is True
    assert report["status"] == "refreshed"
    assert indexer.index_paths_called_with, "expected index_paths to be called"
    called_subdirs = indexer.index_paths_called_with[0][0]
    assert "Code&DBs" in called_subdirs


def test_maybe_refresh_index_unsupported_indexer():
    class _Plain:
        pass

    report = maybe_refresh_index(_Plain(), stale_threshold=5)
    assert report["status"] == "unsupported"


def test_semantic_mode_uses_indexer_results(tmp_path: Path):
    repo = _make_repo(
        tmp_path,
        {
            "Code&DBs/Workflow/runtime/example.py": "x = 1\n",
        },
    )
    envelope = parse_envelope(
        {"query": "anything semantic", "mode": "semantic", "shape": "match"}
    )
    indexer = _StubIndexer(
        hits=[
            {
                "module_path": "Code&DBs/Workflow/runtime/example.py",
                "name": "example",
                "kind": "module",
                "summary": "an example module",
                "docstring_preview": "Example module.",
                "signature": "module example",
                "cosine_similarity": 0.7,
                "fused_score": 0.04,
            }
        ],
        stats={"total_indexed": 1, "by_kind": {"module": 1}, "observability_state": "complete"},
    )
    results, freshness = search_code(envelope=envelope, indexer=indexer, repo_root=repo)
    assert results
    assert results[0]["source"] == "code"
    assert results[0]["found_via"] == "semantic"
    assert results[0]["score"] > 0
    assert freshness["total_indexed"] == 1
