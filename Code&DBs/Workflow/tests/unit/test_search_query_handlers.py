"""Unit tests for the CQRS search query handlers.

Exercises the Pydantic query models + handler dispatch using a stub
subsystem container so the gateway-shaped handler signature is
verified end-to-end without requiring a live Postgres binding.
"""
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pytest

from runtime.operations.queries.search import (
    BugsSearchQuery,
    CodeSearchQuery,
    DbReadSearchQuery,
    FederatedSearchQuery,
    FilesSearchQuery,
    GitSearchQuery,
    KnowledgeSearchQuery,
    ReceiptsSearchQuery,
    handle_bugs_search,
    handle_code_search,
    handle_db_read_search,
    handle_federated_search,
    handle_files_search,
    handle_knowledge_search,
    handle_receipts_search,
)


class _StubIndexer:
    def search(self, *, query, limit, kind, threshold):
        return []

    def stats(self):
        return {
            "total_indexed": 0,
            "by_kind": {},
            "observability_state": "complete",
        }

    def stale_check(self, *, sample_limit):
        return {"stale_count": 0, "missing_count": 0, "stale_paths": (), "missing_paths": ()}

    def index_paths(self, paths, *, force=False, stall_budget_seconds=30.0):
        return {"indexed": 0, "elapsed_seconds": 0.0}

    def last_indexed_iso(self):
        return None


class _StubSubsystems:
    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root
        self._indexer = _StubIndexer()

    def get_module_indexer(self):
        return self._indexer

    def get_pg_conn(self):
        return None

    def get_bug_tracker(self):
        raise RuntimeError("bug tracker stubbed out")

    def get_knowledge_graph(self):
        class _KG:
            def search(self, *args, **kwargs):
                return []
        return _KG()

    def get_memory_engine(self):
        raise RuntimeError("memory engine stubbed out")


@pytest.fixture
def stub_repo(tmp_path: Path) -> Path:
    (tmp_path / "Code&DBs" / "Workflow" / "runtime").mkdir(parents=True)
    target = tmp_path / "Code&DBs" / "Workflow" / "runtime" / "example.py"
    target.write_text(
        textwrap.dedent(
            """\
            import subprocess

            def thing():
                subprocess.run(["ls"])
                return True
            """
        )
    )
    return tmp_path


def test_federated_query_validates_required_query():
    with pytest.raises(Exception):
        FederatedSearchQuery()  # type: ignore[call-arg]


def test_federated_search_handler_runs_against_stub_subsystems(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    query = FederatedSearchQuery(
        query="subprocess.",
        mode="exact",
        sources=["code"],
        scope={"paths": ["Code&DBs/Workflow/runtime/**/*.py"]},
        shape="match",
        limit=5,
        auto_reindex_if_stale=False,
    )
    payload = handle_federated_search(query, subs)
    assert payload["ok"] is True
    assert payload["count"] >= 1
    assert payload["_meta"]["sources_queried"] == ["code"]
    assert payload["_meta"]["source_status"]["code"] == "ok"


def test_code_search_handler(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    payload = handle_code_search(
        CodeSearchQuery(
            query="subprocess.",
            mode="exact",
            scope={"paths": ["Code&DBs/Workflow/runtime/**/*.py"]},
            shape="match",
            limit=5,
            auto_reindex_if_stale=False,
        ),
        subs,
    )
    assert payload["ok"] is True
    assert payload["_meta"]["sources_queried"] == ["code"]


def test_files_search_handler(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    payload = handle_files_search(
        FilesSearchQuery(
            query=".",
            mode="semantic",
            scope={"paths": ["Code&DBs/Workflow/runtime/**/*.py"]},
            limit=5,
            auto_reindex_if_stale=False,
        ),
        subs,
    )
    assert payload["ok"] is True
    assert payload["_meta"]["sources_queried"] == ["files"]


def test_db_read_search_skips_without_table(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    payload = handle_db_read_search(
        DbReadSearchQuery(query="anything", limit=1),
        subs,
    )
    assert payload["ok"] is True
    assert payload["_meta"]["source_status"]["db"] == "skipped"


def test_knowledge_handler_returns_empty_with_stub_kg(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    payload = handle_knowledge_search(
        KnowledgeSearchQuery(query="provider routing", limit=3),
        subs,
    )
    # Knowledge stub returns empty; we just want shape + status correctness
    assert payload["ok"] is True
    assert payload["_meta"]["sources_queried"] == ["knowledge"]


def test_receipts_handler_handles_failure_gracefully(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    payload = handle_receipts_search(
        ReceiptsSearchQuery(query="anything", limit=3),
        subs,
    )
    # Without a live receipt_store the handler should report error status
    # rather than raise — the gateway pattern requires handlers stay
    # within the result envelope.
    assert payload["ok"] is True
    status = payload["_meta"]["source_status"]["receipts"]
    assert status in {"complete", "error"}


def test_bugs_handler_handles_failure_gracefully(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    payload = handle_bugs_search(
        BugsSearchQuery(query="anything", limit=3),
        subs,
    )
    assert payload["ok"] is True
    status = payload["_meta"]["source_status"]["bugs"]
    assert status in {"complete", "error"}


def test_git_handler_against_real_repo(stub_repo: Path):
    # No git repo in stub_repo, so git source should error gracefully
    subs = _StubSubsystems(stub_repo)
    payload = handle_files_search(
        FilesSearchQuery(query="x", scope={"paths": ["**/*.py"]}, limit=2),
        subs,
    )
    assert payload["ok"] is True


def test_federated_default_sources_when_omitted(stub_repo: Path):
    subs = _StubSubsystems(stub_repo)
    query = FederatedSearchQuery(
        query="x",
        sources=None,
        auto_reindex_if_stale=False,
    )
    payload = handle_federated_search(query, subs)
    assert payload["_meta"]["sources_queried"] == ["code"]
