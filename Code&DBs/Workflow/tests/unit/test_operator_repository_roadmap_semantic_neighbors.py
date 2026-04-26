from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from surfaces.api import _operator_repository as operator_repository
from surfaces.api._operator_repository import NativeOperatorQueryFrontdoor


class _FakeConn:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def fetch(self, query: str, *args: object):
        del args
        normalized = " ".join(query.lower().split())
        self.queries.append(normalized)
        if "from semantic_assertions" in normalized:
            return []
        raise AssertionError(f"unexpected roadmap semantic neighbor query: {normalized}")


def test_roadmap_semantic_neighbors_do_not_fallback_to_embeddings_when_semantics_are_empty() -> None:
    conn = _FakeConn()
    frontdoor = NativeOperatorQueryFrontdoor()

    neighbors, reason = asyncio.run(
        frontdoor._fetch_roadmap_semantic_neighbors(
            conn=conn,
            as_of=datetime(2026, 4, 17, 18, 0, tzinfo=timezone.utc),
            root_roadmap_item_id="roadmap_item.semantic.root",
            subtree_roadmap_item_ids=("roadmap_item.semantic.root",),
            limit=5,
        )
    )

    assert neighbors == ()
    assert reason == "roadmap.semantic_neighbors.none"
    assert len(conn.queries) == 1
    assert "semantic_assertions" in conn.queries[0]
    assert "embedding" not in conn.queries[0]
    assert "<=>" not in conn.queries[0]


def test_query_frontdoor_resolves_repo_database_authority_before_native_instance(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeInstance:
        def to_contract(self) -> dict[str, str]:
            return {"praxis_instance_name": "praxis"}

    def _workflow_database_env_for_repo(repo_root, *, env):
        captured["repo_root"] = repo_root
        captured["incoming_env"] = dict(env)
        return {
            "WORKFLOW_DATABASE_URL": "postgresql://resolved/praxis",
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "test",
            "PATH": "/bin",
        }

    def _get_workflow_pool(*, env):
        captured["pool_env"] = dict(env)
        return object()

    def _resolve_native_instance_from_connection(conn, *, env):
        captured["conn"] = conn
        captured["resolved_env"] = dict(env)
        return _FakeInstance()

    monkeypatch.setattr(
        operator_repository,
        "workflow_database_env_for_repo",
        _workflow_database_env_for_repo,
    )
    monkeypatch.setattr(
        operator_repository,
        "get_workflow_pool",
        _get_workflow_pool,
    )
    monkeypatch.setattr(
        operator_repository,
        "resolve_native_instance_from_connection",
        _resolve_native_instance_from_connection,
    )

    source, instance = NativeOperatorQueryFrontdoor()._resolve_instance(env={"PATH": "/bin"})

    assert instance.to_contract() == {"praxis_instance_name": "praxis"}
    assert source["WORKFLOW_DATABASE_URL"] == "postgresql://resolved/praxis"
    assert captured["incoming_env"] == {"PATH": "/bin"}
    assert captured["resolved_env"]["WORKFLOW_DATABASE_URL"] == "postgresql://resolved/praxis"
