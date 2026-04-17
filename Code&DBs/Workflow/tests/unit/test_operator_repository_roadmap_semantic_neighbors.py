from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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
