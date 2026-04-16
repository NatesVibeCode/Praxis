from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from ..registry import registry


class QueryRoadmapTree(BaseModel):
    root_roadmap_item_id: str
    semantic_neighbor_limit: int = Field(default=5, ge=0)


def handle_query_roadmap_tree(query: QueryRoadmapTree, subsystems: Any) -> dict[str, Any]:
    from surfaces.api import operator_read

    env = getattr(subsystems, "_postgres_env", None)
    resolved_env = env() if callable(env) else None
    return operator_read.query_roadmap_tree(
        root_roadmap_item_id=query.root_roadmap_item_id,
        semantic_neighbor_limit=query.semantic_neighbor_limit,
        env=resolved_env,
    )


registry.register(
    path="/api/operator/roadmap/tree/{root_roadmap_item_id}",
    method="GET",
    command_class=QueryRoadmapTree,
    handler=handle_query_roadmap_tree,
    description="Read a roadmap tree with dependency edges and semantic neighbors.",
)
