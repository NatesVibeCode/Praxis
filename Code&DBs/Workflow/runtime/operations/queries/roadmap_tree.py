from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class QueryRoadmapTree(BaseModel):
    root_roadmap_item_id: str
    semantic_neighbor_limit: int = Field(default=5, ge=0)
    include_completed_nodes: bool = True


def handle_query_roadmap_tree(query: QueryRoadmapTree, subsystems: Any) -> dict[str, Any]:
    from surfaces.api.operator_read import NativeOperatorQueryFrontdoor

    env = getattr(subsystems, "_postgres_env", None)
    resolved_env = env() if callable(env) else None
    return NativeOperatorQueryFrontdoor().query_roadmap_tree(
        root_roadmap_item_id=query.root_roadmap_item_id,
        semantic_neighbor_limit=query.semantic_neighbor_limit,
        include_completed_nodes=query.include_completed_nodes,
        env=resolved_env,
    )
