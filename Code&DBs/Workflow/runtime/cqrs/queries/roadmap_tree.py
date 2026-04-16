from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from ..registry import registry


class QueryRoadmapTree(BaseModel):
    root_roadmap_item_id: str
    semantic_neighbor_limit: int = Field(default=5, ge=0)
    include_completed_nodes: bool = True


def handle_query_roadmap_tree(query: QueryRoadmapTree, subsystems: Any) -> dict[str, Any]:
    from surfaces.api import operator_read

    env = getattr(subsystems, "_postgres_env", None)
    resolved_env = env() if callable(env) else None
    return operator_read.query_roadmap_tree(
        root_roadmap_item_id=query.root_roadmap_item_id,
        semantic_neighbor_limit=query.semantic_neighbor_limit,
        include_completed_nodes=query.include_completed_nodes,
        env=resolved_env,
    )


registry.register(
    path="/api/operator/roadmap/tree/{root_roadmap_item_id}",
    method="GET",
    command_class=QueryRoadmapTree,
    handler=handle_query_roadmap_tree,
    description="Read a roadmap tree with dependency edges and semantic neighbors.",
    operation_name="operator.roadmap_tree",
    operation_kind="query",
    source_kind="cqrs_query",
    authority_ref="authority.roadmap_items",
    projection_ref="projection.roadmap_tree",
)
