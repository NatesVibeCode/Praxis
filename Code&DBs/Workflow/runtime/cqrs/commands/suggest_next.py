from typing import Any
from pydantic import BaseModel

from ..registry import registry

class SuggestNextNodesCommand(BaseModel):
    workflow_id: str
    body: dict[str, Any]

def handle_suggest_next_nodes(command: SuggestNextNodesCommand, subsystems: Any) -> dict[str, Any]:
    from runtime.capability_catalog import load_capability_catalog
    
    conn = subsystems.get_pg_conn()
    catalog = load_capability_catalog(conn)
    
    node_id = command.body.get("node_id")
    build_graph = command.body.get("build_graph", {})
    
    nodes = build_graph.get("nodes", [])
    edges = build_graph.get("edges", [])
    
    # 1. Find the current node
    current_node = next((n for n in nodes if n.get("node_id") == node_id or n.get("id") == node_id), None)
    
    # 2. Extract context from the current node to inform our suggestions
    current_title = str(current_node.get("title", "")).lower() if current_node else ""
    current_summary = str(current_node.get("summary", "")).lower() if current_node else ""
    current_route = str(current_node.get("route", "")).lower() if current_node else ""
    
    # Simple heuristics engine to boost related capabilities
    def score_capability(cap: dict[str, Any]) -> int:
        score = 0
        cap_title = str(cap.get("title", "")).lower()
        cap_kind = str(cap.get("capability_kind", "")).lower()
        cap_slug = str(cap.get("capability_slug", "")).lower()
        
        # Domain Pattern 1: If we just researched/searched, we likely want to draft, analyze, or synthesize
        if any(word in current_title or word in current_summary for word in ["search", "research", "find", "gather"]):
            if any(word in cap_title or word in cap_slug for word in ["draft", "analyze", "synthesize", "review"]):
                score += 50
                
        # Domain Pattern 2: If we just drafted/wrote something, we likely want to review or notify
        elif any(word in current_title or word in current_summary for word in ["draft", "write", "compose"]):
            if any(word in cap_title for word in ["review", "notify", "send", "github"]):
                score += 50
                
        # Domain Pattern 3: If it's a trigger, we almost always want a task or integration next
        elif current_route.startswith("trigger"):
            if cap_kind == "task":
                score += 30
            elif cap_kind == "integration":
                score += 20
                
        # Base utility: Task capabilities are generally good generic next steps
        if cap_kind == "task":
            score += 10
            
        return score

    # 3. Score and sort all capabilities
    scored_capabilities = []
    for cap in catalog:
        # In a fully typed system, we would also filter out nodes whose `required_inputs` 
        # are not satisfied by the upstream topological DAG traversal.
        # For now, we assume all are "possible" and rank by "likely".
        score = score_capability(cap)
        scored_capabilities.append({"score": score, "capability": cap})
        
    scored_capabilities.sort(key=lambda x: x["score"], reverse=True)
    
    # Split into likely (top 3) and possible (the rest)
    likely = [item["capability"] for item in scored_capabilities[:4] if item["score"] > 10]
    possible = [item["capability"] for item in scored_capabilities if item["capability"] not in likely]
    
    return {
        "status": "success",
        "likely_next_steps": likely,
        "possible_next_steps": possible
    }

# Register the capability
registry.register(
    path="/api/workflows/{workflow_id}/build/suggest-next",
    method="POST",
    command_class=SuggestNextNodesCommand,
    handler=handle_suggest_next_nodes,
    description="Context-aware graph autocomplete: suggests the most logical next steps for a given node.",
    operation_name="workflow_build.suggest_next",
    operation_kind="query",
    source_kind="cqrs_query",
    authority_ref="authority.capability_catalog",
    projection_ref="projection.capability_catalog",
)
