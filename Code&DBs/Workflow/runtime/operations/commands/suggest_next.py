from typing import Any
from pydantic import BaseModel

from runtime.workflow_type_contracts import (
    capability_type_contract,
    selected_accumulated_types,
    type_contract_satisfaction,
)

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

    type_context = selected_accumulated_types(
        nodes=[n for n in nodes if isinstance(n, dict)],
        edges=[e for e in edges if isinstance(e, dict)],
        selected_node_id=str(node_id or "").strip() or None,
    )
    available_types = type_context["available_types"]

    # Simple heuristics engine to boost related capabilities
    def score_capability(cap: dict[str, Any]) -> int:
        score = 0
        cap_title = str(cap.get("title", "")).lower()
        cap_kind = str(cap.get("capability_kind", "")).lower()
        cap_slug = str(cap.get("capability_slug", "")).lower()
        cap_route = str(cap.get("route", "")).lower()
        
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
        if current_route.startswith("trigger") and cap_route.startswith("trigger"):
            score -= 25

        return score

    # 3. Type-check, score, and sort all capabilities. The heuristic score is
    # only used after the graph contract says the next step is legal.
    scored_capabilities = []
    blocked_capabilities = []
    for cap in catalog:
        contract = capability_type_contract(cap)
        satisfaction = type_contract_satisfaction(available_types, contract)
        typed_capability = {
            **cap,
            "type_contract": contract,
            "type_satisfaction": satisfaction,
        }
        if not satisfaction["legal"]:
            blocked_capabilities.append(typed_capability)
            continue
        score = score_capability(cap)
        score += 100
        score += 10 * len(satisfaction.get("satisfied") or [])
        scored_capabilities.append({"score": score, "capability": typed_capability})
        
    scored_capabilities.sort(key=lambda x: x["score"], reverse=True)

    # Split into likely (top 4) and possible (the rest). Both lists are legal;
    # blocked candidates are returned separately for debugging and future UI.
    likely = [item["capability"] for item in scored_capabilities[:4]]
    possible = [item["capability"] for item in scored_capabilities if item["capability"] not in likely]
    
    return {
        "status": "success",
        "type_context": type_context,
        "likely_next_steps": likely,
        "possible_next_steps": possible,
        "blocked_next_steps": blocked_capabilities,
    }
