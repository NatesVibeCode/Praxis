"""Phase 2.1.B regression: definition graph nodes carry typed contracts.

Before this change, ``build_definition_graph`` emitted capability and
draft_step nodes with only a ``payload`` blob — no machine-readable
``consumes`` / ``produces``. Moon Composer rendered nodes without typed
ports, the type-flow validator could not reason about graph state, and
"Provide X" uncertainty cards filled the gap with prose stubs
(BUG-C6EE740C / BUG-99B9DC7E).

The fix:
  - capability nodes carry ``capability_type_contract(payload)``
  - draft_step nodes carry ``route_type_contract(route, title=, summary=)``
  - both keep their existing ``payload`` for back-compat
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.definition_compile_kernel import build_definition_graph


def test_capability_node_carries_typed_contract():
    graph = build_definition_graph(
        source_prose="research and analyze the situation",
        compiled_prose="research and analyze the situation",
        references=[],
        capabilities=[
            {
                "slug": "research/local-knowledge",
                "title": "Local research",
                "description": "Search local notes for context",
            }
        ],
        authority="",
        sla={},
        narrative_blocks=[],
        trigger_intent=[],
        draft_flow=[],
    )
    capability_nodes = [n for n in graph["nodes"] if n["kind"] == "capability"]
    assert len(capability_nodes) == 1
    node = capability_nodes[0]
    assert "consumes" in node
    assert "consumes_any" in node
    assert "produces" in node
    # research-tagged capability produces research_findings/evidence_pack
    assert "research_findings" in node["produces"]


def test_draft_step_node_carries_typed_contract_for_implement_step():
    graph = build_definition_graph(
        source_prose="implement the feature",
        compiled_prose="implement the feature",
        references=[],
        capabilities=[],
        authority="",
        sla={},
        narrative_blocks=[],
        trigger_intent=[],
        draft_flow=[
            {
                "id": "step-001",
                "title": "Implement the feature",
                "summary": "Build the main code path",
                "order": 1,
            }
        ],
    )
    draft_steps = [n for n in graph["nodes"] if n["kind"] == "draft_step"]
    assert len(draft_steps) == 1
    node = draft_steps[0]
    # implement/build family produces code_change / diff / execution_receipt
    assert "code_change" in node["produces"]
    assert "execution_receipt" in node["produces"]


def test_draft_step_node_with_review_title_routes_to_review_contract():
    graph = build_definition_graph(
        source_prose="check it",
        compiled_prose="check it",
        references=[],
        capabilities=[],
        authority="",
        sla={},
        narrative_blocks=[],
        trigger_intent=[],
        draft_flow=[
            {
                "id": "step-001",
                "title": "Review the implementation",
                "summary": "Validate the build",
                "order": 1,
            }
        ],
    )
    node = next(n for n in graph["nodes"] if n["kind"] == "draft_step")
    assert node["produces"] == ["review_result"]


def test_draft_step_node_default_when_no_inferable_role():
    graph = build_definition_graph(
        source_prose="x",
        compiled_prose="x",
        references=[],
        capabilities=[],
        authority="",
        sla={},
        narrative_blocks=[],
        trigger_intent=[],
        draft_flow=[
            {
                "id": "step-001",
                "title": "Step one",
                "summary": "Do the thing",
                "order": 1,
            }
        ],
    )
    node = next(n for n in graph["nodes"] if n["kind"] == "draft_step")
    # Default route auto/build picked → produces code_change family
    assert "code_change" in node["produces"]


def test_reference_and_narrative_nodes_dont_get_typed_contract():
    """Reference and narrative_block nodes are descriptive, not executable.
    They don't need typed contracts. Only capability and draft_step do."""
    graph = build_definition_graph(
        source_prose="x",
        compiled_prose="x",
        references=[{"slug": "doc.api", "title": "API docs"}],
        capabilities=[],
        authority="",
        sla={},
        narrative_blocks=[
            {
                "id": "block-001",
                "summary": "Some narrative",
                "order": 1,
                "reference_slugs": ["doc.api"],
                "capability_slugs": [],
            }
        ],
        trigger_intent=[],
        draft_flow=[],
    )
    for node in graph["nodes"]:
        if node["kind"] in {"reference", "narrative_block", "trigger"}:
            assert "consumes" not in node
            assert "produces" not in node
