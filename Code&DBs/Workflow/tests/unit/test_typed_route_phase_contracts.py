"""Phase 2.1.A/B regression: typed route contracts on compiler output phases.

Before this change, ``make_execution_phase`` emitted phases with
``agent_route="auto/build"`` and a human-readable ``outputs`` list but no
machine-readable ``consumes`` / ``produces``. Canvas Composer nodes inherited
prose-only contracts and the type-flow validator could not check them at
commit time (BUG-C6EE740C and the data-dictionary / gates / Provide-X
chain it gates: BUG-5DD67C2A, BUG-99B9DC7E, BUG-2729F8B7).

The fix:
  1. ``_ROUTE_CONTRACTS`` carries a row for the build family
     (build / implement / develop / code / edit / refactor / stage / execute).
  2. ``route_type_contract(route, title=, summary=)`` is the public helper
     that resolves the contract from route + text.
  3. ``make_execution_phase`` calls the helper and adds typed fields to
     every emitted phase dict.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.compiler_output_builders import make_execution_phase
from runtime.workflow_type_contracts import route_type_contract


def test_route_type_contract_resolves_auto_build():
    contract = route_type_contract("auto/build", title="Implement the workflow")
    assert "code_change_candidate" in contract["produces"]
    assert "diff" in contract["produces"]
    assert "execution_receipt" in contract["produces"]
    # build accepts upstream context types
    assert "research_findings" in contract["consumes_any"]
    assert "input_text" in contract["consumes_any"]


def test_route_type_contract_resolves_auto_review():
    contract = route_type_contract("auto/review", title="Review before acceptance")
    assert contract["produces"] == ["review_result"]
    assert "code_change_candidate" in contract["consumes_any"]
    assert "diff" in contract["consumes_any"]


def test_route_type_contract_resolves_research_via_title():
    contract = route_type_contract(
        "auto/build", title="Research existing patterns", summary="Gather sources"
    )
    # Title-based inference: the "research" token should win over a generic build
    # search for a research-titled phase. The route+text both contribute to the
    # searchable string, so research/research_findings beats the build candidate family.
    assert "research_findings" in contract["produces"]
    assert "evidence_pack" in contract["produces"]


def test_route_type_contract_falls_back_to_default_for_unknown_route():
    contract = route_type_contract("auto/unicorn", title="Do magic", summary="?")
    # No matching tokens → default produces=("result",)
    assert contract["produces"] == ["result"]
    assert contract["consumes"] == []


def test_make_execution_phase_carries_typed_contract_for_auto_build():
    phase = make_execution_phase(
        phase_id="phase-001",
        kind="build",
        title="Implement the workflow",
        purpose="Execute the primary build work",
        agent_route="auto/build",
        system_prompt="You are the implementation lead",
        temperature=0.1,
        max_tokens=4000,
        timeout_seconds=900,
        route_catalog={},
        requires_citations=False,
        outputs=["primary deliverable"],
    )
    # Typed contract is now on the phase dict alongside the legacy outputs list
    assert "consumes" in phase
    assert "consumes_any" in phase
    assert "produces" in phase
    assert "code_change_candidate" in phase["produces"]
    assert "execution_receipt" in phase["produces"]
    # Legacy human-string outputs stays for back-compat readers
    assert phase["outputs"] == ["primary deliverable"]


def test_make_execution_phase_carries_typed_contract_for_auto_review():
    phase = make_execution_phase(
        phase_id="phase-002",
        kind="review",
        title="Review before acceptance",
        purpose="Use a separate review pass",
        agent_route="auto/review",
        system_prompt="You are the review agent",
        temperature=0.0,
        max_tokens=2200,
        timeout_seconds=600,
        route_catalog={},
        requires_citations=False,
        outputs=["review findings"],
    )
    assert phase["produces"] == ["review_result"]
    assert "code_change_candidate" in phase["consumes_any"]


def test_make_execution_phase_unknown_route_still_emits_typed_fields():
    phase = make_execution_phase(
        phase_id="phase-x",
        kind="execute",
        title="Run something arbitrary",
        purpose="Generic step",
        agent_route="auto/unknown",
        system_prompt="...",
        temperature=0.1,
        max_tokens=1000,
        timeout_seconds=60,
        route_catalog={},
        requires_citations=False,
        outputs=["something"],
    )
    # Even on a route with no matching contract row, the phase carries
    # typed fields (default produces=["result"]) so downstream consumers
    # never see a phase without the typed shape.
    assert phase["produces"] == ["result"]
    assert phase["consumes"] == []
    assert phase["consumes_any"] == []
