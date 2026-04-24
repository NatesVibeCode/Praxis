"""Verify tool type_contract metadata is declared, parseable, and narrows composition.

Prescriptive composition (per standing order
architecture-policy::platform-architecture::one-graph-many-lenses): at any graph
state, the catalog narrows to tools whose `consumes` type is satisfied by the
current accumulator. These tests exercise the bug-lifecycle chain as the first
typed slice.
"""
from __future__ import annotations

from surfaces.mcp.catalog import get_tool_catalog


def _tool(tool_name: str):
    catalog = get_tool_catalog()
    assert tool_name in catalog, f"{tool_name} missing from catalog"
    return catalog[tool_name]


def test_praxis_bugs_declares_per_action_type_contract():
    bugs = _tool("praxis_bugs")
    contract = bugs.type_contract
    assert contract, "praxis_bugs must declare type_contract"
    for action in bugs.action_enum:
        assert action in contract, f"Action '{action}' missing from praxis_bugs type_contract"
        assert isinstance(contract[action]["consumes"], list)
        assert isinstance(contract[action]["produces"], list)


def test_single_shape_chain_tools_use_default_key():
    for tool_name in ("praxis_replay_ready_bugs", "praxis_bug_replay_provenance_backfill"):
        contract = _tool(tool_name).type_contract
        assert contract, f"{tool_name} must declare type_contract"
        assert "default" in contract, f"{tool_name} single-shape contract must key on 'default'"
        assert isinstance(contract["default"]["consumes"], list)
        assert isinstance(contract["default"]["produces"], list)


def test_type_slugs_use_data_dictionary_namespace():
    for tool_name in ("praxis_bugs", "praxis_replay_ready_bugs", "praxis_bug_replay_provenance_backfill"):
        tool = _tool(tool_name)
        for key, contract in tool.type_contract.items():
            for slug in contract["consumes"] + contract["produces"]:
                assert slug.startswith("praxis."), (
                    f"{tool_name}[{key}] slug '{slug}' is not in praxis.* namespace"
                )


def test_narrowing_from_bug_record_state_is_multi_tool():
    """Given a graph state holding praxis.bug.record, multiple tool-actions must be legal."""
    catalog = get_tool_catalog()
    state = {"praxis.bug.record"}
    legal: list[tuple[str, str]] = []
    for name, tool in catalog.items():
        for action, contract in tool.type_contract.items():
            consumes = set(contract["consumes"])
            if "praxis.bug.record" in consumes and consumes.issubset(state):
                legal.append((name, action))
    assert len(legal) >= 3, (
        f"Expected >=3 bug-record-consuming actions to be legal from state {state}, got {legal}"
    )


def test_narrowing_requires_all_consumed_types():
    """resolve requires both bug.record and bug.resolution_request — bug.record alone is insufficient."""
    bugs = _tool("praxis_bugs")
    resolve_consumes = set(bugs.type_contract["resolve"]["consumes"])
    assert "praxis.bug.record" in resolve_consumes
    assert "praxis.bug.resolution_request" in resolve_consumes
    state_with_record_only = {"praxis.bug.record"}
    assert not resolve_consumes.issubset(state_with_record_only), (
        "resolve must NOT be legal when only bug.record is in state (resolution_request missing)"
    )
    state_with_both = state_with_record_only | {"praxis.bug.resolution_request"}
    assert resolve_consumes.issubset(state_with_both), (
        "resolve must be legal when both bug.record and bug.resolution_request are in state"
    )
