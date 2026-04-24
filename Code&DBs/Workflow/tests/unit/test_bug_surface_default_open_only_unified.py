"""Regression pin for BUG-BAEC85C1.

Before the fix, bug-surface defaults for ``open_only`` were split across
multiple MCP tool schemas as hardcoded literals, even though the canonical
primitives lived in ``runtime.primitive_contracts``:

* ``surfaces/mcp/tools/bugs.py``          → schema had ``default: False``
  (matched ``bug_query_default_open_only_list()`` by value, but drifted
  if the primitive ever changed).
* ``surfaces/mcp/tools/operator.py``      → two schemas hardcoded
  ``default: True`` (``praxis_bug_replay_provenance_backfill`` and
  ``praxis_issue_backlog``) — same authority-split shape.

The API GET handler (``surfaces/api/handlers/_query_bugs.py``) and the
runtime call-paths inside those same MCP tools already sourced the default
from the primitive. So the surfaced schema documentation and the enforced
runtime default could silently drift whenever the primitive was retuned —
the MCP inputSchema was a frozen copy of a default that should have been
dynamic, classic authority-split trap.

The fix routes every bug-surface default through the canonical primitives:

* Machine-facing surfaces (``praxis_bugs``, API ``GET /bugs``, API
  ``POST /bugs``/``search``) use ``bug_query_default_open_only_list``.
* Operator-facing surfaces (``praxis_issue_backlog``,
  ``praxis_bug_replay_provenance_backfill``, ``praxis_replay_ready_bugs``,
  CLI ``praxis workflow bugs list``) use
  ``bug_query_default_open_only_backlog``.

Pins:

1. ``praxis_bugs`` schema's ``open_only.default`` equals
   ``bug_query_default_open_only_list()`` — the machine-facing primitive.
2. ``praxis_issue_backlog`` schema's ``open_only.default`` equals
   ``bug_query_default_open_only_backlog()`` — the operator-facing
   primitive.
3. ``praxis_bug_replay_provenance_backfill`` schema's
   ``open_only.default`` equals ``bug_query_default_open_only_backlog()``.
4. The two primitives return distinct booleans (False/True). This is the
   contract the authority-split eliminates: machine surfaces see the full
   bug set by default, operator surfaces see the actionable backlog slice.
5. ``surfaces/mcp/tools/bugs.py`` and
   ``surfaces/mcp/tools/operator.py`` source their defaults through
   primitive imports — pinning that the module source does not contain
   hardcoded boolean literals for the ``open_only`` default fields.
"""
from __future__ import annotations

from pathlib import Path

from runtime.primitive_contracts import (
    bug_query_default_open_only_backlog,
    bug_query_default_open_only_list,
)
from surfaces.mcp.catalog import get_tool_catalog
from surfaces.mcp.tools.bugs import TOOLS as BUG_TOOLS
from surfaces.mcp.tools.operator import TOOLS as OPERATOR_TOOLS


def _open_only_default(tools: dict, tool_name: str) -> object:
    """Pull ``open_only.default`` out of a catalog tool's inputSchema."""
    _fn, meta = tools[tool_name]
    schema = meta["inputSchema"]
    props = schema["properties"]
    assert "open_only" in props, f"{tool_name} schema is missing open_only"
    assert "default" in props["open_only"], (
        f"{tool_name} open_only field has no default — this is the drift "
        f"surface the unified authority closes"
    )
    return props["open_only"]["default"]


# -- 1. praxis_bugs default matches machine-facing primitive --------------


def test_praxis_bugs_open_only_default_matches_list_primitive() -> None:
    """The core BUG-BAEC85C1 pin for the machine-facing surface.

    Before the fix, ``praxis_bugs`` inputSchema hardcoded ``default: False``.
    The value matched the primitive by coincidence but was frozen — a later
    tuning of ``bug_query_default_open_only_list`` would leave the surfaced
    schema stale. Now the schema is built at module-load time from the
    primitive itself, so both surfaces move together by construction.
    """
    assert _open_only_default(BUG_TOOLS, "praxis_bugs") == (
        bug_query_default_open_only_list()
    )


# -- 2. praxis_issue_backlog default matches operator-facing primitive ---


def test_praxis_issue_backlog_open_only_default_matches_backlog_primitive() -> None:
    """Operator-facing tool: schema default must track the backlog primitive.

    ``praxis_issue_backlog`` is part of the operator surface that sees the
    actionable open-work slice by default. The runtime call already routed
    through ``bug_query_default_open_only_backlog``; the inputSchema now
    matches, closing the authority split.
    """
    assert _open_only_default(OPERATOR_TOOLS, "praxis_issue_backlog") == (
        bug_query_default_open_only_backlog()
    )


# -- 3. praxis_bug_replay_provenance_backfill same operator-facing ------


def test_praxis_replay_provenance_backfill_open_only_default() -> None:
    """Replay-provenance backfill is operator-facing maintenance: same
    primitive as the issue backlog. The authority is one function, not
    three hardcoded literals in three tool schemas."""
    assert _open_only_default(
        OPERATOR_TOOLS, "praxis_bug_replay_provenance_backfill"
    ) == bug_query_default_open_only_backlog()


def test_catalog_resolves_operator_bug_default_wrapper() -> None:
    """Catalog loading must fold operator-local primitive wrappers too."""
    catalog = get_tool_catalog()
    schema = catalog["praxis_bug_replay_provenance_backfill"].input_schema

    assert schema["properties"]["open_only"]["default"] == (
        bug_query_default_open_only_backlog()
    )


# -- 4. the two primitives disagree on purpose ---------------------------


def test_machine_and_operator_primitives_return_distinct_defaults() -> None:
    """The fix is only meaningful if the two primitives actually carry
    different values — otherwise the authority-split would be silent and
    the machine/operator distinction invisible.

    Machine-facing surfaces default to ``open_only=False`` (see the full
    bug set), operator-facing surfaces default to ``open_only=True`` (see
    actionable backlog). The two primitives must return distinct booleans;
    if they ever converge, the contract has been flattened and this
    assertion should be re-evaluated deliberately.
    """
    assert bug_query_default_open_only_list() is False
    assert bug_query_default_open_only_backlog() is True


# -- 5. source code does not hardcode boolean literals for these defaults


def test_mcp_bug_tool_sources_avoid_hardcoded_open_only_defaults() -> None:
    """The most important authority-split signal: the module source.

    If a future refactor re-introduces ``"default": True`` or
    ``"default": False`` as a hardcoded literal adjacent to an
    ``open_only`` key in the MCP bug surface files, this test fails — we
    lose the primitive-sourced default and the schema can silently drift
    from the runtime behavior again. The assertion greps for the exact
    drift shape, not the tool behavior, so it catches source-level
    regressions that passing schema-values tests cannot.
    """
    root = Path(__file__).resolve().parents[2]
    for rel in ("surfaces/mcp/tools/bugs.py", "surfaces/mcp/tools/operator.py"):
        text = (root / rel).read_text(encoding="utf-8")
        # Offender shape: a `"default": True,` or `"default": False,`
        # literal immediately under an `"open_only":` schema field. We
        # only fail if we see it in this exact adjacency.
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if '"open_only"' not in line:
                continue
            window = lines[i : i + 10]
            for candidate in window:
                stripped = candidate.strip()
                if stripped in ('"default": True,', '"default": False,'):
                    raise AssertionError(
                        f"{rel} has a hardcoded open_only default literal "
                        f"near line {i + 1}: {stripped!r}. Route it through "
                        f"runtime.primitive_contracts.bug_query_default_open_only_"
                        f"{{list,backlog}}() so the MCP schema and the runtime "
                        f"share one authority (BUG-BAEC85C1)."
                    )
