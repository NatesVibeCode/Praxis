"""Validate that catalog-declared type_contract slugs resolve in the data
dictionary.

Honors architecture-policy::platform-architecture::data-dictionary-
universal-compile-time-clamp: every ``consumes`` / ``produces`` slug
declared on a tool's ``type_contract`` metadata entry must correspond to
a ``data_dictionary_objects`` row. Unresolved slugs are returned as
structured findings so callers (tests, CI gates, typed-gap emitters)
can enumerate and repair them — matching architecture-policy::
platform-architecture::fail-closed-at-compile-no-silent-defaults.

Scope: this module is a query-side validator that runs against the
catalog + data dictionary authority. It does not modify state. A future
typed_gap emitter (Phase 1.5/1.6) may consume the findings to produce
durable repair rows.
"""
from __future__ import annotations

from typing import Any, Iterable


def _extract_slugs_from_type_contract(
    type_contract: dict[str, dict[str, list[str]]],
) -> set[str]:
    """Flatten a tool's type_contract into the set of slug references.

    A type_contract has the shape::

        {"<action_or_default>": {"consumes": [...], "produces": [...]}}

    This helper returns the union of every non-blank slug mentioned on
    either side, across all actions.
    """
    slugs: set[str] = set()
    for action_contract in (type_contract or {}).values():
        if not isinstance(action_contract, dict):
            continue
        for side in ("consumes", "produces"):
            for raw in action_contract.get(side) or ():
                text = str(raw).strip()
                if text:
                    slugs.add(text)
    return slugs


def collect_catalog_type_contract_slugs() -> dict[str, set[str]]:
    """Walk the MCP tool catalog and return ``{tool_name: set_of_slugs}``.

    Skips tools with no declared type_contract. The catalog loader is
    AST-based, so this call does not incur DB round-trips — it only
    surfaces what the tool modules statically declare.
    """
    from surfaces.mcp.catalog import get_tool_catalog

    catalog = get_tool_catalog()
    out: dict[str, set[str]] = {}
    for tool_name, tool in catalog.items():
        slugs = _extract_slugs_from_type_contract(tool.type_contract)
        if slugs:
            out[str(tool_name)] = slugs
    return out


def _existing_data_dictionary_object_kinds(
    conn: Any, *, slugs: Iterable[str]
) -> set[str]:
    """Return the subset of slugs that have a matching row in
    ``data_dictionary_objects``.

    Runs one SELECT against the dictionary authority. ``slugs`` may be an
    empty iterable; the helper short-circuits in that case.
    """
    slug_list = [s for s in (slugs or ()) if s]
    if not slug_list:
        return set()
    placeholders = ", ".join(f"${i+1}" for i in range(len(slug_list)))
    rows = conn.execute(
        f"SELECT object_kind FROM data_dictionary_objects "
        f"WHERE object_kind IN ({placeholders})",
        *slug_list,
    )
    existing: set[str] = set()
    for row in rows or ():
        if isinstance(row, dict):
            existing.add(str(row.get("object_kind") or ""))
        else:
            # asyncpg-style Record or tuple — first column is object_kind.
            try:
                existing.add(str(row[0]))
            except Exception:
                continue
    existing.discard("")
    return existing


def validate_type_contract_slugs_against_data_dictionary(
    conn: Any,
) -> list[dict[str, str]]:
    """Return a list of findings for every type_contract slug declared on a
    catalog tool that has no matching ``data_dictionary_objects`` row.

    Each finding has the shape::

        {"tool": "<tool_name>", "slug": "<slug>",
         "missing_type": "data_dictionary_object",
         "reason_code": "data_dictionary.object_kind.missing",
         "legal_repair_actions": "add_data_dictionary_objects_row"}

    An empty list means every declared slug resolves. Typed gap emission
    (Phase 1.5/1.6) will convert findings into durable repair rows; for
    now tests and CI gates can inspect the list directly.
    """
    by_tool = collect_catalog_type_contract_slugs()
    if not by_tool:
        return []
    all_slugs: set[str] = set()
    for slugs in by_tool.values():
        all_slugs.update(slugs)
    existing = _existing_data_dictionary_object_kinds(conn, slugs=all_slugs)
    findings: list[dict[str, str]] = []
    for tool_name in sorted(by_tool):
        for slug in sorted(by_tool[tool_name]):
            if slug in existing:
                continue
            findings.append(
                {
                    "tool": tool_name,
                    "slug": slug,
                    "missing_type": "data_dictionary_object",
                    "reason_code": "data_dictionary.object_kind.missing",
                    "legal_repair_actions": "add_data_dictionary_objects_row",
                }
            )
    return findings


__all__ = [
    "collect_catalog_type_contract_slugs",
    "validate_type_contract_slugs_against_data_dictionary",
]
