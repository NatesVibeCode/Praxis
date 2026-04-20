"""Provider lane policy — first gate of the lane-control hierarchy.

A ``provider_lane_policy`` row declares which adapter types (``cli_llm``,
``llm_task``) the router may admit for a given provider, plus whether
downstream gates (task_type, spec, job) may widen that set.

The gate narrows: a later level can never widen beyond
``allowed_adapter_types``. ``overridable=False`` means the set is final
regardless of later levels.

Callers load once, query many times:

    policies = load_provider_lane_policies(conn)
    admitted, reason = admit_adapter_type(policies, provider_slug, adapter_type)
    if not admitted:
        # refuse this route; reason carries the trace code
        ...
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from asyncpg import PostgresError

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

_UNDEFINED_TABLE_SQLSTATE = "42P01"

KNOWN_ADAPTER_TYPES = frozenset({"cli_llm", "llm_task"})


@dataclass(frozen=True, slots=True)
class ProviderLanePolicy:
    provider_slug: str
    allowed_adapter_types: frozenset[str]
    overridable: bool


def load_provider_lane_policies(
    conn: "SyncPostgresConnection",
) -> dict[str, ProviderLanePolicy]:
    """Load all provider lane policies. Returns empty dict if table absent."""
    try:
        rows = conn.execute(
            """SELECT provider_slug, allowed_adapter_types, overridable
               FROM provider_lane_policy"""
        )
    except PostgresError as exc:
        if getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE_SQLSTATE:
            return {}
        raise
    out: dict[str, ProviderLanePolicy] = {}
    for row in rows or []:
        slug = str(row["provider_slug"]).strip()
        if not slug:
            continue
        allowed = frozenset(
            str(a).strip().lower()
            for a in (row["allowed_adapter_types"] or ())
            if str(a).strip()
        )
        if not allowed:
            continue
        out[slug] = ProviderLanePolicy(
            provider_slug=slug,
            allowed_adapter_types=allowed,
            overridable=bool(row["overridable"]),
        )
    return out


def admit_adapter_type(
    policies: dict[str, ProviderLanePolicy],
    provider_slug: str,
    adapter_type: str,
    *,
    spend_pressure: str | None = None,
    budget_authority_unreachable: bool = False,
) -> tuple[bool, str]:
    """Decide whether a route may use ``adapter_type`` for ``provider_slug``.

    Returns ``(admitted, reason_code)``. Reason codes:

    - ``lane.admitted``                 — in allowed set
    - ``lane.rejected.not_allowed``     — policy exists, adapter not in set
    - ``lane.rejected.unknown_adapter`` — adapter_type is not a known lane
    - ``lane.rejected.budget_exhausted``— paid lane, spend_pressure=high
    - ``lane.rejected.budget_authority_unreachable``
                                        — paid lane, budget-window authority
                                          table is missing (schema drift);
                                          fail closed per BUG-2D3AECF3
    - ``lane.admitted.no_policy``       — no policy row, fail open (pre-seed)

    ``spend_pressure`` is a hard gate for ``llm_task`` only: when the
    provider's paid-API budget window is exhausted (``high``), the route is
    refused regardless of allow-list. CLI is free, so the budget gate does
    not apply to ``cli_llm``.

    ``budget_authority_unreachable`` is the sibling gate for the same paid
    lane: when :class:`runtime.routing_economics.BudgetAuthoritySnapshot`
    reports that ``provider_budget_windows`` is missing, admitting paid
    routes would turn schema drift into silent paid-lane opening. Refused
    with reason ``lane.rejected.budget_authority_unreachable`` so the
    trace surfaces the authority gap explicitly.
    """
    normalized = (adapter_type or "").strip().lower()
    if normalized not in KNOWN_ADAPTER_TYPES:
        return False, "lane.rejected.unknown_adapter"

    if normalized == "llm_task":
        if budget_authority_unreachable:
            return False, "lane.rejected.budget_authority_unreachable"
        pressure = (spend_pressure or "").strip().lower()
        if pressure == "high":
            return False, "lane.rejected.budget_exhausted"

    policy = policies.get(provider_slug)
    if policy is None:
        return True, "lane.admitted.no_policy"
    if normalized in policy.allowed_adapter_types:
        return True, "lane.admitted"
    return False, "lane.rejected.not_allowed"
