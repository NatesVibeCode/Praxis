"""Cost/value computation for task-type routing.

Covers budget spend pressure, marginal cost computation, economic rationale,
and billing mode analysis.  DB I/O is limited to loading budget windows.

Budget-authority contract
-------------------------

``provider_budget_windows`` is the policy authority for paid-lane admission.
Callers must never treat missing authority as a permissive empty policy —
otherwise schema drift silently opens paid routes. The typed
:class:`BudgetAuthoritySnapshot` carries three states explicitly:

* ``reachable=True`` with a window for a provider — known pressure drives
  admission.
* ``reachable=True`` without a window for a provider — authority exists,
  pre-seed / no-policy state, spend pressure is ``"unknown"`` and paid
  lanes are admitted under the current policy-less defaults.
* ``reachable=False`` — the ``provider_budget_windows`` table is missing
  (sqlstate ``42P01``). Paid lane (``llm_task``) admission MUST fail closed;
  downstream consumers route through
  :func:`runtime.lane_policy.admit_adapter_type` with
  ``budget_authority_unreachable=True``.

Closes BUG-2D3AECF3 (architecture) and BUG-6B34915A (wiring — the explicit
provider/model route path must load budget authority by provider_slug even
when no ``provider_policy_id`` is known upfront).
"""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping

from asyncpg import PostgresError
from registry.provider_execution_registry import (
    resolve_default_adapter_type,
    resolve_adapter_economics,
    supports_adapter,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

_PREPAID_BILLING_MODES = frozenset({"subscription_included", "prepaid_credit", "owned_compute"})
_ADAPTER_COST_ORDER = ("cli_llm", "llm_task")
_UNDEFINED_TABLE_SQLSTATE = "42P01"


def _provider_ref_from_slug(provider_slug: str) -> str:
    """Canonical provider_ref used in provider_budget_windows rows."""
    return f"provider.{(provider_slug or '').strip()}"


# ---------------------------------------------------------------------------
# Cost helpers
# ---------------------------------------------------------------------------

def float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def row_effective_marginal_cost(row: Any) -> float:
    configured = row.get("effective_marginal_cost")
    if configured is not None:
        try:
            return float(configured)
        except (TypeError, ValueError):
            pass
    try:
        return float(row.get("cost_per_m_tokens") or 0.0)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Budget spend pressure
# ---------------------------------------------------------------------------

def budget_spend_pressure(window: dict[str, Any] | None) -> str:
    if not window:
        return "unknown"
    status = str(window.get("budget_status") or "").strip().lower()
    limit_values = (
        (float_or_none(window.get("request_limit")), float_or_none(window.get("requests_used"))),
        (float_or_none(window.get("token_limit")), float_or_none(window.get("tokens_used"))),
        (float_or_none(window.get("spend_limit_usd")), float_or_none(window.get("spend_used_usd"))),
    )
    ratios = [
        used / limit
        for limit, used in limit_values
        if limit is not None and used is not None and limit > 0
    ]
    max_ratio = max(ratios) if ratios else None
    if any(token in status for token in ("blocked", "exhaust", "denied", "hard")):
        return "high"
    if any(token in status for token in ("warning", "warn", "soft", "approach", "constrain")):
        return "medium"
    if max_ratio is None:
        return "low" if status else "unknown"
    if max_ratio >= 0.95:
        return "high"
    if max_ratio >= 0.75:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Economic rationale
# ---------------------------------------------------------------------------

def economic_rationale(row: dict[str, Any]) -> str:
    adapter_type = str(row.get("adapter_type") or "").strip()
    billing_mode = str(row.get("billing_mode") or "").strip()
    budget_bucket = str(row.get("budget_bucket") or "").strip()
    spend_pressure = str(row.get("spend_pressure") or "").strip()
    if not any((adapter_type, billing_mode, budget_bucket, spend_pressure)):
        return ""
    return (
        f"economics={adapter_type or 'unknown'}"
        f"/{billing_mode or 'unknown'}"
        f"/{budget_bucket or 'unknown'}"
        f"/spend:{spend_pressure or 'unknown'}"
    )


# ---------------------------------------------------------------------------
# Budget window loading
# ---------------------------------------------------------------------------

_EMPTY_MAPPING: Mapping[str, Mapping[str, Any]] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class BudgetAuthoritySnapshot:
    """Typed snapshot of ``provider_budget_windows`` authority.

    Keeping the authority-reachability signal on the snapshot is what lets
    paid-lane admission fail closed on schema drift. Consumers must read
    ``reachable`` before treating an empty window set as permissive.

    The two indexes keep equivalent data keyed differently:

    * ``windows_by_provider_policy`` — direct lookup by
      ``provider_policies.provider_policy_id``; used by the auto-chain path
      that already threads ``provider_policy_id`` through the candidate
      catalog.
    * ``windows_by_provider_ref`` — lookup by ``provider.{provider_slug}``;
      used by the explicit ``provider/model`` path that cannot assume a
      policy id is known. Without this, BUG-6B34915A re-emerges.

    ``data_quality_ok`` is ``False`` when at least one enabled
    ``severity='error'`` quality rule on ``table:provider_budget_windows``
    has a latest run in ``('fail', 'error')``. The authority-owner has
    declared such rows invalid, so paid-lane admission must refuse rather
    than route off data the operator has flagged as broken. Orthogonal to
    ``reachable``: the table can be present (reachable=True) yet carry
    rows that violate an operator-layer invariant (data_quality_ok=False).
    Missing quality infrastructure fails *open* on this axis — the
    authority-reachability gate remains the primary guard.
    """

    windows_by_provider_policy: Mapping[str, Mapping[str, Any]]
    windows_by_provider_ref: Mapping[str, Mapping[str, Any]]
    reachable: bool
    data_quality_ok: bool = True

    @classmethod
    def unreachable(cls) -> "BudgetAuthoritySnapshot":
        """Authority table missing — paid lane admission must fail closed."""
        return cls(
            windows_by_provider_policy=_EMPTY_MAPPING,
            windows_by_provider_ref=_EMPTY_MAPPING,
            reachable=False,
        )

    @classmethod
    def empty(cls) -> "BudgetAuthoritySnapshot":
        """Authority reachable but no relevant windows loaded (test default)."""
        return cls(
            windows_by_provider_policy=_EMPTY_MAPPING,
            windows_by_provider_ref=_EMPTY_MAPPING,
            reachable=True,
        )

    def window_for_policy(
        self, provider_policy_id: str | None
    ) -> Mapping[str, Any] | None:
        if not provider_policy_id:
            return None
        return self.windows_by_provider_policy.get(str(provider_policy_id))

    def window_for_provider_slug(
        self, provider_slug: str | None
    ) -> Mapping[str, Any] | None:
        if not provider_slug:
            return None
        return self.windows_by_provider_ref.get(_provider_ref_from_slug(provider_slug))

    def window_for(
        self,
        *,
        provider_policy_id: str | None = None,
        provider_slug: str | None = None,
    ) -> Mapping[str, Any] | None:
        """Lookup most-specific window; policy id wins when both resolve."""
        return self.window_for_policy(provider_policy_id) or self.window_for_provider_slug(
            provider_slug
        )


_SQL_BUDGET_WINDOW_FAILING_ERROR_RULES = """
WITH latest AS (
    SELECT DISTINCT ON (object_kind, field_path, rule_kind)
        object_kind, field_path, rule_kind, status
    FROM data_dictionary_quality_runs
    WHERE object_kind = 'table:provider_budget_windows'
    ORDER BY object_kind, field_path, rule_kind, started_at DESC
)
SELECT COUNT(*) AS failing
FROM data_dictionary_quality_rules_effective r
JOIN latest l
  ON l.object_kind = r.object_kind
 AND l.field_path = r.field_path
 AND l.rule_kind = r.rule_kind
WHERE r.object_kind = 'table:provider_budget_windows'
  AND r.severity = 'error'
  AND r.enabled = TRUE
  AND l.status IN ('fail', 'error')
"""


def _load_budget_window_data_quality_ok(
    conn: "SyncPostgresConnection",
) -> bool:
    """True iff no error-severity quality rule on provider_budget_windows is failing.

    Fails *open* on missing quality infrastructure (sqlstate 42P01) — the
    authority-reachability gate on ``load_budget_authority_snapshot`` is the
    primary fail-closed guard, so a missing quality table must not compound
    it into a blanket paid-lane refusal.
    """
    try:
        rows = conn.execute(_SQL_BUDGET_WINDOW_FAILING_ERROR_RULES)
    except PostgresError as exc:
        if getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE_SQLSTATE:
            return True
        raise
    for row in rows or []:
        try:
            failing = int(row.get("failing") or 0)
        except (TypeError, ValueError):
            failing = 0
        return failing == 0
    return True


def load_budget_authority_snapshot(
    conn: "SyncPostgresConnection",
) -> BudgetAuthoritySnapshot:
    """Load the current budget-window authority snapshot.

    Loads the most recent window per ``provider_policy_id`` and indexes it by
    both ``provider_policy_id`` and ``provider_ref`` so consumers can look up
    by whichever axis they already have. Callers never need to know the
    policy-id mapping.

    Also reads the latest data-quality status for
    ``table:provider_budget_windows`` so paid-lane admission can refuse when
    an operator-layer invariant has been broken. Quality-infra missing fails
    open on that axis — see :func:`_load_budget_window_data_quality_ok`.

    If ``provider_budget_windows`` is missing (sqlstate ``42P01``), returns
    :meth:`BudgetAuthoritySnapshot.unreachable` so downstream paid-lane
    admission can fail closed instead of silently opening.
    """
    try:
        rows = conn.execute(
            """SELECT DISTINCT ON (provider_policy_id)
                      provider_policy_id,
                      provider_ref,
                      budget_status,
                      request_limit,
                      requests_used,
                      token_limit,
                      tokens_used,
                      spend_limit_usd,
                      spend_used_usd,
                      window_started_at,
                      window_ended_at,
                      created_at
               FROM public.provider_budget_windows
               ORDER BY provider_policy_id,
                        window_ended_at DESC NULLS LAST,
                        created_at DESC NULLS LAST""",
        )
    except PostgresError as exc:
        if getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE_SQLSTATE:
            return BudgetAuthoritySnapshot.unreachable()
        raise
    by_policy: dict[str, Mapping[str, Any]] = {}
    by_ref: dict[str, Mapping[str, Any]] = {}
    for raw in rows or []:
        window = dict(raw)
        policy_id = str(window.get("provider_policy_id") or "").strip()
        provider_ref = str(window.get("provider_ref") or "").strip()
        frozen = MappingProxyType(window)
        if policy_id:
            by_policy[policy_id] = frozen
        if provider_ref:
            by_ref[provider_ref] = frozen
    data_quality_ok = _load_budget_window_data_quality_ok(conn)
    return BudgetAuthoritySnapshot(
        windows_by_provider_policy=MappingProxyType(by_policy),
        windows_by_provider_ref=MappingProxyType(by_ref),
        reachable=True,
        data_quality_ok=data_quality_ok,
    )


# ---------------------------------------------------------------------------
# Marginal cost computation / adapter selection
# ---------------------------------------------------------------------------

def resolve_route_economics(
    *,
    provider_slug: str,
    adapter_type: str | None,
    provider_policy_id: str | None,
    raw_cost_per_m_tokens: float,
    budget_authority: BudgetAuthoritySnapshot,
    default_adapter: str | None = None,
    strict_adapter: bool = False,
) -> dict[str, Any]:
    """Pick the cheapest viable adapter and compute effective marginal cost.

    Returns a dict with keys: adapter_type, billing_mode, budget_bucket,
    effective_marginal_cost, spend_pressure, budget_status, prefer_prepaid,
    allow_payg_fallback, budget_authority_unreachable.

    ``budget_authority_unreachable`` propagates the authority-missing state so
    :func:`runtime.lane_policy.admit_adapter_type` can refuse paid lanes when
    ``provider_budget_windows`` is gone (schema drift). Auto and explicit
    paths both route through here, so the gate is uniform.

    ``budget_window_data_quality_error`` propagates the authority-corrupt
    state on the same axis: table reachable, but an operator-declared
    invariant on its rows is failing. Paid-lane admission refuses rather
    than route off corrupt authority data.

    When ``strict_adapter`` is true, ``adapter_type`` is treated as route
    authority rather than a preference and economics will not substitute a
    cheaper transport behind the router's back.
    """
    if default_adapter is None:
        default_adapter = resolve_default_adapter_type(provider_slug)

    budget_window = budget_authority.window_for(
        provider_policy_id=provider_policy_id,
        provider_slug=provider_slug,
    )
    spend_pressure = budget_spend_pressure(budget_window)
    authority_unreachable = not budget_authority.reachable
    data_quality_error = not budget_authority.data_quality_ok

    candidate_adapters: list[str] = []
    normalized_adapter_type = (adapter_type or "").strip().lower()
    if normalized_adapter_type and supports_adapter(provider_slug, normalized_adapter_type):
        candidate_adapters.append(normalized_adapter_type)
    if not strict_adapter:
        for candidate_adapter_type in (default_adapter, *_ADAPTER_COST_ORDER):
            normalized_candidate = (candidate_adapter_type or "").strip().lower()
            if (
                normalized_candidate
                and normalized_candidate not in candidate_adapters
                and supports_adapter(provider_slug, normalized_candidate)
            ):
                candidate_adapters.append(normalized_candidate)
    if not candidate_adapters:
        raise RuntimeError(
            f"provider {provider_slug!r} has no supported adapter for routing economics"
        )

    options: list[dict[str, Any]] = []
    for candidate_adapter_type in candidate_adapters:
        economics = resolve_adapter_economics(provider_slug, candidate_adapter_type)
        billing_mode = str(economics.get("billing_mode") or "metered_api")
        configured_marginal_cost = economics.get("effective_marginal_cost")
        effective_marginal_cost = float(
            configured_marginal_cost
            if configured_marginal_cost is not None
            else (raw_cost_per_m_tokens or 0.0)
        )
        if billing_mode not in _PREPAID_BILLING_MODES:
            effective_marginal_cost = max(effective_marginal_cost, raw_cost_per_m_tokens)
            if spend_pressure == "medium":
                effective_marginal_cost *= 3.0
            elif spend_pressure == "high":
                effective_marginal_cost *= 10.0
        # economics is produced by provider_transport.AdapterEconomicsContract,
        # so prefer_prepaid / allow_payg_fallback are guaranteed present and
        # typed — no local defaulting (closes BUG-8DAA5468).
        options.append({
            "adapter_type": candidate_adapter_type,
            "billing_mode": billing_mode,
            "budget_bucket": str(economics["budget_bucket"]),
            "effective_marginal_cost": effective_marginal_cost,
            "spend_pressure": spend_pressure,
            "budget_status": str((budget_window or {}).get("budget_status") or ""),
            "prefer_prepaid": economics["prefer_prepaid"],
            "allow_payg_fallback": economics["allow_payg_fallback"],
            "budget_authority_unreachable": authority_unreachable,
            "budget_window_data_quality_error": data_quality_error,
        })

    options.sort(
        key=lambda option: (
            row_effective_marginal_cost(option),
            0 if option.get("billing_mode") in _PREPAID_BILLING_MODES else 1,
            0 if bool(option.get("prefer_prepaid")) else 1,
            {"low": 0, "unknown": 1, "medium": 2, "high": 3}.get(
                str(option.get("spend_pressure") or "unknown"),
                1,
            ),
            0 if str(option.get("adapter_type") or "") == default_adapter else 1,
        )
    )
    return options[0]
